"""The whole system in one process, for the hosted live demo.

The Docker Compose demo runs SQL Server, the simulator, the agent and the API as
separate services. A serverless host can only run the API, so this module puts
the rest inside it:

* the simulator changes a small SQLite "depot" database;
* the real agent code (snapshot diff, state store, outbox) detects the changes;
* the events go into the API database through the same code as the ingest
  endpoint.

Work only happens when someone reads the dashboard or the API: each read first
plays the warehouse operations due since the last read (one every few seconds),
then runs one agent cycle. When the process starts, a few hours of history are
played on a simulated clock so the dashboard is never empty.

Nothing is persistent. A new process starts a new depot, and the ingest endpoint
only accepts a random key generated at startup, so nobody can post to it.

Run it locally:  uvicorn depot_monitor.demo.web:app
"""

from __future__ import annotations

import datetime as dt
import logging
import secrets
import tempfile
import threading
import time
from pathlib import Path

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .. import __version__
from ..agent.config import AgentConfig, TableSpec
from ..agent.main import Agent
from ..agent.source import SourceReader
from ..agent.state import StateStore
from ..api.app import Settings, create_app, record_heartbeat, store_events
from ..api.schemas import EventBatch, Heartbeat
from .simulator import Simulator, init_database

log = logging.getLogger("depot_monitor.demo.web")

SOURCE_NAME = "depot-demo"
SECONDS_PER_OPERATION = 3.0  # how often the simulated warehouse does something
MAX_CATCH_UP = 20  # after a quiet period, play at most this many operations at once
HEARTBEAT_SECONDS = 20.0
HISTORY_OPERATIONS = 150
HISTORY_HOURS = 6

TABLES = (
    TableSpec("products", ("product_id",)),
    TableSpec("stock", ("warehouse_id", "product_id")),
    TableSpec("stock_movements", ("movement_id",)),
    TableSpec("transfers", ("transfer_id",)),
    TableSpec("transfer_lines", ("transfer_id", "line_no")),
)

REPO = "https://github.com/taharabs/depot-change-monitor"
BANNER = f"""<div style="margin:0 0 18px;padding:10px 14px;border:1px solid var(--line);
border-radius:10px;background:var(--surface);color:var(--ink-2);font-size:13.5px;line-height:1.5">
<b style="color:var(--ink)">Live demo.</b> A simulated warehouse database changes every few
seconds, and the agent code from this project detects each change and reports it here.
The data is synthetic and starts over when the server restarts.
<a href="{REPO}" style="color:var(--update)">Source code</a> ·
<a href="/docs" style="color:var(--update)">API docs</a></div>"""


class LocalShipper:
    """Delivers the agent's events straight to the API database instead of over HTTPS."""

    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self._sessions = sessions

    def send(self, events: list[dict]) -> bool:
        batch = EventBatch.model_validate({"events": events})
        with self._sessions() as session:
            store_events(session, batch.events)
        return True

    def heartbeat(self, source: str, pending: int, version: str) -> bool:
        beat = Heartbeat(source=source, pending=pending, version=version)
        with self._sessions() as session:
            record_heartbeat(session, beat)
        return True

    def close(self) -> None:
        pass


class LiveDemo:
    """Owns the simulated depot and the agent watching it."""

    def __init__(
        self, workdir: Path, sessions: sessionmaker[Session], *, seed: int | None = None
    ) -> None:
        depot_url = f"sqlite:///{workdir / 'depot.db'}"
        # The stock was counted before the history starts, so its timestamps come first.
        counted = dt.datetime.now(dt.UTC) - dt.timedelta(hours=HISTORY_HOURS + 1)
        init_database(depot_url, seeded_at=counted.replace(tzinfo=None, microsecond=0)).dispose()
        # Reads arrive on different worker threads; every call below holds self._lock.
        self.depot = create_engine(depot_url, connect_args={"check_same_thread": False})
        self._simulated_now: dt.datetime | None = None
        self.simulator = Simulator(self.depot, seed, clock=self._depot_clock)
        config = AgentConfig(
            source_name=SOURCE_NAME,
            source_url=depot_url,
            api_url="in-process",
            api_key="in-process",
            tables=TABLES,
            state_path=str(workdir / "agent-state.db"),
        )
        self.agent = Agent(
            config,
            SourceReader(engine=self.depot),
            StateStore(config.state_path, any_thread=True),
            LocalShipper(sessions),
            clock=self._now,
        )
        self._lock = threading.Lock()
        self._last_operation = time.monotonic()
        self._last_cycle = 0.0

    def _now(self) -> dt.datetime:
        return self._simulated_now or dt.datetime.now(dt.UTC)

    def _depot_clock(self) -> dt.datetime:
        return self._now().replace(tzinfo=None, microsecond=0)

    def play_history(self, operations: int = HISTORY_OPERATIONS, hours: float = HISTORY_HOURS):
        """Play past operations on a simulated clock, so the first visitor sees some history."""
        with self._lock:
            start = dt.datetime.now(dt.UTC) - dt.timedelta(hours=hours)
            spacing = dt.timedelta(hours=hours) / max(operations, 1)
            self._simulated_now = start
            self.agent.poll_once()  # first poll = baseline, reports nothing
            try:
                for n in range(operations):
                    # slot n is [n, n+1) * spacing; the last slot ends now, never later
                    jitter = self.simulator.random.uniform(0.1, 0.9)
                    self._simulated_now = start + spacing * (n + jitter)
                    self.simulator.step()
                    self.agent.poll_once()
            finally:
                self._simulated_now = None
            self.agent.cycle()  # ship everything and report in
            self._last_operation = self._last_cycle = time.monotonic()

    def tick(self) -> None:
        """Catch up with the operations due since the last read. Never fails a read."""
        try:
            with self._lock:
                now = time.monotonic()
                due = int((now - self._last_operation) // SECONDS_PER_OPERATION)
                if due == 0 and now - self._last_cycle < HEARTBEAT_SECONDS:
                    return
                for _ in range(min(due, MAX_CATCH_UP)):
                    self.simulator.step()
                if due > MAX_CATCH_UP:
                    self._last_operation = now
                else:
                    self._last_operation += due * SECONDS_PER_OPERATION
                self.agent.cycle()
                self._last_cycle = now
        except Exception:
            log.exception("demo tick failed")


def create_demo_app(workdir: Path | None = None, *, seed: int | None = None) -> FastAPI:
    workdir = workdir or Path(tempfile.mkdtemp(prefix="depot-demo-"))
    settings = Settings(
        database_url=f"sqlite:///{workdir / 'monitor.db'}",
        api_keys=frozenset({secrets.token_urlsafe(32)}),
        demo_banner=BANNER,
    )
    demo: LiveDemo | None = None

    def before_read() -> None:
        if demo is not None:
            demo.tick()

    app = create_app(settings, before_read=before_read)
    demo = LiveDemo(workdir, app.state.sessions, seed=seed)
    demo.play_history()
    app.state.demo = demo
    log.info("live demo %s ready in %s", __version__, workdir)
    return app


app = create_demo_app()
