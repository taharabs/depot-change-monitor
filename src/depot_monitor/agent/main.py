"""The agent loop: poll the source, queue what changed, ship the queue."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import signal
import threading
import uuid
from collections.abc import Callable

from .. import __version__
from .config import AgentConfig, TableSpec, load_config
from .differ import Change, detect_changes
from .shipper import Shipper
from .source import SourceReader
from .state import StateStore

log = logging.getLogger("depot_monitor.agent")


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class Agent:
    def __init__(
        self,
        config: AgentConfig,
        reader: SourceReader,
        state: StateStore,
        shipper: Shipper,
        *,
        clock: Callable[[], dt.datetime] = _utcnow,
    ) -> None:
        self.config = config
        self.reader = reader
        self.state = state
        self.shipper = shipper
        self._clock = clock

    def _event(self, spec: TableSpec, change: Change, detected_at: str) -> dict:
        row = change.after if change.after is not None else change.before
        return {
            "event_id": uuid.uuid4().hex,
            "source": self.config.source_name,
            "table": spec.name,
            "op": change.op,
            "key": {k: row[k] for k in spec.key},
            "before": change.before,
            "after": change.after,
            "changed_fields": list(change.changed_fields),
            "detected_at": detected_at,
        }

    def poll_table(self, spec: TableSpec) -> int:
        """Read one table, save what changed, return the number of events queued."""
        current = self.reader.fetch(spec)
        changes = detect_changes(
            self.state.load_hashes(spec.name),
            current,
            lambda keys: self.state.load_rows(spec.name, keys),
        )
        first_run = not self.state.has_baseline(spec.name)
        report = changes if (self.config.emit_initial or not first_run) else []
        detected_at = self._clock().isoformat()
        events = [self._event(spec, change, detected_at) for change in report]
        self.state.commit_poll(spec.name, changes, events)
        if first_run:
            log.info("%s: baseline taken (%d rows)", spec.name, len(current))
        elif events:
            log.info("%s: %d change(s)", spec.name, len(events))
        return len(events)

    def poll_once(self) -> int:
        """Poll every table. A table that fails is logged and retried on the next cycle."""
        queued = 0
        for spec in self.config.tables:
            try:
                queued += self.poll_table(spec)
            except Exception as exc:  # the source being down must not stop the agent
                log.error("%s: poll failed (%s: %s)", spec.name, type(exc).__name__, exc)
        return queued

    def flush(self) -> int:
        """Send queued events, oldest first. Stops at the first batch the API does not confirm."""
        sent = 0
        while batch := self.state.peek(self.config.batch_size):
            if not self.shipper.send([event for _, event in batch]):
                break
            self.state.ack(batch[-1][0])
            sent += len(batch)
        return sent

    def cycle(self) -> None:
        self.poll_once()
        sent = self.flush()
        pending = self.state.pending()
        if sent or pending:
            log.info("shipped %d event(s), %d waiting", sent, pending)
        self.shipper.heartbeat(self.config.source_name, pending, __version__)

    def run(self, stop: threading.Event) -> None:
        log.info(
            "watching %s every %ss as source %r",
            ", ".join(t.name for t in self.config.tables),
            self.config.poll_seconds,
            self.config.source_name,
        )
        while not stop.is_set():
            self.cycle()
            stop.wait(self.config.poll_seconds)


def build_agent(config: AgentConfig) -> Agent:
    return Agent(
        config,
        SourceReader(config.source_url),
        StateStore(config.state_path),
        Shipper(config.api_url, config.api_key),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Depot change monitor agent")
    parser.add_argument("--config", default="monitor.toml", help="path to the TOML config")
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--check", action="store_true", help="print the parsed config and exit")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise
    config = load_config(args.config)
    if args.check:
        print(
            json.dumps(
                {
                    "source": config.source_name,
                    "tables": [t.name for t in config.tables],
                    "poll_seconds": config.poll_seconds,
                },
                indent=2,
            )
        )
        return

    agent = build_agent(config)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    if args.once:
        agent.cycle()
    else:
        agent.run(stop)
    log.info("agent stopped")


if __name__ == "__main__":
    main()
