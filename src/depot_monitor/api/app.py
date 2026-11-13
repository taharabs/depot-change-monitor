"""FastAPI application: ingest endpoint for agents, read endpoints and dashboard for people."""

from __future__ import annotations

import datetime as dt
import os
import secrets
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from .. import __version__
from .models import AgentStatus, Base, ChangeEvent
from .schemas import (
    AgentOut,
    EventBatch,
    EventIn,
    EventOut,
    Heartbeat,
    IngestResult,
    Stats,
    TableStats,
)

DASHBOARD = Path(__file__).with_name("dashboard.html")
AGENT_ONLINE_SECONDS = 90


@dataclass(frozen=True)
class Settings:
    database_url: str = "sqlite:///./monitor.db"
    api_keys: frozenset[str] = field(default_factory=frozenset)
    dashboard_user: str | None = None
    dashboard_password: str | None = None
    demo_banner: str | None = None  # HTML shown above the dashboard when set

    @classmethod
    def from_env(cls) -> Settings:
        keys = {k.strip() for k in os.environ.get("MONITOR_API_KEYS", "").split(",") if k.strip()}
        return cls(
            database_url=os.environ.get("DATABASE_URL", cls.database_url),
            api_keys=frozenset(keys),
            dashboard_user=os.environ.get("DASHBOARD_USER") or None,
            dashboard_password=os.environ.get("DASHBOARD_PASSWORD") or None,
        )


def _same(a: str, b: str) -> bool:
    """Constant-time string comparison (works for non-ASCII input too)."""
    return secrets.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def _utc(value: dt.datetime) -> dt.datetime:
    """SQLite drops the time zone; everything is stored in UTC, so put it back."""
    return value if value.tzinfo else value.replace(tzinfo=dt.UTC)


def _event_out(row: ChangeEvent) -> EventOut:
    return EventOut(
        id=row.id,
        event_id=row.event_id,
        source=row.source,
        table=row.table_name,
        op=row.op,
        key=row.key,
        before=row.before,
        after=row.after,
        changed_fields=row.changed_fields or [],
        detected_at=_utc(row.detected_at),
        received_at=_utc(row.received_at),
    )


def store_events(session: Session, events: Sequence[EventIn]) -> IngestResult:
    """Store events that are not stored yet. Sending the same event twice is safe."""
    unique = {event.event_id: event for event in events}
    known = set(
        session.scalars(select(ChangeEvent.event_id).where(ChangeEvent.event_id.in_(unique.keys())))
    )
    now = dt.datetime.now(dt.UTC)
    fresh = [event for event_id, event in unique.items() if event_id not in known]
    session.add_all(
        ChangeEvent(
            event_id=event.event_id,
            source=event.source,
            table_name=event.table,
            op=event.op,
            key=event.key,
            before=event.before,
            after=event.after,
            changed_fields=event.changed_fields,
            detected_at=event.detected_at,
            received_at=now,
        )
        for event in fresh
    )
    session.commit()
    return IngestResult(accepted=len(fresh), duplicates=len(events) - len(fresh))


def record_heartbeat(session: Session, beat: Heartbeat) -> None:
    agent = session.get(AgentStatus, beat.source) or AgentStatus(source=beat.source)
    agent.last_seen = dt.datetime.now(dt.UTC)
    agent.pending = beat.pending
    agent.version = beat.version
    session.add(agent)
    session.commit()


def create_app(
    settings: Settings | None = None,
    *,
    before_read: Callable[[], None] | None = None,
) -> FastAPI:
    """Build the API. `before_read` runs before every read request (the live demo uses it)."""
    settings = settings or Settings.from_env()
    if not settings.api_keys:
        raise RuntimeError("MONITOR_API_KEYS is empty: agents could not authenticate")

    connect_args = (
        {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
    )
    engine = create_engine(settings.database_url, pool_pre_ping=True, connect_args=connect_args)
    Base.metadata.create_all(engine)
    make_session = sessionmaker(engine, expire_on_commit=False)

    app = FastAPI(
        title="Depot Change Monitor API",
        version=__version__,
        description="Receives change events from depot agents and serves them to the dashboard.",
    )
    app.state.sessions = make_session
    basic = HTTPBasic(auto_error=False)

    def db() -> Iterator[Session]:
        with make_session() as session:
            yield session

    def require_agent(x_api_key: str | None = Header(default=None)) -> None:
        """Agents authenticate with a shared secret sent in the X-API-Key header."""
        supplied = x_api_key or ""
        if not any(_same(supplied, key) for key in settings.api_keys):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing API key")

    def require_reader(credentials: HTTPBasicCredentials | None = Depends(basic)) -> None:
        """Dashboard and read endpoints use HTTP Basic auth when a user is configured."""
        if settings.dashboard_user is not None:
            check_reader(credentials)
        if before_read is not None:
            before_read()

    def check_reader(credentials: HTTPBasicCredentials | None) -> None:
        ok = (
            credentials is not None
            and _same(credentials.username, settings.dashboard_user)
            and _same(credentials.password, settings.dashboard_password or "")
        )
        if not ok:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "authentication required",
                headers={"WWW-Authenticate": 'Basic realm="depot-monitor"'},
            )

    @app.get("/health", tags=["ops"])
    def health(session: Session = Depends(db)) -> dict:
        session.execute(select(1))
        return {"status": "ok", "version": __version__}

    @app.post(
        "/api/v1/events",
        response_model=IngestResult,
        dependencies=[Depends(require_agent)],
        tags=["agent"],
    )
    def ingest(batch: EventBatch, session: Session = Depends(db)) -> IngestResult:
        """Store a batch of events. Sending the same event twice is safe: it is stored once."""
        return store_events(session, batch.events)

    @app.post(
        "/api/v1/heartbeat",
        status_code=status.HTTP_204_NO_CONTENT,
        dependencies=[Depends(require_agent)],
        tags=["agent"],
    )
    def heartbeat(beat: Heartbeat, session: Session = Depends(db)) -> None:
        record_heartbeat(session, beat)

    @app.get(
        "/api/v1/events",
        response_model=list[EventOut],
        dependencies=[Depends(require_reader)],
        tags=["read"],
    )
    def list_events(
        table: str | None = None,
        op: Literal["insert", "update", "delete"] | None = None,
        source: str | None = None,
        since: dt.datetime | None = Query(None, description="only events detected after this"),
        before_id: int | None = Query(None, description="page backwards from this event id"),
        limit: int = Query(50, ge=1, le=500),
        session: Session = Depends(db),
    ) -> list[EventOut]:
        """Newest events first, with optional filters."""
        query = select(ChangeEvent).order_by(ChangeEvent.id.desc()).limit(limit)
        if table:
            query = query.where(ChangeEvent.table_name == table)
        if op:
            query = query.where(ChangeEvent.op == op)
        if source:
            query = query.where(ChangeEvent.source == source)
        if since:
            query = query.where(ChangeEvent.detected_at > since)
        if before_id:
            query = query.where(ChangeEvent.id < before_id)
        return [_event_out(row) for row in session.scalars(query)]

    @app.get(
        "/api/v1/stats",
        response_model=Stats,
        dependencies=[Depends(require_reader)],
        tags=["read"],
    )
    def stats(session: Session = Depends(db)) -> Stats:
        now = dt.datetime.now(dt.UTC)
        total = session.scalar(select(func.count(ChangeEvent.id))) or 0
        last_24h = (
            session.scalar(
                select(func.count(ChangeEvent.id)).where(
                    ChangeEvent.detected_at > now - dt.timedelta(hours=24)
                )
            )
            or 0
        )
        last_event_at = session.scalar(select(func.max(ChangeEvent.detected_at)))

        tables: dict[str, TableStats] = {}
        grouped = select(ChangeEvent.table_name, ChangeEvent.op, func.count(ChangeEvent.id))
        for name, op, count in session.execute(
            grouped.group_by(ChangeEvent.table_name, ChangeEvent.op)
        ):
            setattr(tables.setdefault(name, TableStats(table=name)), op, count)

        agents = [
            AgentOut(
                source=agent.source,
                last_seen=_utc(agent.last_seen),
                pending=agent.pending,
                version=agent.version,
                online=(now - _utc(agent.last_seen)).total_seconds() < AGENT_ONLINE_SECONDS,
            )
            for agent in session.scalars(select(AgentStatus).order_by(AgentStatus.source))
        ]
        return Stats(
            total=total,
            last_24h=last_24h,
            last_event_at=_utc(last_event_at) if last_event_at else None,
            by_table=sorted(tables.values(), key=lambda t: t.table),
            agents=agents,
        )

    @app.get(
        "/",
        response_class=HTMLResponse,
        include_in_schema=False,
        dependencies=[Depends(require_reader)],
    )
    def dashboard() -> str:
        page = DASHBOARD.read_text(encoding="utf-8")
        if settings.demo_banner:
            page = page.replace("<main>", "<main>\n" + settings.demo_banner, 1)
        return page

    return app
