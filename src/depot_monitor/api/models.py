"""Database tables of the API (PostgreSQL in production, SQLite for tests and local demos)."""

from __future__ import annotations

import datetime as dt

from sqlalchemy import JSON, DateTime, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ChangeEvent(Base):
    __tablename__ = "change_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True)  # makes ingestion idempotent
    source: Mapped[str] = mapped_column(String(100))
    table_name: Mapped[str] = mapped_column(String(128))
    op: Mapped[str] = mapped_column(String(10))
    key: Mapped[dict] = mapped_column(JSON)
    before: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    after: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    changed_fields: Mapped[list] = mapped_column(JSON, default=list)
    detected_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_change_events_table_detected", "table_name", "detected_at"),
        Index("ix_change_events_detected", "detected_at"),
    )


class AgentStatus(Base):
    __tablename__ = "agent_status"

    source: Mapped[str] = mapped_column(String(100), primary_key=True)
    last_seen: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True))
    pending: Mapped[int] = mapped_column(Integer, default=0)
    version: Mapped[str] = mapped_column(String(32), default="")
