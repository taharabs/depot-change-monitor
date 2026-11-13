"""Request and response shapes of the HTTP API."""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    event_id: str = Field(min_length=8, max_length=64)
    source: str = Field(min_length=1, max_length=100)
    table: str = Field(min_length=1, max_length=128)
    op: Literal["insert", "update", "delete"]
    key: dict[str, Any]
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    changed_fields: list[str] = Field(default_factory=list)
    detected_at: dt.datetime


class EventBatch(BaseModel):
    events: list[EventIn] = Field(max_length=1000)


class IngestResult(BaseModel):
    accepted: int
    duplicates: int


class Heartbeat(BaseModel):
    source: str = Field(min_length=1, max_length=100)
    pending: int = Field(ge=0)
    version: str = Field(default="", max_length=32)


class EventOut(BaseModel):
    id: int
    event_id: str
    source: str
    table: str
    op: str
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    changed_fields: list[str]
    detected_at: dt.datetime
    received_at: dt.datetime


class TableStats(BaseModel):
    table: str
    insert: int = 0
    update: int = 0
    delete: int = 0


class AgentOut(BaseModel):
    source: str
    last_seen: dt.datetime
    pending: int
    version: str
    online: bool


class Stats(BaseModel):
    total: int
    last_24h: int
    last_event_at: dt.datetime | None
    by_table: list[TableStats]
    agents: list[AgentOut]
