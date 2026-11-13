"""Read-only access to the source database.

This module is the only place that talks to the source. It issues nothing but
SELECT statements built by SQLAlchemy from the reflected table definition, so
the agent works with a login that has read permission only.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, MetaData, Table, create_engine, select

from .config import ConfigError, TableSpec

Row = dict[str, Any]


def normalize_value(value: Any) -> Any:
    """Turn a database value into something JSON can carry, without losing precision."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


def key_id(row: Row, key: tuple[str, ...]) -> str:
    """Stable text identifier of a row, built from its key columns in order."""
    return json.dumps([row[k] for k in key], separators=(",", ":"), ensure_ascii=False)


class SourceReader:
    def __init__(self, url: str | None = None, *, engine: Engine | None = None) -> None:
        if engine is None:
            if url is None:
                raise ValueError("pass a database URL or an engine")
            engine = create_engine(url, pool_pre_ping=True)
        self.engine = engine
        self._metadata = MetaData()
        self._tables: dict[str, Table] = {}

    def _table(self, spec: TableSpec) -> Table:
        if spec.name not in self._tables:
            table = Table(spec.name, self._metadata, schema=spec.schema, autoload_with=self.engine)
            wanted = spec.columns or tuple(c.name for c in table.columns)
            missing = [c for c in (*spec.key, *wanted) if c not in table.columns]
            if missing:
                raise ConfigError(f"table {spec.name!r} has no column(s) {sorted(set(missing))}")
            self._tables[spec.name] = table
        return self._tables[spec.name]

    def fetch(self, spec: TableSpec) -> dict[str, Row]:
        """Return the table's current rows as {key_id: row}."""
        table = self._table(spec)
        names = spec.columns or tuple(c.name for c in table.columns)
        statement = select(*(table.columns[name] for name in names))
        rows: dict[str, Row] = {}
        with self.engine.connect() as connection:
            for record in connection.execute(statement).mappings():
                row = {name: normalize_value(record[name]) for name in names}
                rows[key_id(row, spec.key)] = row
        return rows

    def close(self) -> None:
        self.engine.dispose()
