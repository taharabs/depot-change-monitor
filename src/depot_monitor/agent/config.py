"""Agent configuration: watched tables come from a TOML file, secrets from the environment."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


class ConfigError(ValueError):
    """The configuration is missing something or is inconsistent."""


@dataclass(frozen=True)
class TableSpec:
    """One watched table. `key` is the column (or columns) that identifies a row."""

    name: str
    key: tuple[str, ...]
    schema: str | None = None
    columns: tuple[str, ...] | None = None  # None = every column

    def __post_init__(self) -> None:
        if not self.key:
            raise ConfigError(f"table {self.name!r}: 'key' must name at least one column")
        if self.columns is not None:
            missing = [k for k in self.key if k not in self.columns]
            if missing:
                raise ConfigError(f"table {self.name!r}: key columns {missing} not in 'columns'")


@dataclass(frozen=True)
class AgentConfig:
    source_name: str
    source_url: str
    api_url: str
    api_key: str
    tables: tuple[TableSpec, ...]
    poll_seconds: float = 10.0
    batch_size: int = 200
    state_path: str = "agent-state.db"
    emit_initial: bool = False  # True = report every existing row as an insert on first run


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"environment variable {name} is required")
    return value


def load_config(path: str | Path, env: Mapping[str, str] | None = None) -> AgentConfig:
    """Read the TOML file at `path`; connection URLs and the API key come from `env`."""
    env = os.environ if env is None else env
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)

    agent = raw.get("agent", {})
    tables = []
    for item in raw.get("tables", []):
        if "name" not in item or "key" not in item:
            raise ConfigError("every [[tables]] entry needs 'name' and 'key'")
        key = item["key"]
        tables.append(
            TableSpec(
                name=item["name"],
                key=tuple([key] if isinstance(key, str) else key),
                schema=item.get("schema"),
                columns=tuple(item["columns"]) if "columns" in item else None,
            )
        )
    if not tables:
        raise ConfigError("no [[tables]] configured: nothing to watch")
    names = [t.name for t in tables]
    if len(set(names)) != len(names):
        raise ConfigError("a table is listed twice in [[tables]]")

    return AgentConfig(
        source_name=agent.get("source_name", "depot"),
        source_url=_require(env, "SOURCE_URL"),
        api_url=_require(env, "API_URL").rstrip("/"),
        api_key=_require(env, "API_KEY"),
        tables=tuple(tables),
        poll_seconds=float(env.get("POLL_SECONDS", agent.get("poll_seconds", 10))),
        batch_size=int(agent.get("batch_size", 200)),
        state_path=env.get("STATE_PATH", agent.get("state_path", "agent-state.db")),
        emit_initial=bool(agent.get("emit_initial", False)),
    )
