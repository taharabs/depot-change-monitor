from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from depot_monitor.agent.config import AgentConfig, TableSpec
from depot_monitor.agent.main import Agent
from depot_monitor.agent.shipper import Shipper
from depot_monitor.agent.source import SourceReader
from depot_monitor.agent.state import StateStore
from depot_monitor.api.app import Settings, create_app
from depot_monitor.demo.simulator import init_database

API_KEY = "test-key"

TABLES = (
    TableSpec("products", ("product_id",)),
    TableSpec("stock", ("warehouse_id", "product_id")),
    TableSpec("stock_movements", ("movement_id",)),
    TableSpec("transfers", ("transfer_id",)),
    TableSpec("transfer_lines", ("transfer_id", "line_no")),
)


class FakeShipper:
    """Stands in for the network: can be switched offline, remembers what it delivered."""

    def __init__(self) -> None:
        self.online = True
        self.delivered: list[dict] = []
        self.heartbeats: list[tuple] = []

    def send(self, events) -> bool:
        if self.online:
            self.delivered.extend(events)
        return self.online

    def heartbeat(self, source, pending, version) -> bool:
        self.heartbeats.append((source, pending, version))
        return self.online


@pytest.fixture
def depot(tmp_path):
    """A seeded synthetic depot database (SQLite file)."""
    engine = init_database(f"sqlite:///{tmp_path / 'depot.db'}")
    yield engine
    engine.dispose()


@pytest.fixture
def config(tmp_path, depot) -> AgentConfig:
    return AgentConfig(
        source_name="depot-test",
        source_url=str(depot.url),
        api_url="http://testserver",
        api_key=API_KEY,
        tables=TABLES,
        state_path=str(tmp_path / "state.db"),
        batch_size=50,
    )


@pytest.fixture
def api(tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'api.db'}", api_keys=frozenset({API_KEY})
    )
    with TestClient(create_app(settings)) as client:
        yield client


@pytest.fixture
def make_agent(config):
    made = []

    def factory(shipper=None, **overrides) -> Agent:
        cfg = config if not overrides else type(config)(**{**config.__dict__, **overrides})
        agent = Agent(
            cfg,
            SourceReader(cfg.source_url),
            StateStore(cfg.state_path),
            shipper or FakeShipper(),
        )
        made.append(agent)
        return agent

    yield factory
    for agent in made:
        agent.reader.close()
        agent.state.close()


@pytest.fixture
def api_shipper(api):
    """A real Shipper that talks to the in-process API."""
    return Shipper("http://testserver", API_KEY, client=api)
