"""Integration test against a real SQL Server.

Runs in CI (see .github/workflows/ci.yml). To run it yourself:

    docker run -d -p 1433:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Ci_Passw0rd1 \
        mcr.microsoft.com/mssql/server:2022-latest
    MSSQL_ADMIN_URL=mssql+pymssql://sa:Ci_Passw0rd1@localhost:1433/depot_it \
        python -m pytest -m integration
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from depot_monitor.agent.config import AgentConfig
from depot_monitor.agent.main import Agent
from depot_monitor.agent.source import SourceReader
from depot_monitor.agent.state import StateStore
from depot_monitor.demo.simulator import Simulator, init_database
from tests.conftest import TABLES, FakeShipper

ADMIN_URL = os.environ.get("MSSQL_ADMIN_URL")
READER_PASSWORD = "Reader_Passw0rd1"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not ADMIN_URL, reason="MSSQL_ADMIN_URL is not set"),
]


@pytest.fixture(scope="module")
def admin():
    engine = init_database(ADMIN_URL, reader_password=READER_PASSWORD, timeout=180)
    yield engine
    engine.dispose()


@pytest.fixture
def reader_url(admin) -> str:
    url = make_url(ADMIN_URL).set(username="monitor_reader", password=READER_PASSWORD)
    return url.render_as_string(hide_password=False)


def test_reader_login_cannot_write(reader_url):
    engine = create_engine(reader_url)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM products")).scalar() == 12
        with pytest.raises(DBAPIError):
            connection.execute(text("UPDATE stock SET quantity = 0"))
    engine.dispose()


def test_agent_detects_changes_through_the_read_only_login(admin, reader_url, tmp_path):
    config = AgentConfig(
        "depot-it", reader_url, "http://unused", "k", TABLES, state_path=str(tmp_path / "state.db")
    )
    shipper = FakeShipper()
    agent = Agent(config, SourceReader(reader_url), StateStore(config.state_path), shipper)
    try:
        agent.cycle()
        assert shipper.delivered == []  # baseline only

        simulator = Simulator(admin, seed=3)
        actions = [simulator.step() for _ in range(30)]
        agent.cycle()

        tables = {e["table"] for e in shipper.delivered}
        assert {"stock", "stock_movements"} <= tables, actions
        updates = [e for e in shipper.delivered if e["table"] == "stock" and e["op"] == "update"]
        assert updates and all("quantity" in e["changed_fields"] for e in updates)
        assert agent.poll_once() == 0
    finally:
        agent.reader.close()
        agent.state.close()
