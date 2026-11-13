from __future__ import annotations

import datetime as dt

import pytest
from fastapi.testclient import TestClient

from depot_monitor.api.app import Settings, create_app
from depot_monitor.demo.simulator import Simulator
from tests.conftest import API_KEY

AUTH = {"X-API-Key": API_KEY}


def event(n: int, *, table="stock", op="update", minutes_ago=0) -> dict:
    detected = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=minutes_ago)
    return {
        "event_id": f"event-{n:04d}",
        "source": "depot-test",
        "table": table,
        "op": op,
        "key": {"id": n},
        "before": None if op == "insert" else {"id": n, "qty": 1},
        "after": None if op == "delete" else {"id": n, "qty": 2},
        "changed_fields": ["qty"] if op == "update" else [],
        "detected_at": detected.isoformat(),
    }


def test_health(api):
    assert api.get("/health").json()["status"] == "ok"


def test_ingest_requires_a_valid_key(api):
    payload = {"events": [event(1)]}
    assert api.post("/api/v1/events", json=payload).status_code == 401
    assert (
        api.post("/api/v1/events", json=payload, headers={"X-API-Key": "nope"}).status_code == 401
    )
    assert api.post("/api/v1/events", json=payload, headers=AUTH).status_code == 200
    assert api.post("/api/v1/heartbeat", json={"source": "d", "pending": 0}).status_code == 401


def test_ingest_is_idempotent(api):
    batch = {"events": [event(1), event(2)]}
    assert api.post("/api/v1/events", json=batch, headers=AUTH).json() == {
        "accepted": 2,
        "duplicates": 0,
    }
    again = {"events": [event(2), event(3)]}
    assert api.post("/api/v1/events", json=again, headers=AUTH).json() == {
        "accepted": 1,
        "duplicates": 1,
    }
    assert api.get("/api/v1/stats").json()["total"] == 3


def test_invalid_events_are_rejected(api):
    bad = event(1) | {"op": "truncate"}
    assert api.post("/api/v1/events", json={"events": [bad]}, headers=AUTH).status_code == 422


def test_events_are_listed_newest_first_with_filters_and_paging(api):
    events = [
        event(1, table="stock"),
        event(2, table="transfers", op="insert"),
        event(3, table="stock", op="delete"),
        event(4, table="stock"),
    ]
    api.post("/api/v1/events", json={"events": events}, headers=AUTH)

    listed = api.get("/api/v1/events").json()
    assert [e["event_id"] for e in listed] == [
        "event-0004",
        "event-0003",
        "event-0002",
        "event-0001",
    ]
    assert [e["key"]["id"] for e in api.get("/api/v1/events?table=stock&op=update").json()] == [
        4,
        1,
    ]
    page = api.get("/api/v1/events?limit=2").json()
    older = api.get(f"/api/v1/events?limit=2&before_id={page[-1]['id']}").json()
    assert [e["key"]["id"] for e in older] == [2, 1]
    assert api.get("/api/v1/events?op=bogus").status_code == 422


def test_stats_group_by_table_and_track_agents(api):
    events = [
        event(1),
        event(2, op="insert"),
        event(3, table="transfers", op="delete"),
        event(4, minutes_ago=60 * 30),
    ]
    api.post("/api/v1/events", json={"events": events}, headers=AUTH)
    api.post(
        "/api/v1/heartbeat",
        json={"source": "depot-test", "pending": 4, "version": "0.1.0"},
        headers=AUTH,
    )

    stats = api.get("/api/v1/stats").json()
    assert stats["total"] == 4 and stats["last_24h"] == 3
    assert stats["by_table"] == [
        {"table": "stock", "insert": 1, "update": 2, "delete": 0},
        {"table": "transfers", "insert": 0, "update": 0, "delete": 1},
    ]
    assert stats["agents"][0] | {"last_seen": None} == {
        "source": "depot-test",
        "pending": 4,
        "version": "0.1.0",
        "online": True,
        "last_seen": None,
    }


def test_dashboard_is_served(api):
    page = api.get("/")
    assert page.status_code == 200 and "Depot change monitor" in page.text


def test_read_endpoints_can_be_password_protected(tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'p.db'}",
        api_keys=frozenset({API_KEY}),
        dashboard_user="ops",
        dashboard_password="s3cret",
    )
    with TestClient(create_app(settings)) as client:
        assert client.get("/api/v1/stats").status_code == 401
        assert client.get("/").headers["www-authenticate"].startswith("Basic")
        assert client.get("/api/v1/stats", auth=("ops", "wrong")).status_code == 401
        assert client.get("/api/v1/stats", auth=("ops", "s3cret")).status_code == 200
        assert client.get("/health").status_code == 200  # stays open for uptime checks


def test_api_refuses_to_start_without_keys(tmp_path):
    with pytest.raises(RuntimeError):
        create_app(Settings(database_url=f"sqlite:///{tmp_path / 'x.db'}"))


def test_end_to_end_agent_to_api(make_agent, api_shipper, api, depot):
    """Simulated warehouse activity ends up in the API exactly once, even after a resend."""
    agent = make_agent(api_shipper)
    agent.cycle()
    simulator = Simulator(depot, seed=7)
    for _ in range(25):
        simulator.step()
    queued = agent.poll_once()
    batch = [e for _, e in agent.state.peek(1000)]
    assert queued == len(batch) > 0

    agent.cycle()
    assert agent.state.pending() == 0
    # the network "lost" the confirmation and the agent sends everything again:
    assert api_shipper.send(batch) is True

    stats = api.get("/api/v1/stats").json()
    assert stats["total"] == queued
    assert {t["table"] for t in stats["by_table"]} >= {"stock", "stock_movements"}
    assert stats["agents"][0]["source"] == "depot-test" and stats["agents"][0]["pending"] == 0
