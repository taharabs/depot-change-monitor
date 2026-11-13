from __future__ import annotations

import datetime as dt

import pytest
from fastapi.testclient import TestClient

from depot_monitor.demo.web import SECONDS_PER_OPERATION, SOURCE_NAME, create_demo_app


@pytest.fixture
def demo(tmp_path):
    app = create_demo_app(tmp_path, seed=7)
    with TestClient(app) as client:
        yield client, app.state.demo


def test_starts_with_history_and_an_online_agent(demo):
    client, _ = demo
    stats = client.get("/api/v1/stats").json()
    assert stats["total"] > 100
    assert {t["table"] for t in stats["by_table"]} >= {"stock", "stock_movements", "transfers"}
    [agent] = stats["agents"]
    assert agent["source"] == SOURCE_NAME
    assert agent["online"] and agent["pending"] == 0


def test_history_is_spread_over_hours_and_matches_row_times(demo):
    client, _ = demo
    events = client.get("/api/v1/events", params={"limit": 500}).json()
    detected = [dt.datetime.fromisoformat(e["detected_at"]) for e in events]
    assert max(detected) - min(detected) > dt.timedelta(hours=4)
    assert max(detected) <= dt.datetime.now(dt.UTC)  # history never lands in the future
    for e in events:
        if e["table"] == "stock_movements" and e["op"] == "insert":
            created = dt.datetime.fromisoformat(e["after"]["created_at"]).replace(tzinfo=dt.UTC)
            seen = dt.datetime.fromisoformat(e["detected_at"])
            assert abs(seen - created) < dt.timedelta(minutes=5)
        if e["table"] == "stock" and e["op"] == "update":
            assert e["after"]["updated_at"] >= e["before"]["updated_at"]  # time only moves forward


def test_reads_play_the_operations_that_are_due(demo):
    client, live = demo
    before = client.get("/api/v1/stats").json()["total"]
    live._last_operation -= 10 * SECONDS_PER_OPERATION  # pretend 30 s went by
    after = client.get("/api/v1/stats").json()["total"]
    assert after > before


def test_dashboard_says_it_is_a_demo(demo):
    client, _ = demo
    page = client.get("/").text
    assert "Live demo." in page and "github.com/taharabs/depot-change-monitor" in page


def test_nobody_can_post_events(demo):
    client, _ = demo
    payload = {"events": []}
    assert client.post("/api/v1/events", json=payload).status_code == 401
    assert (
        client.post("/api/v1/events", json=payload, headers={"X-API-Key": "demo-key"}).status_code
        == 401
    )
