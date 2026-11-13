from __future__ import annotations

import datetime as dt
import hashlib
from decimal import Decimal

import pytest
from sqlalchemy import insert, text, update

from depot_monitor.agent.config import ConfigError, TableSpec, load_config
from depot_monitor.agent.source import SourceReader, key_id, normalize_value
from depot_monitor.agent.state import StateStore
from depot_monitor.demo.simulator import Simulator, products, stock, transfers
from tests.conftest import FakeShipper


def set_quantity(depot, warehouse, product, quantity):
    with depot.begin() as connection:
        connection.execute(
            update(stock)
            .where(stock.c.warehouse_id == warehouse, stock.c.product_id == product)
            .values(quantity=quantity)
        )


# -- source ---------------------------------------------------------------------------


def test_values_are_normalized_without_losing_precision():
    assert normalize_value(Decimal("14.50")) == "14.50"
    assert normalize_value(dt.datetime(2026, 11, 13, 17, 5)) == "2026-11-13T17:05:00"
    assert normalize_value(b"\x01\xff") == "01ff"
    assert normalize_value(None) is None


def test_composite_keys_identify_rows(depot):
    reader = SourceReader(engine=depot)
    rows = reader.fetch(TableSpec("stock", ("warehouse_id", "product_id")))
    assert len(rows) == 36
    assert key_id({"warehouse_id": 2, "product_id": 5}, ("warehouse_id", "product_id")) in rows


def test_column_subset_and_unknown_column(depot):
    reader = SourceReader(engine=depot)
    rows = reader.fetch(TableSpec("products", ("product_id",), columns=("product_id", "sku")))
    assert set(next(iter(rows.values()))) == {"product_id", "sku"}
    with pytest.raises(ConfigError):
        reader.fetch(TableSpec("stock", ("warehouse_id", "nope")))


# -- config ---------------------------------------------------------------------------


def test_config_reads_tables_from_file_and_secrets_from_env(tmp_path):
    path = tmp_path / "monitor.toml"
    path.write_text(
        '[agent]\nsource_name = "d1"\n'
        '[[tables]]\nname = "stock"\nkey = ["warehouse_id", "product_id"]\n'
        '[[tables]]\nname = "products"\nkey = "product_id"\n'
    )
    env = {"SOURCE_URL": "sqlite:///x.db", "API_URL": "https://api.example/", "API_KEY": "k"}
    config = load_config(path, env)
    assert config.api_url == "https://api.example"
    assert [t.key for t in config.tables] == [("warehouse_id", "product_id"), ("product_id",)]
    with pytest.raises(ConfigError, match="API_KEY"):
        load_config(path, {**env, "API_KEY": ""})


# -- polling --------------------------------------------------------------------------


def test_first_poll_takes_a_baseline_without_events(make_agent):
    agent = make_agent()
    assert agent.poll_once() == 0
    assert agent.state.pending() == 0
    assert len(agent.state.load_hashes("stock")) == 36


def test_emit_initial_reports_existing_rows(make_agent):
    agent = make_agent(emit_initial=True)
    assert agent.poll_once() == 12 + 36  # products + stock; the other tables start empty


def test_update_insert_delete_are_detected(make_agent, depot):
    agent = make_agent()
    agent.poll_once()

    set_quantity(depot, 1, 1, 999)
    with depot.begin() as connection:
        connection.execute(
            insert(transfers).values(
                transfer_id=77,
                from_warehouse=1,
                to_warehouse=2,
                status="draft",
                created_at=dt.datetime(2026, 11, 13, 9, 0),
            )
        )
        connection.execute(text("DELETE FROM products WHERE product_id = 12"))

    assert agent.poll_once() == 3
    events = {e["table"]: e for _, e in agent.state.peek(10)}

    assert events["stock"]["op"] == "update"
    assert events["stock"]["key"] == {"warehouse_id": 1, "product_id": 1}
    assert events["stock"]["changed_fields"] == ["quantity"]
    assert events["stock"]["after"]["quantity"] == 999
    assert events["transfers"]["op"] == "insert" and events["transfers"]["before"] is None
    assert events["products"]["op"] == "delete" and events["products"]["after"] is None
    assert events["products"]["before"]["sku"] == "BOX-CRT-S"
    assert agent.poll_once() == 0  # nothing new: the same change is never reported twice


def test_agent_works_with_read_only_access_and_leaves_the_source_untouched(
    make_agent, depot, tmp_path
):
    path = tmp_path / "depot.db"
    agent = make_agent(source_url=f"sqlite:///file:{path}?mode=ro&uri=true")
    agent.cycle()
    set_quantity(depot, 1, 2, 3)
    depot.dispose()
    fingerprint = hashlib.sha256(path.read_bytes()).hexdigest()

    assert agent.poll_once() == 1  # the change is seen through a read-only connection
    agent.cycle()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == fingerprint


def test_a_failing_table_does_not_stop_the_others(make_agent, depot):
    agent = make_agent(
        tables=(
            TableSpec("missing_table", ("id",)),
            TableSpec("stock", ("warehouse_id", "product_id")),
        )
    )
    agent.poll_once()
    set_quantity(depot, 3, 3, 1)
    assert agent.poll_once() == 1


# -- offline queue --------------------------------------------------------------------


def test_events_wait_while_offline_and_ship_in_order_afterwards(make_agent, depot):
    shipper = FakeShipper()
    agent = make_agent(shipper)
    agent.cycle()

    shipper.online = False
    for quantity in (10, 20, 30):
        set_quantity(depot, 1, 1, quantity)
        agent.cycle()
    assert agent.state.pending() == 3 and shipper.delivered == []

    shipper.online = True
    agent.cycle()
    assert agent.state.pending() == 0
    assert [e["after"]["quantity"] for e in shipper.delivered] == [10, 20, 30]


def test_queue_survives_a_restart(make_agent, config, depot):
    shipper = FakeShipper()
    shipper.online = False
    agent = make_agent(shipper)
    agent.cycle()
    set_quantity(depot, 2, 2, 5)
    agent.cycle()
    agent.state.close()

    reopened = StateStore(config.state_path)
    try:
        assert reopened.pending() == 1
        assert reopened.peek(1)[0][1]["after"]["quantity"] == 5
    finally:
        reopened.close()


def test_large_backlog_is_sent_in_batches(make_agent, depot):
    shipper = FakeShipper()
    agent = make_agent(shipper, batch_size=7)
    agent.cycle()
    simulator = Simulator(depot, seed=1)
    for _ in range(40):
        simulator.step()
    queued = agent.poll_once()
    assert queued > 7
    assert agent.flush() == queued and len(shipper.delivered) == queued


def test_commit_is_atomic(tmp_path):
    state = StateStore(str(tmp_path / "s.db"))
    from depot_monitor.agent.differ import Change

    good = Change("insert", "[1]", None, {"id": 1})
    with pytest.raises(TypeError):
        state.commit_poll("t", [good], [{"bad": object()}])  # cannot be serialized
    assert state.load_hashes("t") == {} and state.pending() == 0 and not state.has_baseline("t")
    state.close()


def test_decimal_change_is_reported_exactly(make_agent, depot):
    agent = make_agent()
    agent.poll_once()
    with depot.begin() as connection:
        connection.execute(
            update(products).where(products.c.product_id == 1).values(unit_cost=Decimal("4.35"))
        )
    agent.poll_once()
    event = agent.state.peek(1)[0][1]
    assert event["changed_fields"] == ["unit_cost"]
    assert (event["before"]["unit_cost"], event["after"]["unit_cost"]) == ("4.20", "4.35")
