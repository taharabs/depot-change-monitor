from depot_monitor.agent.differ import changed_fields, detect_changes, row_hash


def snapshot(rows):
    return {k: row_hash(r) for k, r in rows.items()}


def loader(rows):
    return lambda keys: {k: rows[k] for k in keys}


def test_hash_ignores_column_order():
    assert row_hash({"a": 1, "b": "x"}) == row_hash({"b": "x", "a": 1})
    assert row_hash({"a": 1}) != row_hash({"a": 2})


def test_no_change_means_no_events():
    rows = {"[1]": {"id": 1, "qty": 5}}
    assert detect_changes(snapshot(rows), rows, loader(rows)) == []


def test_insert_update_delete_are_told_apart():
    before = {"[1]": {"id": 1, "qty": 5}, "[2]": {"id": 2, "qty": 7}}
    after = {"[1]": {"id": 1, "qty": 9}, "[3]": {"id": 3, "qty": 1}}

    changes = detect_changes(snapshot(before), after, loader(before))

    by_op = {c.op: c for c in changes}
    assert [c.op for c in changes] == ["insert", "update", "delete"]
    assert by_op["insert"].after == {"id": 3, "qty": 1} and by_op["insert"].before is None
    assert by_op["update"].before["qty"] == 5 and by_op["update"].after["qty"] == 9
    assert by_op["update"].changed_fields == ("qty",)
    assert by_op["delete"].before == {"id": 2, "qty": 7} and by_op["delete"].after is None


def test_previous_rows_are_only_loaded_for_changed_keys():
    before = {f"[{i}]": {"id": i, "qty": i} for i in range(100)}
    after = dict(before)
    after["[42]"] = {"id": 42, "qty": 0}
    asked = []

    def spy(keys):
        asked.extend(keys)
        return {k: before[k] for k in keys}

    detect_changes(snapshot(before), after, spy)
    assert asked == ["[42]"]


def test_changed_fields_handles_null_and_added_columns():
    assert changed_fields({"a": None, "b": 1}, {"a": "x", "b": 1, "c": 2}) == ("a", "c")
