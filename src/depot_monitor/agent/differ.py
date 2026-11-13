"""Compare two snapshots of a table and describe what changed."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

Row = dict[str, Any]
Op = Literal["insert", "update", "delete"]


def row_hash(row: Mapping[str, Any]) -> str:
    """Fingerprint of a row. Two rows with the same values always hash the same."""
    canonical = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Change:
    op: Op
    key_id: str
    before: Row | None
    after: Row | None
    changed_fields: tuple[str, ...] = field(default=())


def changed_fields(before: Mapping[str, Any], after: Mapping[str, Any]) -> tuple[str, ...]:
    names = sorted(set(before) | set(after))
    return tuple(n for n in names if before.get(n) != after.get(n))


def detect_changes(
    previous_hashes: Mapping[str, str],
    current: Mapping[str, Row],
    load_previous: Callable[[Iterable[str]], Mapping[str, Row]],
) -> list[Change]:
    """Diff the stored snapshot against the rows just read.

    `previous_hashes` maps key_id -> row hash from the last poll. Full previous
    rows are loaded (through `load_previous`) only for rows that changed or
    disappeared, so an unchanged table costs one hash comparison per row.
    """
    current_hashes = {k: row_hash(row) for k, row in current.items()}
    inserted = sorted(current_hashes.keys() - previous_hashes.keys())
    deleted = sorted(previous_hashes.keys() - current_hashes.keys())
    updated = sorted(
        k
        for k in current_hashes.keys() & previous_hashes.keys()
        if current_hashes[k] != previous_hashes[k]
    )
    before_rows = load_previous([*updated, *deleted]) if (updated or deleted) else {}

    changes = [Change("insert", k, None, current[k]) for k in inserted]
    for k in updated:
        before = before_rows[k]
        changes.append(Change("update", k, before, current[k], changed_fields(before, current[k])))
    changes.extend(Change("delete", k, before_rows[k], None) for k in deleted)
    return changes
