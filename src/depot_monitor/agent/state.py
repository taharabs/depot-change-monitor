"""The agent's local memory, in one SQLite file.

It holds two things:

* ``snapshot`` - the last known content of every watched table, so the next
  poll has something to compare against;
* ``outbox``   - change events that still have to reach the API. Events wait
  here while the network is down and are sent in order when it comes back.

A poll writes the new snapshot and its events in a single transaction: either
both are saved or neither is, so a crash can never lose or duplicate a change.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Sequence
from typing import Any

from .differ import Change, row_hash

Row = dict[str, Any]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    table_name TEXT NOT NULL,
    key_id     TEXT NOT NULL,
    row_hash   TEXT NOT NULL,
    row_json   TEXT NOT NULL,
    PRIMARY KEY (table_name, key_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS baseline (
    table_name TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS outbox (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_json TEXT NOT NULL
);
"""


class StateStore:
    def __init__(self, path: str = "agent-state.db", *, any_thread: bool = False) -> None:
        # any_thread=True lets several threads use the store; the caller must serialize calls.
        self._db = sqlite3.connect(
            path, isolation_level=None, check_same_thread=not any_thread
        )  # explicit transactions below
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)

    # -- snapshot -----------------------------------------------------------------

    def has_baseline(self, table: str) -> bool:
        cur = self._db.execute("SELECT 1 FROM baseline WHERE table_name = ?", (table,))
        return cur.fetchone() is not None

    def load_hashes(self, table: str) -> dict[str, str]:
        cur = self._db.execute(
            "SELECT key_id, row_hash FROM snapshot WHERE table_name = ?", (table,)
        )
        return dict(cur.fetchall())

    def load_rows(self, table: str, key_ids: Iterable[str]) -> dict[str, Row]:
        rows: dict[str, Row] = {}
        for key in key_ids:
            cur = self._db.execute(
                "SELECT row_json FROM snapshot WHERE table_name = ? AND key_id = ?", (table, key)
            )
            found = cur.fetchone()
            if found is not None:
                rows[key] = json.loads(found[0])
        return rows

    def commit_poll(self, table: str, changes: Sequence[Change], events: Sequence[dict]) -> None:
        """Save one poll's result atomically: snapshot rows, baseline mark and outbox events."""
        self._db.execute("BEGIN IMMEDIATE")
        try:
            for change in changes:
                if change.op == "delete":
                    self._db.execute(
                        "DELETE FROM snapshot WHERE table_name = ? AND key_id = ?",
                        (table, change.key_id),
                    )
                else:
                    self._db.execute(
                        "INSERT OR REPLACE INTO snapshot VALUES (?, ?, ?, ?)",
                        (
                            table,
                            change.key_id,
                            row_hash(change.after),
                            json.dumps(change.after, ensure_ascii=False),
                        ),
                    )
            self._db.execute("INSERT OR IGNORE INTO baseline VALUES (?)", (table,))
            self._db.executemany(
                "INSERT INTO outbox (event_json) VALUES (?)",
                [(json.dumps(event, ensure_ascii=False),) for event in events],
            )
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        self._db.execute("COMMIT")

    # -- outbox -------------------------------------------------------------------

    def peek(self, limit: int) -> list[tuple[int, dict]]:
        """The oldest `limit` unsent events, as (sequence number, event)."""
        cur = self._db.execute("SELECT seq, event_json FROM outbox ORDER BY seq LIMIT ?", (limit,))
        return [(seq, json.loads(payload)) for seq, payload in cur.fetchall()]

    def ack(self, up_to_seq: int) -> None:
        """Forget events the API has confirmed, up to and including `up_to_seq`."""
        self._db.execute("DELETE FROM outbox WHERE seq <= ?", (up_to_seq,))

    def pending(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]

    def close(self) -> None:
        self._db.close()
