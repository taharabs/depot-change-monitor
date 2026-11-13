"""Stand-in for the warehouse software: creates a small depot database and keeps it busy.

Everything here is synthetic. The simulator is the only component that writes
to the source database; the agent connects with a separate read-only login
that the simulator creates on SQL Server.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import random
import time
from decimal import Decimal

from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    delete,
    func,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.engine import make_url

log = logging.getLogger("depot_monitor.simulator")

metadata = MetaData()

products = Table(
    "products",
    metadata,
    Column("product_id", Integer, primary_key=True, autoincrement=False),
    Column("sku", String(32), nullable=False),
    Column("name", String(120), nullable=False),
    Column("unit_cost", Numeric(10, 2), nullable=False),
)
stock = Table(
    "stock",
    metadata,
    Column("warehouse_id", Integer, primary_key=True, autoincrement=False),
    Column(
        "product_id",
        Integer,
        ForeignKey("products.product_id"),
        primary_key=True,
        autoincrement=False,
    ),
    Column("quantity", Integer, nullable=False),
    Column("updated_at", DateTime, nullable=False),
)
stock_movements = Table(
    "stock_movements",
    metadata,
    Column("movement_id", Integer, primary_key=True, autoincrement=True),
    Column("warehouse_id", Integer, nullable=False),
    Column("product_id", Integer, nullable=False),
    Column("kind", String(16), nullable=False),  # receipt | issue | transfer_out | transfer_in
    Column("quantity", Integer, nullable=False),
    Column("created_at", DateTime, nullable=False),
)
transfers = Table(
    "transfers",
    metadata,
    Column("transfer_id", Integer, primary_key=True, autoincrement=True),
    Column("from_warehouse", Integer, nullable=False),
    Column("to_warehouse", Integer, nullable=False),
    Column("status", String(16), nullable=False),  # draft | shipped | received
    Column("created_at", DateTime, nullable=False),
)
transfer_lines = Table(
    "transfer_lines",
    metadata,
    Column(
        "transfer_id",
        Integer,
        ForeignKey("transfers.transfer_id"),
        primary_key=True,
        autoincrement=False,
    ),
    Column("line_no", Integer, primary_key=True, autoincrement=False),
    Column("product_id", Integer, nullable=False),
    Column("quantity", Integer, nullable=False),
)

WAREHOUSES = (1, 2, 3)
CATALOG = [
    ("PAP-A4-080", "Offset paper A4 80 g, ream", "4.20"),
    ("PAP-A3-120", "Coated paper A3 120 g, ream", "9.80"),
    ("CRD-SBS-300", "SBS board 300 g, sheet", "0.35"),
    ("INK-CYN-1K", "Process cyan ink, 1 kg", "14.50"),
    ("INK-MAG-1K", "Process magenta ink, 1 kg", "14.50"),
    ("INK-YEL-1K", "Process yellow ink, 1 kg", "13.90"),
    ("INK-BLK-1K", "Process black ink, 1 kg", "11.75"),
    ("PLT-CTP-745", "Thermal CTP plate 745x605", "3.10"),
    ("CHM-DEV-20L", "Plate developer, 20 L", "42.00"),
    ("VRN-GLS-5K", "Gloss varnish, 5 kg", "27.30"),
    ("GLU-HOT-25K", "Hot-melt glue, 25 kg", "61.00"),
    ("BOX-CRT-S", "Shipping carton, small", "0.48"),
]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None, microsecond=0)


def _wait_for(engine: Engine, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception as exc:
            if time.monotonic() > deadline:
                raise
            log.info("database not ready yet (%s), retrying", type(exc).__name__)
            time.sleep(3)


def _prepare_sql_server(url: str, reader_password: str | None, timeout: float) -> None:
    """Create the database and, if asked, a login that can only read it."""
    target = make_url(url)
    master = create_engine(target.set(database="master"), isolation_level="AUTOCOMMIT")
    _wait_for(master, timeout)
    database = target.database or ""
    if not database.replace("_", "").isalnum():
        raise ValueError(f"unsupported database name: {database!r}")
    with master.connect() as connection:
        connection.execute(text(f"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}]"))
        if reader_password:
            password = reader_password.replace("'", "''")
            connection.execute(
                text(
                    "IF NOT EXISTS ("
                    "SELECT 1 FROM sys.server_principals WHERE name = 'monitor_reader') "
                    f"CREATE LOGIN monitor_reader WITH PASSWORD = '{password}', CHECK_POLICY = OFF"
                )
            )
    master.dispose()
    if reader_password:
        scoped = create_engine(url, isolation_level="AUTOCOMMIT")
        with scoped.connect() as connection:
            connection.execute(
                text(
                    "IF NOT EXISTS ("
                    "SELECT 1 FROM sys.database_principals WHERE name = 'monitor_reader') "
                    "CREATE USER monitor_reader FOR LOGIN monitor_reader"
                )
            )
            connection.execute(text("ALTER ROLE db_datareader ADD MEMBER monitor_reader"))
        scoped.dispose()


def init_database(url: str, *, reader_password: str | None = None, timeout: float = 120) -> Engine:
    """Create tables and seed data if the database is empty. Safe to call again."""
    if make_url(url).get_backend_name() == "mssql":
        _prepare_sql_server(url, reader_password, timeout)
    engine = create_engine(url, pool_pre_ping=True)
    _wait_for(engine, timeout)
    metadata.create_all(engine)
    with engine.begin() as connection:
        if connection.scalar(select(func.count()).select_from(products)) == 0:
            now = _now()
            connection.execute(
                insert(products),
                [
                    {"product_id": i, "sku": sku, "name": name, "unit_cost": Decimal(cost)}
                    for i, (sku, name, cost) in enumerate(CATALOG, start=1)
                ],
            )
            connection.execute(
                insert(stock),
                [
                    {
                        "warehouse_id": w,
                        "product_id": p,
                        "quantity": 100 + 10 * ((w * p) % 7),
                        "updated_at": now,
                    }
                    for w in WAREHOUSES
                    for p in range(1, len(CATALOG) + 1)
                ],
            )
            log.info("seeded %d products in %d warehouses", len(CATALOG), len(WAREHOUSES))
    return engine


class Simulator:
    """Performs one random, realistic warehouse operation per step."""

    def __init__(self, engine: Engine, seed: int | None = None) -> None:
        self.engine = engine
        self.random = random.Random(seed)

    def _move(self, connection, warehouse: int, product: int, delta: int, kind: str) -> None:
        now = _now()
        connection.execute(
            update(stock)
            .where(stock.c.warehouse_id == warehouse, stock.c.product_id == product)
            .values(quantity=stock.c.quantity + delta, updated_at=now)
        )
        connection.execute(
            insert(stock_movements).values(
                warehouse_id=warehouse,
                product_id=product,
                kind=kind,
                quantity=abs(delta),
                created_at=now,
            )
        )

    def receipt(self, connection) -> str:
        w, p = self.random.choice(WAREHOUSES), self.random.randint(1, len(CATALOG))
        self._move(connection, w, p, self.random.randint(5, 60), "receipt")
        return "receipt"

    def issue(self, connection) -> str:
        w, p = self.random.choice(WAREHOUSES), self.random.randint(1, len(CATALOG))
        available = connection.scalar(
            select(stock.c.quantity).where(stock.c.warehouse_id == w, stock.c.product_id == p)
        )
        if available <= 0:
            return self.receipt(connection)
        self._move(connection, w, p, -self.random.randint(1, min(25, available)), "issue")
        return "issue"

    def new_transfer(self, connection) -> str:
        source, target = self.random.sample(WAREHOUSES, 2)
        result = connection.execute(
            insert(transfers).values(
                from_warehouse=source,
                to_warehouse=target,
                status="draft",
                created_at=_now(),
            )
        )
        transfer_id = result.inserted_primary_key[0]
        chosen = self.random.sample(range(1, len(CATALOG) + 1), self.random.randint(1, 3))
        connection.execute(
            insert(transfer_lines),
            [
                {
                    "transfer_id": transfer_id,
                    "line_no": n,
                    "product_id": p,
                    "quantity": self.random.randint(1, 12),
                }
                for n, p in enumerate(chosen, start=1)
            ],
        )
        return "new transfer"

    def advance_transfer(self, connection) -> str:
        row = (
            connection.execute(
                select(transfers)
                .where(transfers.c.status != "received")
                .order_by(transfers.c.transfer_id)
                .limit(1)
            )
            .mappings()
            .first()
        )
        if row is None:
            return self.new_transfer(connection)
        lines = (
            connection.execute(
                select(transfer_lines).where(transfer_lines.c.transfer_id == row["transfer_id"])
            )
            .mappings()
            .all()
        )
        if row["status"] == "draft":
            for line in lines:
                self._move(
                    connection,
                    row["from_warehouse"],
                    line["product_id"],
                    -line["quantity"],
                    "transfer_out",
                )
            new_status = "shipped"
        else:
            for line in lines:
                self._move(
                    connection,
                    row["to_warehouse"],
                    line["product_id"],
                    line["quantity"],
                    "transfer_in",
                )
            new_status = "received"
        connection.execute(
            update(transfers)
            .where(transfers.c.transfer_id == row["transfer_id"])
            .values(status=new_status)
        )
        return f"transfer {new_status}"

    def cancel_draft(self, connection) -> str:
        transfer_id = connection.scalar(
            select(transfers.c.transfer_id)
            .where(transfers.c.status == "draft")
            .order_by(transfers.c.transfer_id.desc())
            .limit(1)
        )
        if transfer_id is None:
            return self.new_transfer(connection)
        connection.execute(
            delete(transfer_lines).where(transfer_lines.c.transfer_id == transfer_id)
        )
        connection.execute(delete(transfers).where(transfers.c.transfer_id == transfer_id))
        return "draft cancelled"

    def step(self) -> str:
        action = self.random.choices(
            [self.receipt, self.issue, self.new_transfer, self.advance_transfer, self.cancel_draft],
            weights=[30, 35, 12, 18, 5],
        )[0]
        with self.engine.begin() as connection:
            return action(connection)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Synthetic depot database for the demo")
    parser.add_argument("--url", default=os.environ.get("DEPOT_ADMIN_URL", "sqlite:///depot.db"))
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between operations")
    parser.add_argument("--steps", type=int, default=0, help="stop after N operations (0 = never)")
    parser.add_argument("--init-only", action="store_true", help="create and seed, then exit")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    engine = init_database(args.url, reader_password=os.environ.get("READER_PASSWORD"))
    if args.init_only:
        return
    simulator = Simulator(engine, args.seed)
    done = 0
    while args.steps == 0 or done < args.steps:
        log.info(simulator.step())
        done += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
