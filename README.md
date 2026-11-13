# depot-change-monitor

[![CI](https://github.com/taharabs/depot-change-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/taharabs/depot-change-monitor/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**See every change made in a remote warehouse database, without touching that database.**

A small Python agent sits next to the depot's SQL Server, reads it with a read-only login, works out what changed since the last look, and ships those changes over HTTPS to a FastAPI service with a live dashboard. If the depot's internet drops, changes wait in a local queue and are delivered when it comes back.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/dashboard-dark.png">
  <img src="docs/dashboard-light.png" alt="Dashboard: totals, changes per table split by insert, update and delete, and a live list of the latest changes with before and after values.">
</picture>

## The problem

A company runs its stock on an ERP database at a remote depot. Head office wants to know what moves there (stock levels, movements, transfers between warehouses) as it happens. The usual tools are off the table:

- **The production database must not be modified.** No triggers, no extra columns, no Change Data Capture to switch on, no risk of locking the ERP.
- **The link to the depot is unreliable.** A solution that loses changes during an outage is not acceptable.
- **It has to be simple to run.** One small process at the depot, one small service in the cloud.

## How it works

```mermaid
flowchart LR
    subgraph Depot["Depot (local network)"]
        ERP[("SQL Server<br/>ERP database")]
        Agent["Agent<br/>Python"]
        State[("SQLite<br/>snapshot + outbox")]
        ERP -- "SELECT only<br/>read-only login" --> Agent
        Agent <--> State
    end
    subgraph Cloud
        API["FastAPI<br/>ingest + read API"]
        PG[("PostgreSQL")]
        Dash["Dashboard"]
        API <--> PG
        API --> Dash
    end
    Agent -- "HTTPS + API key<br/>batches, retried" --> API
```

1. **Poll.** Every few seconds the agent reads the watched tables with plain `SELECT` statements and fingerprints each row (SHA-256).
2. **Diff.** It compares the fingerprints with the snapshot it keeps in a local SQLite file. A new key is an *insert*, a missing key is a *delete*, a different fingerprint is an *update*, reported with the fields that changed and their before and after values.
3. **Queue.** The new snapshot and the resulting events are written in **one SQLite transaction** (the outbox pattern). A crash or power cut can neither lose a change nor report it twice.
4. **Ship.** Events leave in order, in batches. A batch is removed from the queue only after the API confirms it. Offline? The queue simply grows and drains later.
5. **Store.** The API stores each event once: every event carries a unique id, so a resend after a lost confirmation is harmless (idempotent ingestion).

## Quick start

### With Docker (SQL Server + PostgreSQL, the real setup)

```bash
git clone https://github.com/taharabs/depot-change-monitor.git
cd depot-change-monitor
docker compose up --build
```

Open <http://localhost:8000>. SQL Server takes about 30 seconds to start; then a simulator plays the warehouse software (receipts, issues, transfers) and the dashboard fills up. API docs are at <http://localhost:8000/docs>.

### Without Docker (everything on SQLite, about a minute)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# terminal 1: the API and dashboard
MONITOR_API_KEYS=demo-key uvicorn depot_monitor.api.main:app

# terminal 2: a synthetic depot that keeps changing
depot-simulator --url sqlite:///depot.db

# terminal 3: the agent
SOURCE_URL=sqlite:///depot.db API_URL=http://127.0.0.1:8000 API_KEY=demo-key \
  depot-agent --config monitor.example.toml
```

Try the offline queue: stop the API (terminal 1), wait, start it again, and watch the backlog arrive in order.

## Configuration

Watched tables live in a TOML file; secrets never do. Copy `monitor.example.toml` to `monitor.toml`:

```toml
[agent]
source_name = "depot-01"
poll_seconds = 5

[[tables]]
name = "stock"
key = ["warehouse_id", "product_id"]   # the column(s) that identify a row

[[tables]]
name = "transfers"
key = "transfer_id"
columns = ["transfer_id", "status"]     # optional: watch only some columns
```

| Variable | Used by | Meaning |
| --- | --- | --- |
| `SOURCE_URL` | agent | SQLAlchemy URL of the source, e.g. `mssql+pymssql://reader:...@host:1433/db` |
| `API_URL` | agent | Base URL of the API, e.g. `https://monitor.example.com` |
| `API_KEY` | agent | Secret sent in the `X-API-Key` header |
| `STATE_PATH` | agent | Where the agent keeps its SQLite state (default `agent-state.db`) |
| `POLL_SECONDS` | agent | Overrides `poll_seconds` from the file |
| `DATABASE_URL` | API | Where events are stored, e.g. `postgresql+psycopg://...` |
| `MONITOR_API_KEYS` | API | Comma-separated keys accepted from agents (required) |
| `DASHBOARD_USER`, `DASHBOARD_PASSWORD` | API | If set, the dashboard and read endpoints require HTTP Basic auth |

Give the agent a login that can only read. On SQL Server:

```sql
CREATE LOGIN monitor_reader WITH PASSWORD = '...';
CREATE USER monitor_reader FOR LOGIN monitor_reader;
ALTER ROLE db_datareader ADD MEMBER monitor_reader;
```

## API

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/api/v1/events` | API key | Store a batch of events (idempotent) |
| `POST` | `/api/v1/heartbeat` | API key | Agent reports it is alive and how many events are waiting |
| `GET` | `/api/v1/events` | Basic (optional) | Latest events; filters `table`, `op`, `source`, `since`; paging with `before_id` |
| `GET` | `/api/v1/stats` | Basic (optional) | Totals, counts per table and operation, agent status |
| `GET` | `/health` | none | Liveness check (also checks the database) |
| `GET` | `/` | Basic (optional) | The dashboard |

An event looks like this:

```json
{
  "event_id": "5f0c1a...",
  "source": "depot-01",
  "table": "stock",
  "op": "update",
  "key": {"warehouse_id": 1, "product_id": 7},
  "before": {"warehouse_id": 1, "product_id": 7, "quantity": 120, "updated_at": "2026-11-13T09:00:00"},
  "after":  {"warehouse_id": 1, "product_id": 7, "quantity": 96,  "updated_at": "2026-11-13T09:04:10"},
  "changed_fields": ["quantity", "updated_at"],
  "detected_at": "2026-11-13T09:04:12.512+00:00"
}
```

## Design decisions

| Decision | Why | Cost |
| --- | --- | --- |
| Snapshot diff instead of triggers or CDC | Needs nothing from the source but `SELECT` permission, and works on any database SQLAlchemy can read | Reads each watched table in full on every poll; several changes to one row between two polls are reported as one net change |
| Outbox in SQLite | Snapshot and events commit together, so the agent can crash at any point without losing or duplicating a change | One more file to back up or, if lost, a new baseline |
| Idempotent ingestion | "At least once" delivery from the agent becomes "exactly once" in storage | A unique index on `event_id` |
| Secrets only in the environment | The config file is safe to commit and review | Two places to look when configuring |
| Decimals and dates as strings | No rounding: `14.50` stays `14.50` across SQL Server, JSON and PostgreSQL | Consumers parse them when they need arithmetic |

## Tests

```bash
python -m pytest             # 29 unit and end-to-end tests, on SQLite, about a second
ruff check . && ruff format --check .
```

Continuous integration runs three jobs on every push:

- lint and tests on Python 3.11 and 3.13;
- an **integration test against a real SQL Server 2022**, which checks that the read-only login cannot write and that the agent detects changes through it;
- a **smoke test of the Docker Compose demo**: the whole stack starts and changes must reach the API.

## Limitations and roadmap

- [ ] Large tables: use a `rowversion` or "modified at" column when one exists, to read only what changed
- [ ] Per-table poll intervals
- [ ] Alert rules (for example, stock below a threshold) with e-mail or webhook notifications
- [ ] Export to CSV from the dashboard
- [ ] Retention policy for old events

Today the agent suits tables up to a few hundred thousand rows, polled every few seconds. Events from one poll share one detection time, and row keys must be stable.

## About the data

All data in this repository is synthetic. The schema and the simulator were written for this project; nothing comes from a real company database.

## License

MIT. Built by [Mohamed Tahar Ababsa](https://github.com/taharabs).
