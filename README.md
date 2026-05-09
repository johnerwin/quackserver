# quackserver

A constrained, append-oriented application server built on top of [DuckDB](https://duckdb.org).

**Built for solo developers and small teams who need analytical reads, serialized writes, and operational sanity — without the complexity of a general-purpose database server.**

---

## The Problem

DuckDB is an exceptional analytical database. Fast, embedded, zero infrastructure. But it has a single-writer constraint — only one process can write at a time. For solo developers building internal tools, dashboards, AI apps, or lightweight SaaS backends, this is usually fine. You just need a thin, disciplined layer to manage it safely.

Most solutions either expose too much (raw SQL endpoints), require managed infrastructure (MotherDuck), or leave durability as an afterthought (in-memory queues that lose writes on crash).

quackserver takes a different approach.

---

## The Design

```
POST /events, /users, /metrics
         │
         ▼
 1. Durable append log      ← canonical truth, survives crashes
         │
         ▼
 2. asyncio write queue     ← execution buffer only
         │
         ▼
 3. Single write worker     ← one DuckDB connection
         │
         ▼
     DuckDB file            ← derived read projection, disposable

GET /dashboard, /reports
         │
         ▼
 Read pool + semaphore      ← concurrent reads, bounded concurrency
         │
         ▼
     DuckDB file
```

**The durable append log is canonical truth. DuckDB is a derived read layer that can be dropped and rebuilt at any time.**

This means:
- Process crashes lose nothing — writes replay from the log on restart
- Migrations are replay operations, not schema surgeries
- Corruption recovery is drop-and-rebuild, not a production incident
- DuckDB version upgrades are non-events

---

## What This Is

- A **command-oriented HTTP API** (not a SQL endpoint)
- A **serialized write path** with bounded queue and timeouts
- A **concurrent read path** with semaphore-limited parallelism
- A **durable append log** as the source of truth
- A **clean StorageInterface** boundary — the migration exit ramp to Postgres if you ever need it

## What This Is Not

- Not a general-purpose database server
- Not a distributed system
- Not a replacement for Postgres at scale
- Not a system that exposes arbitrary SQL to clients
- Not over-engineered infrastructure for a workload that doesn't need it

---

## Operating Envelope

**Designed for:**
- Moderate read concurrency (< 50 simultaneous)
- Append-heavy write workloads
- Short-lived write transactions (< 500ms)
- Analytical reads on derived tables
- Small operational teams (1–3 people)
- Internal tools, dashboards, AI-native apps, ETL pipelines, reporting systems

**Not designed for:**
- High-frequency transactional OLTP
- Arbitrary user-managed transactions
- Unbounded write contention across sessions
- Multi-region distributed consistency
- Ad hoc SQL execution by end users

If your workload fits the envelope, this is operationally very simple. If it doesn't, use Postgres.

---

## Resource Limits (Defaults)

| Constant | Default | Purpose |
|---|---|---|
| `MAX_WRITE_QUEUE_SIZE` | 1,000 | Caps queue memory; 503 on overflow |
| `MAX_WRITE_TIME_MS` | 500ms | Prevents write locks from starving reads |
| `MAX_READ_QUERY_TIME_MS` | 10,000ms | Cancels runaway analytical queries |
| `MAX_CONCURRENT_READS` | 10 | Semaphore slots; prevents CPU saturation |
| `MAX_CONNECTIONS` | 50 | Total concurrent HTTP sessions |
| `MAX_PAYLOAD_SIZE_MB` | 10MB | Prevents oversized write abuse |
| `MAX_RESULT_ROWS` | 100,000 | Bounds read response size |
| `IDEMPOTENCY_KEY_TTL_HOURS` | 24hrs | Deduplication window for retried writes |

All limits are named constants in `config.py`. They can be tuned under measured pressure, never under speculation.

---

## Idempotency

Every write command requires a client-generated `request_id`. The server deduplicates within the TTL window — retried requests return the cached result without re-executing.

```json
POST /events
{
  "request_id": "01HXYZ...",
  "event_type": "user.signup",
  "payload": { ... }
}
```

Missing `request_id` returns `400`. Duplicate `request_id` within TTL returns `200` with the original result.

---

## Failure Semantics

| Situation | Behavior | Status |
|---|---|---|
| Write queue full | Reject immediately | 503 |
| Write timeout | Abort + log | 504 |
| Read timeout | Cancel query + log | 504 |
| Read semaphore full | Reject immediately | 503 |
| Duplicate `request_id` | Return cached result | 200 |
| Missing `request_id` | Reject before processing | 400 |
| Payload too large | Reject before processing | 413 |
| Process crash mid-write | Replay from append log on restart | N/A |

**The rule: never hide overload. Loud failures are recoverable. Silent degradation is not.**

---

## Installation

> **Note:** PyPI publishing is not yet configured — see [issue #15](https://github.com/johnerwin/quackserver/issues/15). Install from source in the meantime:

```bash
git clone https://github.com/johnerwin/quackserver.git
cd quackserver
pip install .
```

For development dependencies (pytest, httpx):

```bash
pip install ".[dev]"
```

Requires Python 3.11+ and DuckDB 1.0+.

---

## Quickstart

```python
# server.py
from quackserver import QuackServer

server = QuackServer(db_path="./data/analytics.duckdb")
server.run(host="0.0.0.0", port=8000)
```

```bash
python server.py
```

```bash
# Write an event
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"request_id": "01HXYZ", "event_type": "user.signup", "payload": {"user_id": 42}}'

# Read dashboard metrics
curl http://localhost:8000/dashboard
```

---

## The StorageInterface

Application logic never calls DuckDB directly. Everything goes through `StorageInterface`:

```python
class StorageInterface:
    async def create_user(self, user_id: str, payload: dict) -> Result: ...
    async def append_event(self, event_type: str, payload: dict) -> Result: ...
    async def get_dashboard_metrics(self, filters: dict) -> Result: ...
    async def query_events(self, filters: dict, limit: int) -> Result: ...
```

This is the migration exit ramp. If you ever outgrow DuckDB, swap the implementation behind this interface. Your application code changes nothing. A weekend, not a rewrite.

---

## Migrating an Existing DuckDB Database

quackserver can onboard an existing DuckDB database as the initial state of the append log — and produce a clean, compacted copy in the process.

This is useful if your existing database has accumulated space from deleted rows, WAL fragmentation, or repeated writes. A clean rebuild from structured export often reclaims 20–40% of disk space.

```bash
quackserver migrate --source ./existing.duckdb --output ./clean.duckdb
```

See [Migration Guide](docs/migration.md) for full details.

---

## Stack

| Layer | Technology |
|---|---|
| HTTP framework | FastAPI |
| Concurrency | asyncio |
| Durable append log | JSONL file |
| Deduplication store | SQLite |
| Write serialization | asyncio.Queue |
| Read concurrency cap | asyncio.Semaphore |
| Storage | DuckDB |
| Logging | Structured JSON |

---

## Scope Policy

This project has an explicit scope boundary. The following will not be added regardless of demand:

- Arbitrary SQL execution endpoints
- User-managed transactions
- Custom WAL or MVCC implementation
- Distributed transaction manager
- Realtime subscriptions
- Background compaction engine

These are not missing features. They are deliberate constraints that keep the system understandable, operable, and recoverable by a single developer.

If a feature request falls outside the operating envelope, it will be closed with an explanation — not a workaround.

---

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR. The most important section is the scope boundary. A contribution that holds the constraints is worth more than one that adds capability.

The whiteboard test applies to every PR: if the change makes the system harder to explain in 5 minutes, it needs a very strong justification.

---

## Built On

- [DuckDB](https://duckdb.org) — MIT licensed analytical database
- [FastAPI](https://fastapi.tiangolo.com) — MIT licensed Python web framework

---

## License

MIT

---

*"Build the smallest thing that could embarrass you if it worked too well."*
