# DuckDB Application Server — Implementation Specification

**Version 1.0 · May 2026**

*A constrained, append-oriented analytical runtime for solo developers*

---

# 1. Purpose & Design Philosophy

This document specifies the architecture, constraints, and operational model for a lightweight DuckDB-backed application server. It is intended as a handoff document to guide implementation without deviation.

The central design principle:

> **Guiding Principle**
>
> Build the smallest thing that could embarrass you if it worked too well.
> Optimize for reversible decisions. Constrain aggressively. Fail loudly.
> A system you can explain on a whiteboard in 5 minutes is a system you can debug at 2am.

## 1.1 What This System Is

A constrained application runtime with durable analytical storage. Not a general-purpose database server.

- Append-heavy workload optimized
- Moderate concurrency — reads parallel, writes serialized
- Analytical reads separated from transactional writes
- Fast recovery as a first-class design goal
- Operable by a single developer without heroics

## 1.2 What This System Is Not

The following are explicit non-goals. Pressure to add these should be treated as a scope conversation, not an implementation decision.

- Not a general-purpose OLTP database
- Not a distributed system
- Not a multi-region consistency layer
- Not a generalized database hosting platform
- Not a system that exposes arbitrary SQL to clients

---

# 2. Architecture

## 2.1 System Diagram

```
Client A  ──┐
Client B  ──┼──▶  FastAPI (HTTP)  ──▶  Command Router
Client C  ──┘                                │
                                 ┌──────────┴──────────┐
                                 │                     │
                            WRITE PATH            READ PATH
                                 │                     │
                       1. Durable Append Log    Read Pool
                          (canonical truth)    (3 read-only
                                 │              conns + semaphore)
                       2. asyncio.Queue               │
                          (execution buffer)          │
                                 │                     │
                       3. Write Worker                 │
                          (1 connection)               │
                                 │                     │
                                 └────────┬────────────┘
                                          ▼
                                   DuckDB File
                                (read projection,
                                   disposable)
```

Key insight: the durable append log is canonical truth. DuckDB is a derived read projection. A process crash loses nothing — the queue drains on restart from the log. DuckDB can be dropped and rebuilt at any time.

## 2.2 Core Components

| Component | Role | Key Constraint |
| --- | --- | --- |
| FastAPI HTTP layer | Receives all client requests | Stateless — no DB logic here |
| Command Router | Dispatches to read pool or write queue | Never exposes raw SQL |
| Durable Append Log | Canonical source of truth for all writes | Written before queue enqueue — survives crashes |
| asyncio.Queue | Execution buffer for pending writes | In-memory only — not a durability guarantee |
| Write Worker | Owns the sole write connection | Dequeues from log; one active write at a time |
| Read Connection Pool | Serves SELECT queries in parallel | Read-only conns; bounded by semaphore |
| Read Semaphore | Caps concurrent analytical reads | MAX_CONCURRENT_READS slots — prevents CPU starvation |
| DuckDB File | Derived read projection | Disposable — rebuild from append log at any time |

## 2.3 Storage Interface

Application logic must never call DuckDB directly. All storage access goes through named methods on a `StorageInterface` class. This is the migration exit ramp.

```python
# The storage interface — the only layer that knows about DuckDB

class StorageInterface:
    async def create_user(user_id, payload) -> Result
    async def append_event(event_type, payload) -> Result
    async def get_dashboard_metrics(filters) -> Result
    async def query_events(filters, limit) -> Result

# Application logic depends on this interface.
# DuckDB is an implementation detail behind it.
# Swapping to Postgres is a weekend, not a rewrite.
```

## 2.4 API Design — Command-Oriented

The HTTP API is command-oriented, not query-oriented. This is a load-bearing constraint.

| Correct — Command API | Forbidden — Query API |
| --- | --- |
| `POST /users` | `POST /query` |
| `POST /events` | `POST /sql` |
| `POST /metrics` | `GET /execute` |
| `GET /dashboard` | Any endpoint accepting raw SQL |
| `GET /reports/{id}` | |

Command-oriented endpoints preserve architectural constraints, prevent arbitrary query behavior, reduce attack surface, and make migrations easier. Once `/query` exists, every constraint becomes negotiable.

---

# 3. Resource Limits

Every resource is bounded. Every failure mode is intentional. These are defaults — they may be tuned under measured pressure, never under speculation.

| Constant | Default Value | Rationale |
| --- | --- | --- |
| `MAX_WRITE_QUEUE_SIZE` | 1,000 | Caps memory; triggers 503 on overload |
| `MAX_WRITE_TIME_MS` | 500 ms | Prevents write locks from starving reads |
| `MAX_READ_QUERY_TIME_MS` | 10,000 ms | Cancels runaway analytical queries |
| `MAX_CONCURRENT_READS` | 10 | Semaphore slots; prevents CPU saturation from parallel scans |
| `MAX_CONNECTIONS` | 50 | Limits total concurrent HTTP sessions |
| `MAX_PAYLOAD_SIZE_MB` | 10 MB | Prevents oversized write abuse |
| `MAX_RESULT_ROWS` | 100,000 | Bounds read response size |
| `WRITE_QUEUE_WARNING_PCT` | 80% | Emit metric warning before hard limit |
| `MAX_WRITE_RETRIES` | 3 | Retry on transient lock, then fail loudly |
| `IDEMPOTENCY_KEY_TTL_HOURS` | 24 hrs | Window for deduplicating retried writes |

> **Critical Rule**
>
> Without concrete numerical limits, 'moderate concurrency' becomes infinite concurrency.
> These numbers give overload a shape. You can test it, monitor it, and reason about it.
> A system with explicit limits fails predictably. A system without them degrades ambiguously.

---

# 4. Failure Semantics

Failure behavior must be defined before implementation begins. Hidden overload is where systems become nondeterministic.

| Situation | System Behavior | HTTP Status |
| --- | --- | --- |
| Write queue full | Reject immediately — do not wait | 503 |
| Write exceeds timeout | Abort transaction + structured log | 504 |
| Read exceeds timeout | Cancel query + structured log | 504 |
| Read semaphore full (> MAX_CONCURRENT_READS) | Reject immediately — do not queue reads | 503 |
| DuckDB locked unexpectedly | Retry up to MAX_WRITE_RETRIES, then fail | 503 |
| Process crashes mid-write | Recover from durable append log on restart | N/A |
| Duplicate request_id within TTL window | Return 200 with cached result — do not re-execute | 200 |
| Missing request_id on write command | Reject — idempotency key is required | 400 |
| Queue backlog > 80% | Emit metric alert + begin rejecting new writes | 503 |
| Payload exceeds limit | Reject before processing | 413 |
| Unknown command endpoint | 404 — never fall through to raw SQL | 404 |

The rule: never hide overload. A 503 that tells the client 'queue full' is a recoverable situation. Silent queue buildup with delayed failures is an operational disaster.

---

# 5. Operating Envelope

Systems become dangerous when their real limits are unclear and operators assume guarantees that don't exist. This envelope should be documented, version-controlled, and treated as a product decision.

| Designed For | Not Designed For |
| --- | --- |
| Moderate read concurrency (< 50 simultaneous) | High-frequency transactional OLTP workloads |
| Append-heavy write workloads | Arbitrary user-managed transactions |
| Short-lived write transactions (< 500ms) | Unbounded write contention across sessions |
| Analytical read queries on derived tables | Multi-region distributed consistency |
| Small operational teams (1–3 people) | Ad hoc SQL execution by end users |
| Fast recovery from process restart | Long-running write locks (> 500ms) |
| Internal tools, dashboards, AI-native apps | Background compaction or replication |
| ETL pipelines, reporting systems, admin UIs | Generalized database hosting |

---

# 6. Schema & Data Design Principles

## 6.1 Append-Only First

Mutable state is where coordination complexity originates. Append-only design replaces coordination with sequencing — an enormous simplification.

| Prefer Append Patterns | Avoid Mutation Patterns |
| --- | --- |
| Event logs with timestamps | Heavy UPDATE workloads |
| Immutable records | Row-level contention |
| Derived/aggregated tables | Mutable shared state |
| Async background compaction | In-place record mutation |
| Periodic snapshots from log replay | User-visible transaction rollback |

## 6.2 DuckDB as Disposable State

Architect so that the append log or source data is canonical, and DuckDB is a derived read layer that can be discarded and rebuilt.

- Corruption recovery: drop and rebuild from log
- Schema migration: replay events into new schema
- Experimentation: safe to test against rebuilt copies
- Upgrades: DuckDB version bumps become non-events

This changes operational risk from 'database is precious' to 'database is fast cache'. That is a profound simplification for a solo operator.

---

# 7. Durability Semantics

This is the most critical gap to close before implementation. The `asyncio.Queue` is an in-memory execution buffer — it provides no durability guarantees. A process crash will lose any writes that were accepted but not yet flushed to DuckDB.

> **The Problem**
>
> HTTP request accepted → 200 returned to client → client believes data is safe.
> Write sits in asyncio.Queue → process crashes → write is silently lost.
> This is not a tradeoff. It is a silent lie to the client.

## 7.1 The Two-Layer Write Model

The fix is to write to the durable append log before enqueuing. This makes the log the source of truth and the queue a pure execution buffer.

```
POST /events (with request_id)
       │
       ▼
  1. Write to durable append log  ←── canonical truth, survives crashes
       │
       ▼
  2. Enqueue to asyncio.Queue     ←── execution buffer only
       │
       ▼
  3. Write worker dequeues        ←── projects into DuckDB
       │
       ▼
  4. DuckDB updated               ←── derived read layer

  On crash restart:
  → Replay unprocessed entries from append log
  → DuckDB projection rebuilt
  → No writes lost
```

## 7.2 Append Log Options

| Option | Complexity | Best For |
| --- | --- | --- |
| SQLite file (append table) | Very low — stdlib only | Solo projects, simplest recovery model |
| JSONL flat file (line-per-event) | Very low — stdlib only | Human-readable, easy to inspect and replay |
| Embedded key-value (e.g. lmdb) | Low-medium | Higher write throughput needs |
| External queue (Redis, Kafka) | High — new dependency | Only if truly needed at scale |

Recommended default: a JSONL append file or SQLite table. Both are zero-dependency, human-readable, easily inspected at 2am, and trivially replayable. Do not add an external queue dependency until workload evidence demands it.

## 7.3 Crash Recovery Model

- On startup: scan append log for entries not yet reflected in DuckDB
- Replay any unprocessed entries through the write worker
- Use `request_id` to detect and skip already-applied entries (idempotent replay)
- Recovery must complete before the HTTP server begins accepting requests
- Log recovery duration and entry count on every restart

---

# 8. Idempotency Semantics

Once clients can safely retry — and they must be able to, given timeout semantics — duplicate writes become the expected failure mode, not the exceptional one. Append-only systems amplify this: a retry that succeeds twice creates two events.

> **The Problem**
>
> POST /events → timeout occurs → client retries.
> Did the first write succeed or not? The client cannot know.
> Without idempotency: retry = duplicate event in append log.
> In append-only systems, duplicates are silent and permanent.

## 8.1 Required: Client-Generated request_id

Every write command must include a client-generated `request_id`. The server enforces uniqueness within `IDEMPOTENCY_KEY_TTL_HOURS`.

```
POST /events
Content-Type: application/json

{
  "request_id": "cli_01HXYZ...",   // required — UUID or ULID, client-generated
  "event_type": "user.signup",
  "payload": { ... }
}

# First call:  write executes → 200 returned
# Retry:       request_id found in dedup store → 200 returned (cached)
# No request_id: 400 rejected before processing
```

## 8.2 Deduplication Store

| Requirement | Specification |
| --- | --- |
| Key | `request_id` (client-generated UUID or ULID) |
| Value | Result snapshot (status, timestamp, summary) |
| TTL | `IDEMPOTENCY_KEY_TTL_HOURS` (default: 24 hours) |
| Storage | Separate from DuckDB — SQLite table or in-memory with TTL |
| Lookup timing | Before append log write — reject dupes at the gate |
| Replay safety | `request_id` also used during crash recovery to skip re-applied entries |

The dedup store is not the append log. They are separate concerns. The append log is a write journal. The dedup store is a short-lived response cache keyed by client request identity.

---

# 9. Read Backpressure

The write path has explicit overload handling. The read path needs it too. Analytical queries on DuckDB can be expensive — 50 concurrent large table scans will saturate CPU and starve the write worker indirectly, even though reads use separate connections.

> **The Risk**
>
> 50 concurrent analytical reads → CPU saturates → write worker slows.
> 'Parallel reads are safe' becomes accidental self-denial-of-service.
> The read pool needs a concurrency ceiling, not just a connection pool.

## 9.1 Read Semaphore

Wrap all read query execution in an `asyncio.Semaphore` bounded by `MAX_CONCURRENT_READS`. Requests that cannot acquire a slot are rejected immediately with 503 — never queued.

```python
read_semaphore = asyncio.Semaphore(MAX_CONCURRENT_READS)  # default: 10

async def execute_read(query, params):
    try:
        async with asyncio.wait_for(
            read_semaphore.acquire(), timeout=0.0  # non-blocking
        ):
            return await run_query(query, params)
    except asyncio.TimeoutError:
        raise HTTPException(503, 'Read concurrency limit reached')
```

## 9.2 Per-Endpoint Query Limits

In addition to the global semaphore, enforce per-endpoint constraints to prevent any single query pattern from dominating the read pool.

| Endpoint Category | Timeout | Max Rows | Notes |
| --- | --- | --- | --- |
| Dashboard / summary | 3,000 ms | 10,000 | Pre-aggregated queries only |
| Report generation | 10,000 ms | 100,000 | Background-friendly — lower priority |
| Event queries | 5,000 ms | 50,000 | Always filtered by time range |
| Health / status | 500 ms | 100 | Must never block — separate fast path |

If a query pattern consistently hits its timeout, that is a signal to pre-aggregate the result, not to raise the limit.

---

# 10. Logging Strategy

Good logs are worth more than sophisticated abstractions. At 2am, logs beat elegance. Every time.

## 10.1 Required Structured Log Fields

| Event | Fields to Log | Priority |
| --- | --- | --- |
| Write accepted | `request_id, command, queue_depth, timestamp` | Info |
| Append log write | `request_id, command, log_offset, duration_ms` | Info |
| Write completed | `request_id, command, duration_ms, queue_depth` | Info |
| Write rejected (queue full) | `request_id, command, queue_depth, reason` | Warning |
| Write timeout | `request_id, command, elapsed_ms, limit_ms` | Error |
| Duplicate request_id detected | `request_id, original_ts, action=deduplicated` | Info |
| Read accepted (semaphore acquired) | `endpoint, semaphore_slots_used` | Debug |
| Read completed | `endpoint, duration_ms, rows_returned` | Info |
| Read rejected (semaphore full) | `endpoint, slots_in_use, reason` | Warning |
| Read timeout / cancel | `endpoint, elapsed_ms, limit_ms` | Error |
| DuckDB lock retry | `attempt_n, elapsed_ms` | Warning |
| Crash recovery started | `log_entries_to_replay, timestamp` | Critical |
| Crash recovery completed | `replayed_count, skipped_count, duration_ms` | Critical |
| DB reopen / recovery | `reason, elapsed_ms` | Critical |
| Queue depth > 80% | `queue_depth, queue_max, pct` | Warning |
| Process start / restart | `config_snapshot, timestamp` | Info |

Log format: structured JSON. Every log line must be parseable without regex. Include `request_id` on every entry where available for full lifecycle correlation.

---

# 11. Implementation Stack

| Layer | Technology | Rationale |
| --- | --- | --- |
| HTTP framework | FastAPI (Python) | Async-native, minimal, fast to iterate |
| Concurrency model | asyncio | Single-threaded event loop — simple mental model |
| Durable append log | JSONL file or SQLite table | Zero dependencies; human-readable; replayable |
| Deduplication store | SQLite table with TTL | Simple; separate from append log; survives restarts |
| Write serialization | asyncio.Queue | Execution buffer only — not a durability guarantee |
| Read concurrency cap | asyncio.Semaphore | Bounds parallel analytical queries; prevents CPU starvation |
| Read connections | `duckdb.connect(read_only=True)` | Multiple readers, zero write contention |
| Write connection | Single `duckdb.connect()` | One writer, owned by write worker |
| Logging | Python structlog or stdlib JSON | Structured from day one |
| Config | Environment variables + .env | No config database; keep it flat |

## 11.1 Minimal Viable Implementation

```
FastAPI app
  └── POST /events          →  dedup check → append log → queue.put()
  └── POST /users           →  dedup check → append log → queue.put()
  └── GET  /dashboard       →  semaphore → read_pool.execute(query)
  └── GET  /health          →  queue_depth, semaphore_slots, uptime

write_worker (single coroutine)
  └── asyncio.Queue consumer
  └── Owns one duckdb write connection
  └── Enforces MAX_WRITE_TIME_MS
  └── Marks log entry as applied after success
  └── Logs every operation with request_id

read_pool (N=3 read-only connections + semaphore)
  └── asyncio.Semaphore(MAX_CONCURRENT_READS)
  └── Rejects at gate if semaphore full (no queuing)
  └── Enforces per-endpoint timeouts and row limits

startup sequence
  └── Scan append log for unprocessed entries
  └── Replay through write worker (idempotent via request_id)
  └── Begin accepting HTTP requests only after recovery complete
```

---

# 12. Enforced Constraints & Scope Rules

These constraints are product decisions, not technical limitations. Once framed as temporary deficiencies, pressure to violate them becomes constant. They are enforced permanently unless explicitly revisited as a product decision.

## 12.1 What Must Never Be Built

**Hard Scope Boundaries — Do Not Cross**

- Custom WAL (Write-Ahead Log) implementation
- MVCC (Multi-Version Concurrency Control)
- Optimistic concurrency control layer
- Distributed transaction manager
- Custom SQL wire protocol
- Full generic database abstraction layer
- Realtime subscription / push system
- Background compaction engine
- Transparent retry-with-backoff (masks overload)
- User-managed transaction semantics
- Arbitrary SQL execution endpoint
- Unbounded query execution (no timeouts)

## 12.2 The Slippery Slope — Warning Signs

Each of the following sounds small. Together they become accidental database engineering. Treat any of these as a trigger to stop and reassess scope:

- "I'll just expose transactions for this one use case"
- "I'll allow arbitrary SQL but only for internal users"
- "I'll add realtime subscriptions — it's just WebSockets"
- "I'll support concurrent writes eventually"
- "I'll add background compaction — it's just a cron job"
- "I'll implement retries transparently — users won't notice"

Databases are among the hardest software systems humans build — not because individual concepts are impossible, but because edge cases multiply combinatorially. The only winning move is to not play.

---

# 13. Migration Exit Ramp

The `StorageInterface` boundary is the migration exit ramp. If application logic depends on `StorageInterface` methods — not raw DuckDB calls — then replacing the backend is a contained problem.

| Clean Boundary (Weekend Migration) | Coupled (6-Month Rewrite) |
| --- | --- |
| Application calls `StorageInterface.append_event()` | Application calls `duckdb.execute()` directly |
| Storage layer owns all DuckDB-specific code | SQL scattered across business logic |
| Commands encode business intent, not SQL | SQL semantics leak into API contracts |
| Tests mock the StorageInterface, not DuckDB | Tests depend on DuckDB being present |
| Migration = new StorageInterface implementation | Migration = months of grep-and-replace |

When write throughput eventually requires Postgres, the `StorageInterface` makes it a weekend project. This is the single highest-leverage architectural decision in the stack.

---

# 14. Pre-Implementation Validation Checklist

Before writing a single line of application code, validate these decisions are locked. If any answer is unclear, resolve it first.

| Question | Expected Answer |
| --- | --- |
| Does every write go through the durable append log first? | Yes — before queue enqueue |
| Does every write command carry a client-generated request_id? | Yes — 400 if missing |
| Is the deduplication store separate from the append log? | Yes — separate concerns |
| Is the asyncio.Queue treated as execution buffer only (not durable)? | Yes — log is durable, queue is not |
| Does every write go through the asyncio.Queue? | Yes — no exceptions |
| Is the read pool bounded by asyncio.Semaphore? | Yes — MAX_CONCURRENT_READS enforced |
| Does any endpoint accept raw SQL? | No — ever |
| Does application logic import duckdb directly? | No — only StorageInterface |
| Is every resource limit set as a named constant? | Yes — before first deploy |
| Is every failure mode in the semantics table handled? | Yes — 503/504/413/400 all tested |
| Does startup replay the append log before accepting requests? | Yes — recovery tested manually |
| Can the system restart and recover in < 60 seconds after a crash? | Yes — verify this manually |
| Are all log lines structured JSON with request_id? | Yes — from day one |
| Is DuckDB treated as derivable / disposable state? | Yes — rebuild from log tested |
| Is the operating envelope documented in the repo? | Yes — as a committed file |
| Can the full architecture be whiteboarded in 5 minutes? | Yes — if not, simplify |

---

# 15. Final Note

> **The Goal**
>
> Not: 'Build a database server.'
> But: 'Build a constrained application runtime with durable analytical storage.'
>
> That framing keeps you out of dangerous territory.
> Every feature request that doesn't fit the framing is a scope conversation.
> Every constraint you hold is operational burden you never carry.
>
> Add complexity only in response to measured bottlenecks, repeated operational pain,
> and concrete workload evidence — never theoretical scalability.

---

*— End of Specification —*
