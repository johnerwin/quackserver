# quackserver — Claude Context

## What This Is

A constrained, append-oriented HTTP server built on DuckDB. Designed for solo developers who need analytical reads, serialized writes, and operational simplicity. The durable append log (JSONL) is the source of truth; DuckDB is a derived read projection that can be dropped and rebuilt at any time.

## Architecture

```
HTTP (FastAPI)
    │
    ▼
Runtime (state machine: STOPPED→STARTING→RECOVERING→RUNNING→STOPPING, FAILED sticky)
    │
    ├── AppendLogWriter  ← canonical truth, fsync-durable JSONL
    ├── DedupStore       ← SQLite, request_id idempotency (24h TTL)
    ├── ReplayCheckpoint ← SQLite, marks projection convergence point
    ├── asyncio.Queue    ← execution buffer only, not durable
    └── WriteWorker      ← single-writer guarantee
            │
            ▼
      ProjectionStore (ABC)
            │
            ▼
      DuckDB file  (derived, disposable)
```

Read path: concurrent reads under `asyncio.Semaphore` (slot count = `MAX_CONCURRENT_READS`). Read connections are read-only; no write contention.

## Key Files

| File | Role |
|---|---|
| `quackserver/core/runtime.py` | State machine and lifecycle coordinator — start here |
| `quackserver/core/worker.py` | Single write worker, retry logic, checkpoint writes |
| `quackserver/core/log.py` | AppendLogWriter / AppendLogReader (JSONL, fsync) |
| `quackserver/core/dedup.py` | SQLite idempotency cache |
| `quackserver/core/checkpoint.py` | ReplayCheckpoint — projection convergence boundary |
| `quackserver/core/replay.py` | ReplayScanner — startup recovery from log |
| `quackserver/core/projection.py` | ProjectionStore ABC + InMemoryProjectionStore |
| `quackserver/storage/interface.py` | StorageInterface ABC + Result dataclass |
| `quackserver/storage/duckdb_impl.py` | DuckDB implementation (stub — issue #2) |
| `quackserver/http/routes.py` | FastAPI routes; read/write paths diverge here |
| `quackserver/http/app.py` | FastAPI lifespan — starts/stops Runtime |
| `quackserver/config.py` | All resource limit constants with rationale |

## Invariants That Must Not Break

- **Single writer.** Only WriteWorker calls write methods on ProjectionStore. HTTP handlers enqueue; they never write directly.
- **Log is canonical.** A write is durable when it is fsynced to the append log, not when DuckDB has processed it. DuckDB state can always be rebuilt from the log.
- **`_transition()` is the sole authority** for runtime state changes. Never set `_state` directly.
- **`retryable` is set by the failure producer.** WriteWorker reads it; it does not decide retry policy itself.
- **`request_id` is required on all writes.** Missing request_id returns 400 before any processing.
- **ProjectionStore.apply() must be idempotent.** The replay path calls it on already-applied commands.

## Current Status (as of 2026-05-05)

Write path is complete and tested (260 tests). Read path is stubbed:
- **Issue #2**: DuckDB projection — `storage/duckdb_impl.py` needs implementing
- **Issue #3**: Read endpoints — `GET /dashboard` and `GET /reports/{id}` return 501
- **Issue #1**: Backpressure metrics — queue occupancy exposure

Do not implement read endpoints until issue #2 is resolved.

## Test Suite

```bash
pytest                        # all 260 tests
pytest tests/test_chaos.py    # durability contracts (most important)
pytest tests/test_projection.py  # idempotency invariants
pytest tests/test_runtime.py  # state machine
```

The chaos suite (`test_chaos.py`) is the bar for write-path changes: no data loss, no duplicates across failure/restart cycles. If you touch the log, dedup store, worker, or checkpoint, run it explicitly.

`InMemoryProjectionStore` in `core/projection.py` is the strict fake used in tests. It enforces the same invariants as the real DuckDB implementation will — UPSERT for `create_user`, unique-by-request_id for `append_event`, terminal failure for unknown commands.

## Resource Limits

All in `config.py`. Named constants with rationale. Do not change them without workload evidence. The key ones:

- `MAX_WRITE_QUEUE_SIZE = 1000` — 503 on overflow
- `MAX_WRITE_TIME_MS = 500` — 504 + abort on exceeded
- `MAX_CONCURRENT_READS = 10` — semaphore slots, 503 on overflow
- `IDEMPOTENCY_KEY_TTL_HOURS = 24` — dedup window

## Spec Reference

`DuckDB_AppServer_Spec.docx` is the source of truth. Config comments and docstrings reference spec sections (e.g., `spec §3`, `spec §8.2`). When something looks odd, check the spec before changing it.

## What Not To Add

The scope boundary is explicit. Do not add:
- Arbitrary SQL execution endpoints
- User-managed transactions
- Distributed coordination
- Background compaction
- Realtime subscriptions

If a request falls outside the operating envelope, close it with an explanation. The constraints are features.
