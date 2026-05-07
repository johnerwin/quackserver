# Deploying and Operating quackserver

From a freshly cloned repo to a running instance, and how to keep it running.

---

## 1. Installation

**Requirements:** Python 3.11+, no other system dependencies.

```bash
# With pip (editable install for development)
pip install -e .

# With uv
uv sync
```

Dependencies pulled in automatically: `duckdb>=1.0`, `fastapi>=0.110`, `uvicorn[standard]>=0.29`.

---

## 2. Starting the server

```bash
python -m quackserver serve
```

This creates a `./data/` directory and writes all state there:

| File | Purpose |
|---|---|
| `data/quack.duckdb` | DuckDB projection (derived, disposable) |
| `data/quack.jsonl` | Append log — **canonical truth** |
| `data/dedup.db` | Idempotency cache (SQLite, 24h TTL) |
| `data/checkpoint.db` | Replay checkpoint (SQLite) |

**Custom paths:**

```bash
python -m quackserver serve \
  --db-path /var/quackserver/quack.duckdb \
  --log-path /var/quackserver/quack.jsonl \
  --host 0.0.0.0 \
  --port 8000
```

The dedup and checkpoint stores are always placed in the same directory as the log.

**Help:**

```bash
python -m quackserver serve --help
```

---

## 3. Smoke-test checklist

Run these after every fresh start or redeploy. All should return HTTP 200.

```bash
# 1. Health check — confirms runtime is RUNNING
curl -s http://localhost:8000/health | python -m json.tool

# 2. Write a user
curl -s -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"request_id":"u-001","user_id":"alice","payload":{"email":"alice@example.com"}}' \
  | python -m json.tool

# 3. Idempotency check — same request_id returns status: "deduplicated"
curl -s -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{"request_id":"u-001","user_id":"alice","payload":{"email":"alice@example.com"}}' \
  | python -m json.tool

# 4. Write an event
curl -s -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"request_id":"e-001","event_type":"page.view","payload":{"url":"/home"}}' \
  | python -m json.tool

# 5. Read dashboard
curl -s http://localhost:8000/dashboard | python -m json.tool

# 6. Read reports
curl -s http://localhost:8000/reports/events | python -m json.tool
curl -s http://localhost:8000/reports/users | python -m json.tool
```

**Expected healthy `/health` response:**

```json
{
  "healthy": true,
  "state": "RUNNING",
  "state_since": "2026-01-01T00:00:00Z",
  "queue_depth": 0,
  "queue_max": 1000,
  "queue_pct": 0.0,
  "queue_warning": false,
  "worker_running": true,
  "replay_pending": 0,
  "malformed_log_entries": 0
}
```

**Degraded signal:** `healthy: false`, `state` other than `RUNNING`, or `queue_pct` above 80 (`queue_warning: true`). Investigate the server log before retrying writes.

---

## 4. Crash recovery test

This verifies the durable append log survives a hard kill and replays correctly on restart.

```bash
# Start the server
python -m quackserver serve &
SERVER_PID=$!

# Write some data
curl -s -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"request_id":"crash-test-1","event_type":"test.event","payload":{"n":1}}'

curl -s -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"request_id":"crash-test-2","event_type":"test.event","payload":{"n":2}}'

# Hard kill (simulates crash)
kill -9 $SERVER_PID

# Restart
python -m quackserver serve &

# Verify both events survived — count should be 2 (or more if you wrote others)
curl -s http://localhost:8000/reports/events | python -m json.tool
```

The server replays the append log on startup before accepting requests. Events written before the crash will be present; any write that had not yet been fsynced to the log is the only thing at risk (the write queue is not durable).

---

## 5. Day-to-day operations

### Log location and format

All server logs are written to stdout as structured JSON (one object per line). Pipe to `jq` for readability:

```bash
python -m quackserver serve 2>&1 | jq .
```

Key fields: `event` (what happened), `level` (`info`/`warning`/`error`), timestamp. Errors include `error` and context fields.

### Inspecting the append log

The append log is a plain JSONL file — one JSON object per write, in arrival order:

```bash
# View last 20 entries
tail -20 data/quack.jsonl | python -m json.tool --no-ensure-ascii

# Count total entries
wc -l data/quack.jsonl

# Find all events of a specific type
grep '"append_event"' data/quack.jsonl | grep '"page.view"' | wc -l
```

Each entry has: `request_id`, `command` (`create_user` or `append_event`), `payload`, `timestamp`.

### Rebuilding the DuckDB projection

DuckDB is disposable derived state. If it becomes corrupted, fragmented, or you want a clean compaction after heavy UPSERT activity:

```bash
# Stop the server first (rebuild requires exclusive file access)
# Then:
python -m quackserver rebuild \
  --log-path data/quack.jsonl \
  --db-path data/quack.duckdb \
  --overwrite
```

The rebuild replays every entry in the log through the same apply logic used during normal crash recovery. The result is a compact file with no WAL fragmentation or dead space from overwritten rows.

After rebuild, restart the server normally.

### Monitoring queue health

The `/health` endpoint is the primary operational signal. Watch `queue_pct` under load:

- `queue_pct < 80%` — normal
- `queue_pct >= 80%` — `queue_warning: true`; writes are slow or volume is high; investigate
- `queue_pct = 100%` — queue full; new writes are rejected with 503

If you see sustained 503s on the write path, the write worker is the bottleneck. Check for slow DuckDB writes in the server log (`duckdb_io_error`, `write_timeout`).

---

## 6. Using quackserver alongside an existing DuckDB database

If you have an existing DuckDB file (e.g. `fulcrum.duckdb`) and want to write events alongside it:

```bash
python -m quackserver serve \
  --db-path /path/to/existing.duckdb \
  --log-path /path/to/quack.jsonl
```

On startup, `initialize()` runs `CREATE TABLE IF NOT EXISTS` for the `users`, `events`, and `projection_metadata` tables. Pre-existing tables are untouched.

**Conflict risk:** if `existing.duckdb` already has a `users` or `events` table with an incompatible schema, startup will fail. Options:
- Rename the conflicting table first: `ALTER TABLE users RENAME TO my_users` (run via any DuckDB connection before starting)
- Or use a separate DuckDB file for quackserver state

**The rebuild command does not apply to pre-existing databases.** `quackserver rebuild` requires a quackserver-format JSONL log as input. It cannot compact an arbitrary DuckDB file. For compacting a standalone DuckDB database, use DuckDB's native `EXPORT DATABASE` / `IMPORT DATABASE` or an ATTACH + streaming copy approach.

---

## 7. Recommended request_id strategy

Use a ULID or UUIDv4. ULIDs are preferred because they sort lexicographically by time, which makes log inspection easier:

```python
# pip install python-ulid
from ulid import ULID
request_id = str(ULID())  # e.g. "01HXYZ3M4KV6WPQRST8ABCDEFG"
```

The deduplication window is 24 hours. Within that window, retrying the same `request_id` returns the original result without re-executing. After 24 hours, the cached result expires and a retried `request_id` would be treated as a new write.

---

## 8. Event taxonomy

Suggested naming convention: `noun.verb` in lowercase with dots.

| Event type | When to emit |
|---|---|
| `user.signup` | New account created |
| `user.login` | Successful authentication |
| `page.view` | Page or screen rendered |
| `action.click` | Significant user interaction |
| `job.started` / `job.finished` | Background task lifecycle |
| `error.client` / `error.server` | Application error occurred |

Keep event types stable — they are stored verbatim and used for grouping in `/dashboard`. Changing a name mid-stream splits the series.
