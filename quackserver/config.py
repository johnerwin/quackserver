# Resource limits and operational constants — spec Section 3 and Section 9.2.
#
# Every bounded resource in the system is represented here as a named constant.
# Rationale: without concrete numbers, "moderate concurrency" becomes infinite concurrency.
# These values give overload a testable, monitorable shape.
#
# Tune under measured pressure and concrete workload evidence. Never under speculation.

# ---------------------------------------------------------------------------
# Write path — spec Section 3
# ---------------------------------------------------------------------------

# Maximum number of pending writes held in the asyncio.Queue before rejecting with 503.
# The queue is an execution buffer only — it is not durable. See durability semantics (spec §7).
MAX_WRITE_QUEUE_SIZE: int = 1_000

# Maximum wall-clock time (ms) allowed for a single write transaction.
# Writes that exceed this are aborted and logged. Prevents a slow write from
# starving the read pool even though they use separate connections.
MAX_WRITE_TIME_MS: int = 500

# Number of times to retry a write on a transient DuckDB lock error before
# surfacing a 503. Covers brief contention without masking real overload.
MAX_WRITE_RETRIES: int = 3

# Queue occupancy percentage at which a warning metric is emitted.
# Reaching this threshold means the system is approaching hard capacity;
# new writes are still accepted but operators should investigate.
WRITE_QUEUE_WARNING_PCT: int = 80

# ---------------------------------------------------------------------------
# Read path — spec Section 3 and Section 9
# ---------------------------------------------------------------------------

# Global ceiling on individual read query duration (ms). Queries that exceed
# this are cancelled and the caller receives 504.
MAX_READ_QUERY_TIME_MS: int = 10_000

# asyncio.Semaphore slot count for the read pool. Requests that cannot acquire
# a slot are rejected immediately with 503 — never queued.
# Prevents parallel analytical scans from saturating CPU and indirectly
# starving the write worker.
MAX_CONCURRENT_READS: int = 10

# Number of read-only DuckDB connections in the read pool (spec §11.1).
# Read-only connections carry zero write contention.
READ_POOL_SIZE: int = 3

# ---------------------------------------------------------------------------
# Per-endpoint query limits — spec Section 9.2
# Timeout and row caps are enforced independently per endpoint category.
# If a pattern consistently hits its timeout, pre-aggregate — don't raise the limit.
# ---------------------------------------------------------------------------

# Dashboard / summary endpoints: pre-aggregated, expected to be fast.
DASHBOARD_QUERY_TIMEOUT_MS: int = 3_000
DASHBOARD_MAX_ROWS: int = 10_000

# Report generation endpoints: background-friendly, lower scheduling priority.
REPORT_QUERY_TIMEOUT_MS: int = 10_000
REPORT_MAX_ROWS: int = 100_000

# Event query endpoints: must always be filtered by time range before execution.
EVENT_QUERY_TIMEOUT_MS: int = 5_000
EVENT_MAX_ROWS: int = 50_000

# Health / status endpoint: separate fast path, must never block under load.
HEALTH_QUERY_TIMEOUT_MS: int = 500
HEALTH_MAX_ROWS: int = 100

# ---------------------------------------------------------------------------
# HTTP layer — spec Section 3
# ---------------------------------------------------------------------------

# Total concurrent HTTP sessions accepted by the server.
MAX_CONNECTIONS: int = 50

# Maximum inbound payload size in megabytes. Payloads exceeding this are
# rejected before any processing begins (413).
MAX_PAYLOAD_SIZE_MB: int = 10

# Global ceiling on rows returned in any single read response.
# Bounds response size regardless of per-endpoint limits above.
MAX_RESULT_ROWS: int = 100_000

# ---------------------------------------------------------------------------
# Idempotency — spec Section 8
# ---------------------------------------------------------------------------

# Hours that a processed request_id is retained in the deduplication store.
# Retried writes within this window return the cached result without re-executing.
# The dedup store is separate from the append log — distinct concerns (spec §8.2).
IDEMPOTENCY_KEY_TTL_HOURS: int = 24
