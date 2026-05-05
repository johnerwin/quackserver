"""
DuckDB implementation of StorageInterface.

This is the ONLY file in this codebase that may import duckdb.
All DuckDB-specific code belongs here. If you find yourself importing
duckdb anywhere else, stop and restructure (spec §2.3, §13).

Raw SQL must never leak upward through this module's return values or
exceptions. Callers receive Result objects — not cursors, not row objects,
not DuckDB-specific error types. The StorageInterface contract is the
ceiling; what happens below it is an implementation detail.

Phase 2 will implement each method. Stubs are left intentionally empty
to preserve the import-isolation guarantee while the module exists.
"""

# TODO (Phase 2 — write path):
#   - Implement write connection ownership (single duckdb.connect())
#   - Enforce MAX_WRITE_TIME_MS via asyncio.wait_for around each write
#   - Retry on OperationalError up to MAX_WRITE_RETRIES (spec §3)
#   - Mark append log entry as applied after successful write

# TODO (Phase 2 — read path):
#   - Implement read-only connection pool (READ_POOL_SIZE connections)
#   - Wrap all reads in asyncio.Semaphore(MAX_CONCURRENT_READS)
#   - Enforce per-endpoint timeouts and MAX_RESULT_ROWS (spec §9.2)

# TODO (Phase 2 — replay / recovery):
#   - On startup, scan append log for entries not yet reflected in DuckDB
#   - Replay unprocessed entries through write worker (idempotent via request_id)
#   - Complete recovery before HTTP server starts accepting requests (spec §7.3)

# TODO (Phase 2 — bulk_import):
#   - Accept source_path to existing .duckdb file
#   - Export structured data, rebuild clean DuckDB, reclaim deleted-row space
#   - Must complete before server starts; not a live migration path

from quackserver.storage.interface import Result, StorageInterface  # noqa: F401
