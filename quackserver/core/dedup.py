"""
DedupStore — SQLite-backed idempotency cache.

Separate from the append log and the replay checkpoint (spec §8.2):
  - Append log:         write journal, no TTL, canonical truth
  - ReplayCheckpoint:   tracks DuckDB projection state, no TTL
  - DedupStore:         caches HTTP response by request_id, has TTL

Lookup happens BEFORE writing to the append log (spec §8.2).
If the request_id is found within the TTL window, the cached result
is returned without re-executing the command.

The result is stored AFTER the write worker successfully applies the
entry to DuckDB. This means there is a short window between "log write
accepted" and "dedup entry available" where a retry will not be deduplicated
at this layer. Replay safety for that window is guaranteed by the
ReplayCheckpoint — the log entry is applied at most once regardless.
"""

import json
import sqlite3
import time
from pathlib import Path

from quackserver.config import IDEMPOTENCY_KEY_TTL_HOURS
from quackserver.storage.interface import Result

_SCHEMA = """
CREATE TABLE IF NOT EXISTS dedup_store (
    request_id   TEXT    PRIMARY KEY,
    status_code  INTEGER NOT NULL,
    success      INTEGER NOT NULL,
    data         TEXT,
    error        TEXT,
    duration_ms  INTEGER,
    stored_at    REAL    NOT NULL
)
"""

_LOOKUP = """
SELECT success, status_code, data, error, duration_ms
FROM dedup_store
WHERE request_id = ? AND stored_at > ?
"""

_UPSERT = """
INSERT OR REPLACE INTO dedup_store
    (request_id, status_code, success, data, error, duration_ms, stored_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

_PURGE = "DELETE FROM dedup_store WHERE stored_at <= ?"


class DedupStore:
    """SQLite-backed request_id deduplication cache with TTL expiry.

    All I/O is synchronous. In the async HTTP phase the caller wraps
    get/put in run_in_executor. For Phase 2 tests it is called directly.
    """

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def _cutoff(self) -> float:
        return time.time() - (IDEMPOTENCY_KEY_TTL_HOURS * 3600)

    def get(self, request_id: str) -> Result | None:
        """Return the cached Result for request_id, or None if absent/expired."""
        row = self._conn.execute(_LOOKUP, (request_id, self._cutoff())).fetchone()
        if row is None:
            return None
        success, status_code, data_json, error, duration_ms = row
        return Result(
            success=bool(success),
            status_code=status_code,
            data=json.loads(data_json) if data_json is not None else None,
            error=error,
            request_id=request_id,
            duration_ms=duration_ms,
        )

    def put(self, request_id: str, result: Result) -> None:
        """Cache result for request_id. Called after successful DuckDB projection."""
        self._conn.execute(
            _UPSERT,
            (
                request_id,
                result.status_code,
                int(result.success),
                json.dumps(result.data) if result.data is not None else None,
                result.error,
                result.duration_ms,
                time.time(),
            ),
        )
        self._conn.commit()

    def purge_expired(self) -> int:
        """Delete entries older than IDEMPOTENCY_KEY_TTL_HOURS. Returns count deleted."""
        cursor = self._conn.execute(_PURGE, (self._cutoff(),))
        self._conn.commit()
        return cursor.rowcount

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DedupStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
