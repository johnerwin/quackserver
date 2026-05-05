"""
ReplayCheckpoint — persistent record of which log entries have been
projected into DuckDB.

Separate from the DedupStore (spec §8.2):
  - DedupStore has a TTL (IDEMPOTENCY_KEY_TTL_HOURS) and caches HTTP responses.
  - ReplayCheckpoint has no TTL and tracks DuckDB projection state only.

If an entry is in the checkpoint, it has been applied to DuckDB and must
not be applied again, regardless of whether the DedupStore entry is
still live.

This asymmetry matters for long-running deployments: after 24 hours the
DedupStore entry for a request_id expires, but the checkpoint entry
persists. The ReplayScanner will correctly skip that entry on any future
rebuild.

The checkpoint intentionally never deletes entries. A future compaction
step (outside Phase 2 scope) can trim entries older than the earliest
log position that has not yet been applied.
"""

import sqlite3
import time
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS replay_checkpoint (
    request_id  TEXT PRIMARY KEY,
    applied_at  REAL NOT NULL
)
"""

_IS_APPLIED = "SELECT 1 FROM replay_checkpoint WHERE request_id = ?"
_MARK = "INSERT OR IGNORE INTO replay_checkpoint (request_id, applied_at) VALUES (?, ?)"


class ReplayCheckpoint:
    """Persistent, TTL-free record of applied log entries.

    Used by:
      - ReplayScanner on startup: skip already-applied entries
      - WriteWorker after each successful write: record the applied entry
    """

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def is_applied(self, request_id: str) -> bool:
        return (
            self._conn.execute(_IS_APPLIED, (request_id,)).fetchone() is not None
        )

    def mark_applied(self, request_id: str) -> None:
        self._conn.execute(_MARK, (request_id, time.time()))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "ReplayCheckpoint":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
