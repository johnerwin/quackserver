"""
DuckDB implementation of ProjectionStore and ReadStore.

This is the ONLY file in this codebase that may import duckdb.
All DuckDB-specific code belongs here. If you find yourself importing
duckdb anywhere else, stop and restructure (spec §2.3, §13).

Raw SQL must never leak upward through this module's return values or
exceptions. Callers receive Result objects — not cursors, not row objects,
not DuckDB-specific error types.

Schema (append-only write path):
  users   — UPSERT by user_id; last write wins for data and request_id.
  events  — unique by request_id; duplicate inserts are silently ignored.

Write error classification:
  duckdb.IOException  -> retryable=True  (file lock, transient I/O)
  other duckdb.Error  -> retryable=False (schema mismatch, constraint, etc.)
  malformed payload   -> retryable=False (terminal; retrying cannot help)
  unknown command     -> retryable=False (terminal)

Connection model (spec §11):
  Write path: single connection on a 1-thread executor. Serialises all mutations.
  Read path: READ_POOL_SIZE connections on a separate executor, one per reader
  thread (threading.local). DuckDB allows multiple in-process read-write
  connections to the same file; read_only=True is not used because DuckDB
  rejects it when a read-write connection is already open in the same process.
  Concurrent reads and writes are handled by DuckDB's internal WAL.

No duckdb exception escapes this module. All exceptions are caught and
returned as Result objects.
"""

import asyncio
import concurrent.futures
import functools
import json
import threading
from pathlib import Path

import duckdb

from quackserver.config import READ_POOL_SIZE
from quackserver.core.commands import Command
from quackserver.core.projection import ProjectionStore
from quackserver.core.read_store import ReadStore
from quackserver.core.structured_log import get_logger
from quackserver.storage.interface import Result

_log = get_logger(__name__)

# Increment when the schema changes so rebuilds can record what they produced.
PROJECTION_SCHEMA_VERSION = "1"

_SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id    VARCHAR PRIMARY KEY,
        data       VARCHAR NOT NULL,
        request_id VARCHAR NOT NULL,
        created_at VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        request_id  VARCHAR PRIMARY KEY,
        event_type  VARCHAR NOT NULL,
        data        VARCHAR NOT NULL,
        received_at VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS projection_metadata (
        key        VARCHAR PRIMARY KEY,
        value      VARCHAR NOT NULL,
        updated_at VARCHAR NOT NULL
    )
    """,
]


class DuckDBProjectionStore(ProjectionStore, ReadStore):
    """Writes create_user and append_event commands to a DuckDB file.

    Two executors, two connection scopes:
    - _executor (1 thread): exclusive write connection; all mutations run here.
    - _read_executor (READ_POOL_SIZE threads): each thread holds its own
      connection via threading.local; read queries run here, isolated from writes.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="duckdb-write"
        )
        self._read_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=READ_POOL_SIZE, thread_name_prefix="duckdb-read"
        )
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._read_local = threading.local()

    # ------------------------------------------------------------------
    # ProjectionStore interface
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        await self._ensure_open()

    async def apply(self, command: Command) -> Result:
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        if command.command == "create_user":
            return await loop.run_in_executor(
                self._executor, self._create_user_sync, command
            )
        if command.command == "append_event":
            return await loop.run_in_executor(
                self._executor, self._append_event_sync, command
            )
        return Result(
            success=False,
            status_code=400,
            error=f"unknown command: {command.command!r}",
            request_id=command.request_id,
            retryable=False,
        )

    async def health(self) -> Result:
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._health_sync)

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._read_executor, self._close_read_connections_sync
        )
        self._read_executor.shutdown(wait=True)
        await loop.run_in_executor(self._executor, self._close_sync)
        self._executor.shutdown(wait=True)

    # ------------------------------------------------------------------
    # ReadStore interface
    # Each method opens an independent read-only connection so reads
    # never contend with the write connection.
    # ------------------------------------------------------------------

    async def dashboard(self) -> Result:
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._read_executor, self._dashboard_sync)

    async def events(self, limit: int) -> Result:
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._read_executor, functools.partial(self._events_sync, limit)
        )

    async def users(self, limit: int) -> Result:
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._read_executor, functools.partial(self._users_sync, limit)
        )

    # ------------------------------------------------------------------
    # Synchronous helpers — run exclusively on the executor thread
    # ------------------------------------------------------------------

    def _open_sync(self) -> None:
        if self._conn is not None:
            return  # concurrent _ensure_open calls — idempotent
        self._conn = duckdb.connect(str(self._db_path))
        for stmt in _SCHEMA_SQL:
            self._conn.execute(stmt)
        self._conn.execute(
            """
            INSERT INTO projection_metadata (key, value, updated_at)
            VALUES ('schema_version', ?, ?)
            ON CONFLICT (key) DO NOTHING
            """,
            [PROJECTION_SCHEMA_VERSION, Command.now_utc()],
        )
        self._conn.execute(
            """
            INSERT INTO projection_metadata (key, value, updated_at)
            VALUES ('initialized_at', ?, ?)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            [Command.now_utc(), Command.now_utc()],
        )

    def _create_user_sync(self, command: Command) -> Result:
        p = command.payload
        user_id = p.get("user_id")
        if not user_id:
            return Result(
                success=False,
                status_code=400,
                error="create_user: missing user_id",
                request_id=command.request_id,
                retryable=False,
            )
        try:
            data = json.dumps(p.get("data", {}))
            self._conn.execute(
                """
                INSERT INTO users (user_id, data, request_id, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    data       = excluded.data,
                    request_id = excluded.request_id
                """,
                [user_id, data, command.request_id, command.timestamp],
            )
            return Result(
                success=True,
                status_code=200,
                data={"status": "accepted"},
                request_id=command.request_id,
            )
        except duckdb.IOException as exc:
            _log.warning(
                "duckdb_io_error",
                command="create_user",
                request_id=command.request_id,
                error=str(exc),
            )
            return Result(
                success=False,
                status_code=503,
                error=f"transient storage error: {exc}",
                request_id=command.request_id,
                retryable=True,
            )
        except duckdb.Error as exc:
            _log.error(
                "duckdb_error",
                command="create_user",
                request_id=command.request_id,
                error=str(exc),
            )
            return Result(
                success=False,
                status_code=500,
                error=f"storage error: {exc}",
                request_id=command.request_id,
                retryable=False,
            )

    def _append_event_sync(self, command: Command) -> Result:
        p = command.payload
        event_type = p.get("event_type")
        if not event_type:
            return Result(
                success=False,
                status_code=400,
                error="append_event: missing event_type",
                request_id=command.request_id,
                retryable=False,
            )
        try:
            data = json.dumps(p.get("data", {}))
            self._conn.execute(
                """
                INSERT INTO events (request_id, event_type, data, received_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (request_id) DO NOTHING
                """,
                [command.request_id, event_type, data, command.timestamp],
            )
            return Result(
                success=True,
                status_code=200,
                data={"status": "accepted"},
                request_id=command.request_id,
            )
        except duckdb.IOException as exc:
            _log.warning(
                "duckdb_io_error",
                command="append_event",
                request_id=command.request_id,
                error=str(exc),
            )
            return Result(
                success=False,
                status_code=503,
                error=f"transient storage error: {exc}",
                request_id=command.request_id,
                retryable=True,
            )
        except duckdb.Error as exc:
            _log.error(
                "duckdb_error",
                command="append_event",
                request_id=command.request_id,
                error=str(exc),
            )
            return Result(
                success=False,
                status_code=500,
                error=f"storage error: {exc}",
                request_id=command.request_id,
                retryable=False,
            )

    def _health_sync(self) -> Result:
        try:
            self._conn.execute("SELECT 1")
            return Result(success=True, status_code=200, data={"status": "ok"})
        except duckdb.Error as exc:
            return Result(
                success=False,
                status_code=503,
                error=f"health check failed: {exc}",
                retryable=True,
            )

    def _get_read_conn_sync(self) -> duckdb.DuckDBPyConnection:
        """Return this thread's read connection, opening it lazily on first use."""
        if not hasattr(self._read_local, "conn") or self._read_local.conn is None:
            self._read_local.conn = duckdb.connect(str(self._db_path))
        return self._read_local.conn

    def _close_read_connections_sync(self) -> None:
        """Close this thread's read connection. Called on each reader thread at shutdown."""
        conn = getattr(self._read_local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except duckdb.Error as exc:
                _log.warning("duckdb_read_close_error", error=str(exc))
            self._read_local.conn = None

    def _close_sync(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except duckdb.Error as exc:
                _log.warning("duckdb_close_error", error=str(exc))
            self._conn = None

    def _dashboard_sync(self) -> Result:
        conn = self._get_read_conn_sync()
        try:
            user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            by_type = conn.execute(
                "SELECT event_type, COUNT(*) AS cnt"
                " FROM events GROUP BY event_type ORDER BY cnt DESC"
            ).fetchall()
            return Result(
                success=True,
                status_code=200,
                data={
                    "user_count": user_count,
                    "event_count": event_count,
                    "events_by_type": [
                        {"event_type": row[0], "count": row[1]} for row in by_type
                    ],
                },
            )
        except duckdb.Error as exc:
            _log.error("duckdb_read_error", query="dashboard", error=str(exc))
            return Result(
                success=False,
                status_code=500,
                error=f"dashboard query failed: {exc}",
                retryable=False,
            )

    def _events_sync(self, limit: int) -> Result:
        conn = self._get_read_conn_sync()
        try:
            rows = conn.execute(
                "SELECT request_id, event_type, data, received_at"
                " FROM events ORDER BY received_at DESC LIMIT ?",
                [limit],
            ).fetchall()
            return Result(
                success=True,
                status_code=200,
                data={
                    "events": [
                        {
                            "request_id": r[0],
                            "event_type": r[1],
                            "data": json.loads(r[2]),
                            "received_at": r[3],
                        }
                        for r in rows
                    ],
                    "count": len(rows),
                },
            )
        except duckdb.Error as exc:
            _log.error("duckdb_read_error", query="events", error=str(exc))
            return Result(
                success=False,
                status_code=500,
                error=f"events query failed: {exc}",
                retryable=False,
            )

    def _users_sync(self, limit: int) -> Result:
        conn = self._get_read_conn_sync()
        try:
            rows = conn.execute(
                "SELECT user_id, data, created_at FROM users ORDER BY user_id LIMIT ?",
                [limit],
            ).fetchall()
            return Result(
                success=True,
                status_code=200,
                data={
                    "users": [
                        {
                            "user_id": r[0],
                            "data": json.loads(r[1]),
                            "created_at": r[2],
                        }
                        for r in rows
                    ],
                    "count": len(rows),
                },
            )
        except duckdb.Error as exc:
            _log.error("duckdb_read_error", query="users", error=str(exc))
            return Result(
                success=False,
                status_code=500,
                error=f"users query failed: {exc}",
                retryable=False,
            )

    async def metadata(self) -> Result:
        """Return all projection_metadata key/value pairs."""
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._read_executor, self._metadata_sync)

    async def record_rebuild(
        self, *, log_path: str, entry_count: int, rebuilt_at: str
    ) -> None:
        """Write rebuild provenance into projection_metadata.

        Called by the rebuild CLI after a successful replay so the resulting
        database carries a record of what generated it and when.
        """
        await self._ensure_open()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor,
            self._record_rebuild_sync,
            log_path,
            entry_count,
            rebuilt_at,
        )

    def _metadata_sync(self) -> Result:
        conn = self._get_read_conn_sync()
        try:
            rows = conn.execute(
                "SELECT key, value, updated_at FROM projection_metadata ORDER BY key"
            ).fetchall()
            return Result(
                success=True,
                status_code=200,
                data={r[0]: {"value": r[1], "updated_at": r[2]} for r in rows},
            )
        except duckdb.Error as exc:
            return Result(
                success=False,
                status_code=500,
                error=f"metadata query failed: {exc}",
                retryable=False,
            )

    def _record_rebuild_sync(
        self, log_path: str, entry_count: int, rebuilt_at: str
    ) -> None:
        for key, value in [
            ("rebuilt_at", rebuilt_at),
            ("rebuilt_from_log", log_path),
            ("replay_entry_count", str(entry_count)),
        ]:
            self._conn.execute(
                """
                INSERT INTO projection_metadata (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                [key, value, rebuilt_at],
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _ensure_open(self) -> None:
        if self._conn is None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._open_sync)
