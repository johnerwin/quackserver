"""Tests for DuckDBProjectionStore.

Exercises the same behavioral contracts as InMemoryProjectionStore:
  - create_user UPSERT semantics (last write wins by user_id)
  - append_event idempotency by request_id (duplicate inserts silently ignored)
  - terminal failure for missing required payload fields
  - terminal failure for unknown commands
  - no duckdb exceptions escape apply()
  - health() succeeds when the DB is open

All async tests use asyncio.run() — no external async test framework needed.
"""

import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from quackserver.core.commands import Command
from quackserver.storage.duckdb_impl import DuckDBProjectionStore
from quackserver.storage.interface import Result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cmd(request_id: str, command: str, payload: dict) -> Command:
    return Command(
        request_id=request_id,
        command=command,
        payload=payload,
        timestamp=Command.now_utc(),
    )


def _create_user(request_id: str, user_id: str, data: dict | None = None) -> Command:
    return _cmd(
        request_id,
        "create_user",
        {"user_id": user_id, "data": data or {}},
    )


def _append_event(request_id: str, event_type: str, data: dict | None = None) -> Command:
    return _cmd(
        request_id,
        "append_event",
        {"event_type": event_type, "data": data or {}},
    )


def _store(tmp_path: Path) -> DuckDBProjectionStore:
    return DuckDBProjectionStore(tmp_path / "test.duckdb")


def _read_rows(db_path: Path, table: str) -> list[dict]:
    """Open a second read-only connection to inspect table state after close()."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
        cols = [d[0] for d in conn.execute(f"DESCRIBE {table}").fetchall()]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# create_user
# ---------------------------------------------------------------------------


class TestCreateUser:
    def test_returns_success(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_create_user("r1", "alice"))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is True
        assert r.status_code == 200
        assert r.request_id == "r1"

    def test_inserts_row(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_create_user("r1", "alice", {"email": "alice@example.com"}))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "users")
        assert len(rows) == 1
        assert rows[0]["user_id"] == "alice"
        assert json.loads(rows[0]["data"]) == {"email": "alice@example.com"}
        assert rows[0]["request_id"] == "r1"

    def test_upserts_on_same_user_id(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_create_user("r1", "alice", {"v": 1}))
            await s.apply(_create_user("r2", "alice", {"v": 2}))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "users")
        assert len(rows) == 1
        assert json.loads(rows[0]["data"]) == {"v": 2}
        assert rows[0]["request_id"] == "r2"

    def test_idempotent_same_request_id(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r1 = await s.apply(_create_user("r1", "alice"))
            r2 = await s.apply(_create_user("r1", "alice"))
            await s.close()
            return r1, r2

        r1, r2 = asyncio.run(_run())
        assert r1.success is True
        assert r2.success is True
        rows = _read_rows(tmp_path / "test.duckdb", "users")
        assert len(rows) == 1

    def test_multiple_users_insert_separate_rows(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_create_user("r1", "alice"))
            await s.apply(_create_user("r2", "bob"))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "users")
        assert {r["user_id"] for r in rows} == {"alice", "bob"}

    def test_missing_user_id_returns_400_terminal(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_cmd("r1", "create_user", {"data": {}}))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is False
        assert r.status_code == 400
        assert r.retryable is False

    def test_data_defaults_to_empty_dict(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_cmd("r1", "create_user", {"user_id": "alice"}))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "users")
        assert json.loads(rows[0]["data"]) == {}

    def test_result_data_field_is_accepted(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_create_user("r1", "alice"))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.data == {"status": "accepted"}


# ---------------------------------------------------------------------------
# append_event
# ---------------------------------------------------------------------------


class TestAppendEvent:
    def test_returns_success(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_append_event("r1", "page.view"))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is True
        assert r.status_code == 200
        assert r.request_id == "r1"

    def test_inserts_row(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_append_event("r1", "page.view", {"url": "/home"}))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "events")
        assert len(rows) == 1
        assert rows[0]["event_type"] == "page.view"
        assert json.loads(rows[0]["data"]) == {"url": "/home"}
        assert rows[0]["request_id"] == "r1"

    def test_idempotent_by_request_id(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r1 = await s.apply(_append_event("r1", "page.view"))
            r2 = await s.apply(_append_event("r1", "page.view"))
            await s.close()
            return r1, r2

        r1, r2 = asyncio.run(_run())
        assert r1.success is True
        assert r2.success is True
        rows = _read_rows(tmp_path / "test.duckdb", "events")
        assert len(rows) == 1

    def test_different_request_ids_both_insert(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_append_event("r1", "page.view"))
            await s.apply(_append_event("r2", "page.view"))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "events")
        assert len(rows) == 2

    def test_missing_event_type_returns_400_terminal(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_cmd("r1", "append_event", {"data": {}}))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is False
        assert r.status_code == 400
        assert r.retryable is False

    def test_data_defaults_to_empty_dict(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_cmd("r1", "append_event", {"event_type": "click"}))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "events")
        assert json.loads(rows[0]["data"]) == {}

    def test_result_data_field_is_accepted(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_append_event("r1", "page.view"))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.data == {"status": "accepted"}

    def test_multiple_event_types_stored(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_append_event("r1", "page.view"))
            await s.apply(_append_event("r2", "user.click"))
            await s.apply(_append_event("r3", "user.login"))
            await s.close()

        asyncio.run(_run())
        rows = _read_rows(tmp_path / "test.duckdb", "events")
        event_types = {r["event_type"] for r in rows}
        assert event_types == {"page.view", "user.click", "user.login"}


# ---------------------------------------------------------------------------
# Unknown command
# ---------------------------------------------------------------------------


class TestUnknownCommand:
    def test_unknown_command_returns_400_terminal(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            r = await s.apply(_cmd("r1", "delete_everything", {}))
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is False
        assert r.status_code == 400
        assert r.retryable is False
        assert "unknown command" in (r.error or "").lower()

    def test_unknown_command_does_not_raise(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            result = await s.apply(_cmd("r1", "DROP TABLE users", {}))
            await s.close()
            return result

        result = asyncio.run(_run())
        assert isinstance(result, Result)
        assert result.success is False


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_returns_ok(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s._ensure_open()
            r = await s.health()
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is True
        assert r.status_code == 200

    def test_health_after_write(self, tmp_path):
        async def _run():
            s = _store(tmp_path)
            await s.apply(_create_user("r1", "alice"))
            r = await s.health()
            await s.close()
            return r

        r = asyncio.run(_run())
        assert r.success is True


# ---------------------------------------------------------------------------
# Persistence — data survives close/reopen
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_users_survive_reopen(self, tmp_path):
        db_path = tmp_path / "test.duckdb"

        async def _write():
            s = DuckDBProjectionStore(db_path)
            await s.apply(_create_user("r1", "alice", {"v": 42}))
            await s.close()

        async def _read():
            s = DuckDBProjectionStore(db_path)
            await s._ensure_open()
            await s.close()

        asyncio.run(_write())
        asyncio.run(_read())
        rows = _read_rows(db_path, "users")
        assert len(rows) == 1
        assert rows[0]["user_id"] == "alice"

    def test_events_survive_reopen(self, tmp_path):
        db_path = tmp_path / "test.duckdb"

        async def _write():
            s = DuckDBProjectionStore(db_path)
            await s.apply(_append_event("r1", "page.view"))
            await s.apply(_append_event("r2", "user.login"))
            await s.close()

        asyncio.run(_write())
        rows = _read_rows(db_path, "events")
        assert len(rows) == 2

    def test_idempotency_preserved_across_reopen(self, tmp_path):
        db_path = tmp_path / "test.duckdb"

        async def _write():
            s = DuckDBProjectionStore(db_path)
            await s.apply(_append_event("r1", "page.view"))
            await s.close()

        async def _replay():
            s = DuckDBProjectionStore(db_path)
            r = await s.apply(_append_event("r1", "page.view"))
            await s.close()
            return r

        asyncio.run(_write())
        r = asyncio.run(_replay())
        assert r.success is True
        rows = _read_rows(db_path, "events")
        assert len(rows) == 1
