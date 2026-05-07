"""
Tests for `quackserver rebuild`.

Verifies:
  - Basic replay: users and events are rebuilt correctly
  - Idempotency: duplicate request_ids in the log produce correct final state
  - Overwrite flag: required when db-path already exists
  - Missing log: clean error exit
  - Malformed log lines: skipped, counted, rebuild continues
  - Empty log: succeeds with zero counts
"""

import asyncio
import json
from pathlib import Path

import duckdb
import pytest

from quackserver.cli.rebuild import _run, main as rebuild_main
from quackserver.core.log import AppendLogWriter
from quackserver.core.commands import Command


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_log(path: Path, entries: list[dict]) -> None:
    """Write raw JSONL entries to a log file (bypasses AppendLogWriter validation)."""
    with path.open("w", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry) + "\n")


def _read_db(db_path: Path) -> tuple[list, list]:
    """Return (users, events) rows from a DuckDB file (read-only)."""
    conn = duckdb.connect(str(db_path), read_only=True)
    users = conn.execute("SELECT user_id FROM users ORDER BY user_id").fetchall()
    events = conn.execute(
        "SELECT request_id, event_type FROM events ORDER BY received_at"
    ).fetchall()
    conn.close()
    return [r[0] for r in users], [(r[0], r[1]) for r in events]


def _cmd_dict(request_id: str, command: str, payload: dict) -> dict:
    return {
        "request_id": request_id,
        "command": command,
        "payload": payload,
        "timestamp": "2026-05-07T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# Core correctness
# ---------------------------------------------------------------------------


class TestRebuildBasic:
    def test_creates_users_and_events(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        _write_log(log, [
            _cmd_dict("u1", "create_user", {"user_id": "alice", "data": {}}),
            _cmd_dict("u2", "create_user", {"user_id": "bob",   "data": {}}),
            _cmd_dict("e1", "append_event", {"event_type": "page.view", "data": {}}),
            _cmd_dict("e2", "append_event", {"event_type": "user.login", "data": {}}),
        ])
        stats = asyncio.run(_run(log, db))
        assert stats["total"] == 4
        assert stats["create_user"] == 2
        assert stats["append_event"] == 2
        assert stats["errors"] == 0
        users, events = _read_db(db)
        assert sorted(users) == ["alice", "bob"]
        assert len(events) == 2

    def test_empty_log_succeeds(self, tmp_path):
        log = tmp_path / "empty.jsonl"
        log.write_text("")
        db = tmp_path / "quack.duckdb"
        stats = asyncio.run(_run(log, db))
        assert stats["total"] == 0
        assert stats["errors"] == 0
        users, events = _read_db(db)
        assert users == []
        assert events == []

    def test_unknown_command_counted_not_crashed(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        _write_log(log, [
            _cmd_dict("x1", "delete_user", {"user_id": "alice"}),
        ])
        stats = asyncio.run(_run(log, db))
        assert stats["errors"] == 1
        assert stats["unknown_command"] == 0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestRebuildIdempotency:
    def test_duplicate_request_ids_produce_correct_counts(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        # Same request_id appears twice (simulates retry in the append log)
        _write_log(log, [
            _cmd_dict("u1", "create_user", {"user_id": "alice", "data": {}}),
            _cmd_dict("u1", "create_user", {"user_id": "alice", "data": {}}),  # dup
            _cmd_dict("e1", "append_event", {"event_type": "page.view", "data": {}}),
            _cmd_dict("e1", "append_event", {"event_type": "page.view", "data": {}}),  # dup
        ])
        stats = asyncio.run(_run(log, db))
        assert stats["total"] == 2  # dups are skipped before apply
        assert stats["create_user"] == 1
        assert stats["append_event"] == 1
        users, events = _read_db(db)
        assert users == ["alice"]
        assert len(events) == 1

    def test_user_upsert_last_write_wins(self, tmp_path):
        """Two create_user with same user_id but different request_ids: last wins."""
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        _write_log(log, [
            _cmd_dict("u1", "create_user", {"user_id": "alice", "data": {"role": "viewer"}}),
            _cmd_dict("u2", "create_user", {"user_id": "alice", "data": {"role": "admin"}}),
        ])
        stats = asyncio.run(_run(log, db))
        assert stats["total"] == 2
        assert stats["create_user"] == 2
        conn = duckdb.connect(str(db), read_only=True)
        row = conn.execute("SELECT data FROM users WHERE user_id = 'alice'").fetchone()
        conn.close()
        import json as _json
        assert _json.loads(row[0])["role"] == "admin"

    def test_rebuild_twice_same_result(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db1 = tmp_path / "rebuild1.duckdb"
        db2 = tmp_path / "rebuild2.duckdb"
        _write_log(log, [
            _cmd_dict("u1", "create_user", {"user_id": "alice", "data": {}}),
            _cmd_dict("e1", "append_event", {"event_type": "page.view", "data": {}}),
        ])
        asyncio.run(_run(log, db1))
        asyncio.run(_run(log, db2))
        users1, events1 = _read_db(db1)
        users2, events2 = _read_db(db2)
        assert users1 == users2
        assert events1 == events2


# ---------------------------------------------------------------------------
# Malformed log lines
# ---------------------------------------------------------------------------


class TestRebuildMalformed:
    def test_malformed_lines_skipped_rebuild_continues(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        with log.open("w") as fh:
            fh.write(json.dumps(_cmd_dict("u1", "create_user", {"user_id": "alice", "data": {}})) + "\n")
            fh.write("this is not json\n")
            fh.write(json.dumps(_cmd_dict("e1", "append_event", {"event_type": "page.view", "data": {}})) + "\n")
        stats = asyncio.run(_run(log, db))
        assert stats["malformed_log_lines"] == 1
        assert stats["total"] == 2
        users, events = _read_db(db)
        assert users == ["alice"]
        assert len(events) == 1


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


class TestRebuildCLI:
    def test_cli_rebuilds_correctly(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        _write_log(log, [
            _cmd_dict("u1", "create_user", {"user_id": "carol", "data": {}}),
            _cmd_dict("e1", "append_event", {"event_type": "signup", "data": {}}),
        ])
        rebuild_main(["--log-path", str(log), "--db-path", str(db)])
        users, events = _read_db(db)
        assert users == ["carol"]
        assert len(events) == 1

    def test_cli_errors_if_db_exists_without_overwrite(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        _write_log(log, [])
        db.write_text("placeholder")
        with pytest.raises(SystemExit) as exc:
            rebuild_main(["--log-path", str(log), "--db-path", str(db)])
        assert exc.value.code != 0

    def test_cli_overwrite_replaces_existing_db(self, tmp_path):
        log = tmp_path / "quack.jsonl"
        db = tmp_path / "quack.duckdb"
        _write_log(log, [
            _cmd_dict("u1", "create_user", {"user_id": "dave", "data": {}}),
        ])
        # First build
        rebuild_main(["--log-path", str(log), "--db-path", str(db)])
        # Second build with --overwrite
        rebuild_main(["--log-path", str(log), "--db-path", str(db), "--overwrite"])
        users, _ = _read_db(db)
        assert users == ["dave"]

    def test_cli_errors_if_log_missing(self, tmp_path):
        db = tmp_path / "quack.duckdb"
        with pytest.raises(SystemExit) as exc:
            rebuild_main(["--log-path", str(tmp_path / "no.jsonl"), "--db-path", str(db)])
        assert exc.value.code != 0
