"""
Replay Idempotency Invariant
============================

Formal statement:
    Replaying the append log any number of times must produce identical
    projection state.

This is a foundational property, independent of runtime dedup and checkpoint
guarantees. Even if those layers fail (corrupted checkpoint, interrupted replay,
bug in dedup TTL), the projection must converge to the same state on every
full replay.

Why it matters:
    - Recovery confidence: operators can rebuild without verifying intermediate state.
    - Migration safety: replaying into a new schema version produces a known result.
    - Compaction safety: the rebuilt DB is semantically equivalent to the original.
    - Operational psychology: "drop and rebuild" is safe without heroics.

Tested against both InMemoryProjectionStore (fast, always) and
DuckDBProjectionStore (integration, uses tmp_path). Both implementations must
satisfy the invariant — it is not acceptable for DuckDB to be weaker here.
"""

import asyncio
from pathlib import Path

import duckdb
import pytest

from quackserver.core.commands import Command
from quackserver.core.projection import InMemoryProjectionStore
from quackserver.storage.duckdb_impl import DuckDBProjectionStore


# ---------------------------------------------------------------------------
# Shared test data — a realistic log sequence with edge cases
# ---------------------------------------------------------------------------

_TIMESTAMP = "2026-05-07T12:00:00+00:00"


def _cmd(request_id: str, command: str, payload: dict) -> Command:
    return Command(
        request_id=request_id,
        command=command,
        payload=payload,
        timestamp=_TIMESTAMP,
    )


# A log sequence that exercises all idempotency edge cases:
#   - create_user appears twice with same request_id (dup in log)
#   - same user_id created again with different request_id (UPSERT)
#   - append_event appears twice with same request_id (dup in log)
#   - unknown command (terminal, should not corrupt state)
LOG_SEQUENCE = [
    _cmd("u-1",  "create_user",  {"user_id": "alice", "data": {"role": "viewer"}}),
    _cmd("u-1",  "create_user",  {"user_id": "alice", "data": {"role": "viewer"}}),  # dup
    _cmd("u-2",  "create_user",  {"user_id": "alice", "data": {"role": "admin"}}),   # upsert
    _cmd("u-3",  "create_user",  {"user_id": "bob",   "data": {}}),
    _cmd("e-1",  "append_event", {"event_type": "login", "data": {"ip": "1.2.3.4"}}),
    _cmd("e-1",  "append_event", {"event_type": "login", "data": {"ip": "1.2.3.4"}}),  # dup
    _cmd("e-2",  "append_event", {"event_type": "page.view", "data": {}}),
    _cmd("bad",  "unknown_cmd",  {}),  # terminal failure — must not corrupt state
]


async def _apply_sequence(store, commands):
    for cmd in commands:
        await store.apply(cmd)


# ---------------------------------------------------------------------------
# InMemoryProjectionStore
# ---------------------------------------------------------------------------


class TestInMemoryReplayInvariant:
    """The invariant holds for InMemoryProjectionStore."""

    def test_single_replay_produces_correct_state(self):
        store = InMemoryProjectionStore()
        asyncio.run(_apply_sequence(store, LOG_SEQUENCE))
        assert store.users == {"alice": {"role": "admin"}, "bob": {}}
        assert len(store.events) == 2

    def test_second_replay_produces_identical_state(self):
        store = InMemoryProjectionStore()
        asyncio.run(_apply_sequence(store, LOG_SEQUENCE))
        state_after_1 = (dict(store.users), list(store.events), frozenset(store.applied))

        # Replay again on the same store (simulates interrupted replay resuming)
        asyncio.run(_apply_sequence(store, LOG_SEQUENCE))
        state_after_2 = (dict(store.users), list(store.events), frozenset(store.applied))

        assert state_after_1 == state_after_2

    def test_n_replays_produce_identical_state(self):
        store = InMemoryProjectionStore()
        asyncio.run(_apply_sequence(store, LOG_SEQUENCE))
        reference = (dict(store.users), list(store.events), frozenset(store.applied))

        for _ in range(4):
            asyncio.run(_apply_sequence(store, LOG_SEQUENCE))
            current = (dict(store.users), list(store.events), frozenset(store.applied))
            assert current == reference

    def test_two_fresh_stores_replayed_once_are_identical(self):
        """Two independent stores replayed from the same log must converge identically."""
        store_a = InMemoryProjectionStore()
        store_b = InMemoryProjectionStore()
        asyncio.run(_apply_sequence(store_a, LOG_SEQUENCE))
        asyncio.run(_apply_sequence(store_b, LOG_SEQUENCE))
        assert store_a.users == store_b.users
        assert store_a.events == store_b.events


# ---------------------------------------------------------------------------
# DuckDBProjectionStore — same invariant must hold for the real implementation
# ---------------------------------------------------------------------------


def _duckdb_state(db_path: Path) -> tuple:
    """Read (users, events) from a closed DuckDB file for comparison."""
    conn = duckdb.connect(str(db_path), read_only=True)
    users = dict(conn.execute("SELECT user_id, data FROM users ORDER BY user_id").fetchall())
    events = conn.execute(
        "SELECT request_id, event_type FROM events ORDER BY request_id"
    ).fetchall()
    conn.close()
    return users, events


async def _duckdb_replay(db_path: Path, commands) -> None:
    store = DuckDBProjectionStore(db_path)
    await store.initialize()
    await _apply_sequence(store, commands)
    await store.close()


class TestDuckDBReplayInvariant:
    """The invariant holds for DuckDBProjectionStore."""

    def test_second_replay_produces_identical_duckdb_state(self, tmp_path):
        db = tmp_path / "test.duckdb"
        asyncio.run(_duckdb_replay(db, LOG_SEQUENCE))
        state_after_1 = _duckdb_state(db)

        asyncio.run(_duckdb_replay(db, LOG_SEQUENCE))
        state_after_2 = _duckdb_state(db)

        assert state_after_1 == state_after_2

    def test_n_replays_produce_identical_duckdb_state(self, tmp_path):
        db = tmp_path / "test.duckdb"
        asyncio.run(_duckdb_replay(db, LOG_SEQUENCE))
        reference = _duckdb_state(db)

        for _ in range(3):
            asyncio.run(_duckdb_replay(db, LOG_SEQUENCE))
            assert _duckdb_state(db) == reference

    def test_two_fresh_dbs_replayed_once_are_identical(self, tmp_path):
        db_a = tmp_path / "a.duckdb"
        db_b = tmp_path / "b.duckdb"
        asyncio.run(_duckdb_replay(db_a, LOG_SEQUENCE))
        asyncio.run(_duckdb_replay(db_b, LOG_SEQUENCE))
        assert _duckdb_state(db_a) == _duckdb_state(db_b)

    def test_projection_metadata_records_schema_version(self, tmp_path):
        db = tmp_path / "test.duckdb"
        asyncio.run(_duckdb_replay(db, LOG_SEQUENCE))
        conn = duckdb.connect(str(db), read_only=True)
        version = conn.execute(
            "SELECT value FROM projection_metadata WHERE key = 'schema_version'"
        ).fetchone()
        conn.close()
        assert version is not None
        assert version[0] == "1"

    def test_projection_metadata_records_rebuild_provenance(self, tmp_path):
        db = tmp_path / "test.duckdb"

        async def _with_rebuild():
            store = DuckDBProjectionStore(db)
            await store.initialize()
            await _apply_sequence(store, LOG_SEQUENCE)
            await store.record_rebuild(
                log_path="/path/to/quack.jsonl",
                entry_count=6,
                rebuilt_at="2026-05-07T12:00:00+00:00",
            )
            await store.close()

        asyncio.run(_with_rebuild())
        conn = duckdb.connect(str(db), read_only=True)
        rows = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT key, value FROM projection_metadata ORDER BY key"
            ).fetchall()
        }
        conn.close()
        assert rows["schema_version"] == "1"
        assert rows["rebuilt_from_log"] == "/path/to/quack.jsonl"
        assert rows["replay_entry_count"] == "6"
        assert rows["rebuilt_at"] == "2026-05-07T12:00:00+00:00"
