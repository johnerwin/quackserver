"""Projection idempotency tests against InMemoryProjectionStore.

These tests validate projection-layer idempotency — a separate guarantee
from runtime idempotency (request_id dedup + checkpoint).

Runtime idempotency guarantees the same request_id is not processed twice
under normal operation. Projection idempotency guarantees that even if the
same command IS applied twice (bug, interrupted replay, corrupted checkpoint),
the resulting state is correct.

These tests prove:
  - Applying the same command twice produces the same state as applying it once.
  - The state is correct regardless of how many times the command arrives.
  - Unknown and malformed commands are terminal failures (retryable=False).
"""

import asyncio

import pytest

from quackserver.core.commands import Command
from quackserver.core.projection import InMemoryProjectionStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply(store: InMemoryProjectionStore, command: Command):
    return asyncio.run(store.apply(command))


def _cmd_create_user(
    request_id: str,
    user_id: str,
    data: dict | None = None,
) -> Command:
    return Command(
        request_id=request_id,
        command="create_user",
        payload={"user_id": user_id, "data": data or {}},
        timestamp=Command.now_utc(),
    )


def _cmd_append_event(
    request_id: str,
    event_type: str,
    data: dict | None = None,
) -> Command:
    return Command(
        request_id=request_id,
        command="append_event",
        payload={"event_type": event_type, "data": data or {}},
        timestamp=Command.now_utc(),
    )


# ---------------------------------------------------------------------------
# create_user idempotency
# ---------------------------------------------------------------------------


class TestCreateUserIdempotency:
    def test_single_apply_creates_user(self):
        store = InMemoryProjectionStore()
        result = _apply(store, _cmd_create_user("req-1", "alice"))
        assert result.success is True
        assert "alice" in store.users

    def test_double_apply_same_request_id_is_noop(self):
        store = InMemoryProjectionStore()
        _apply(store, _cmd_create_user("req-1", "alice", {"email": "a@example.com"}))
        _apply(store, _cmd_create_user("req-1", "alice", {"email": "different@example.com"}))
        # State must reflect the first apply, not the second
        assert store.users["alice"] == {"email": "a@example.com"}
        assert len(store.applied) == 1

    def test_double_apply_both_return_success(self):
        store = InMemoryProjectionStore()
        r1 = _apply(store, _cmd_create_user("req-1", "alice"))
        r2 = _apply(store, _cmd_create_user("req-1", "alice"))
        assert r1.success is True
        assert r2.success is True

    def test_upsert_different_request_ids_same_user_id(self):
        """Two different writes for the same user_id: UPSERT semantics."""
        store = InMemoryProjectionStore()
        _apply(store, _cmd_create_user("req-1", "alice", {"role": "user"}))
        _apply(store, _cmd_create_user("req-2", "alice", {"role": "admin"}))
        # Second write overwrites (UPSERT)
        assert store.users["alice"] == {"role": "admin"}
        assert len(store.applied) == 2

    def test_triple_apply_same_request_id(self):
        store = InMemoryProjectionStore()
        cmd = _cmd_create_user("req-1", "bob")
        _apply(store, cmd)
        _apply(store, cmd)
        _apply(store, cmd)
        assert len(store.applied) == 1
        assert len(store.users) == 1

    def test_missing_user_id_returns_terminal_failure(self):
        store = InMemoryProjectionStore()
        cmd = Command("req-x", "create_user", {"data": {}}, Command.now_utc())
        result = _apply(store, cmd)
        assert result.success is False
        assert result.retryable is False
        assert "req-x" not in store.applied

    def test_multiple_distinct_users(self):
        store = InMemoryProjectionStore()
        for i in range(5):
            _apply(store, _cmd_create_user(f"req-{i}", f"user-{i}"))
        assert len(store.users) == 5
        assert len(store.applied) == 5


# ---------------------------------------------------------------------------
# append_event idempotency
# ---------------------------------------------------------------------------


class TestAppendEventIdempotency:
    def test_single_apply_appends_event(self):
        store = InMemoryProjectionStore()
        result = _apply(store, _cmd_append_event("req-1", "page.view"))
        assert result.success is True
        assert len(store.events) == 1
        assert store.events[0]["event_type"] == "page.view"

    def test_double_apply_same_request_id_does_not_duplicate(self):
        store = InMemoryProjectionStore()
        cmd = _cmd_append_event("req-1", "page.view")
        _apply(store, cmd)
        _apply(store, cmd)
        assert len(store.events) == 1

    def test_double_apply_both_return_success(self):
        store = InMemoryProjectionStore()
        cmd = _cmd_append_event("req-1", "page.view")
        r1 = _apply(store, cmd)
        r2 = _apply(store, cmd)
        assert r1.success is True
        assert r2.success is True

    def test_different_request_ids_appends_separate_rows(self):
        """Two different request_ids for the same event type → two rows."""
        store = InMemoryProjectionStore()
        _apply(store, _cmd_append_event("req-1", "page.view"))
        _apply(store, _cmd_append_event("req-2", "page.view"))
        assert len(store.events) == 2

    def test_triple_apply_same_request_id(self):
        store = InMemoryProjectionStore()
        cmd = _cmd_append_event("req-1", "user.login")
        _apply(store, cmd)
        _apply(store, cmd)
        _apply(store, cmd)
        assert len(store.events) == 1
        assert len(store.applied) == 1

    def test_missing_event_type_returns_terminal_failure(self):
        store = InMemoryProjectionStore()
        cmd = Command("req-x", "append_event", {"data": {}}, Command.now_utc())
        result = _apply(store, cmd)
        assert result.success is False
        assert result.retryable is False
        assert "req-x" not in store.applied
        assert len(store.events) == 0

    def test_sequence_of_unique_events_all_appended(self):
        store = InMemoryProjectionStore()
        for i in range(10):
            _apply(store, _cmd_append_event(f"req-{i}", "click"))
        assert len(store.events) == 10
        assert len(store.applied) == 10


# ---------------------------------------------------------------------------
# Mixed command idempotency
# ---------------------------------------------------------------------------


class TestMixedCommandIdempotency:
    def test_create_user_and_append_event_independent(self):
        store = InMemoryProjectionStore()
        _apply(store, _cmd_create_user("req-1", "alice"))
        _apply(store, _cmd_append_event("req-2", "user.created"))
        assert len(store.users) == 1
        assert len(store.events) == 1

    def test_replay_of_mixed_commands_is_idempotent(self):
        """Simulate a replay: apply all commands twice. State must be identical."""
        store = InMemoryProjectionStore()
        commands = [
            _cmd_create_user("req-1", "alice"),
            _cmd_append_event("req-2", "page.view"),
            _cmd_create_user("req-3", "bob"),
            _cmd_append_event("req-4", "click"),
        ]
        for cmd in commands:
            _apply(store, cmd)
        users_after_first = dict(store.users)
        events_after_first = list(store.events)

        # Replay — apply all again
        for cmd in commands:
            _apply(store, cmd)
        assert store.users == users_after_first
        assert store.events == events_after_first
        assert len(store.applied) == 4


# ---------------------------------------------------------------------------
# Unknown and malformed commands
# ---------------------------------------------------------------------------


class TestTerminalFailures:
    def test_unknown_command_returns_failure(self):
        store = InMemoryProjectionStore()
        cmd = Command("req-1", "drop_table", {}, Command.now_utc())
        result = _apply(store, cmd)
        assert result.success is False
        assert result.retryable is False

    def test_unknown_command_not_applied(self):
        store = InMemoryProjectionStore()
        cmd = Command("req-1", "drop_table", {}, Command.now_utc())
        _apply(store, cmd)
        assert "req-1" not in store.applied

    def test_unknown_command_applied_twice_still_not_applied(self):
        """Terminal failure twice — still not checkpointed, state unchanged."""
        store = InMemoryProjectionStore()
        cmd = Command("req-1", "drop_table", {}, Command.now_utc())
        r1 = _apply(store, cmd)
        r2 = _apply(store, cmd)
        assert r1.success is False
        assert r2.success is False
        assert "req-1" not in store.applied

    def test_retryable_false_on_all_terminal_failures(self):
        """All terminal failure paths must explicitly set retryable=False."""
        store = InMemoryProjectionStore()
        cases = [
            Command("r1", "unknown_cmd", {}, Command.now_utc()),
            Command("r2", "create_user", {"data": {}}, Command.now_utc()),  # missing user_id
            Command("r3", "append_event", {"data": {}}, Command.now_utc()),  # missing event_type
        ]
        for cmd in cases:
            result = _apply(store, cmd)
            assert result.retryable is False, (
                f"Expected retryable=False for {cmd.command!r}, got retryable={result.retryable}"
            )
