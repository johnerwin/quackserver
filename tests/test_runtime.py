"""Tests for Runtime — the central write-path coordinator.

All async tests use asyncio.run() — no external async test framework needed.
"""

import asyncio
from pathlib import Path

import pytest

from quackserver.config import WRITE_QUEUE_WARNING_PCT
from quackserver.core.commands import Command
from quackserver.core.health import RuntimeState
from quackserver.core.log import AppendLogWriter
from quackserver.core.projection import ProjectionStore
from quackserver.core.runtime import Runtime
from quackserver.storage.interface import Result

from .conftest import MockProjection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cmd(request_id: str = "req-001", command: str = "append_event") -> Command:
    return Command(
        request_id=request_id,
        command=command,
        payload={"event_type": "test", "data": {}},
        timestamp=Command.now_utc(),
    )


def _runtime(tmp_path: Path, *, projection=None, queue_size: int = 100) -> Runtime:
    return Runtime(
        log_path=tmp_path / "log.jsonl",
        dedup_path=tmp_path / "dedup.db",
        checkpoint_path=tmp_path / "checkpoint.db",
        projection=projection or MockProjection(),
        queue_size=queue_size,
    )


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


class TestStateMachine:
    def test_initial_state_is_stopped(self, tmp_path):
        rt = _runtime(tmp_path)
        assert rt.state == RuntimeState.STOPPED

    def test_state_is_running_after_start(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                assert rt.state == RuntimeState.RUNNING
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_state_is_stopped_after_stop(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            await rt.stop()
            assert rt.state == RuntimeState.STOPPED

        asyncio.run(_run())

    def test_illegal_transition_raises(self, tmp_path):
        # Attempt a direct _transition() that is not in the legal graph.
        rt = _runtime(tmp_path)
        # STOPPED → RUNNING is not a legal transition.
        with pytest.raises(RuntimeError, match="Illegal runtime state transition"):
            rt._transition(RuntimeState.RUNNING)

    def test_transition_to_same_state_is_noop(self, tmp_path):
        rt = _runtime(tmp_path)
        since_before = rt._state_since
        rt._transition(RuntimeState.STOPPED)  # no-op
        assert rt._state_since == since_before

    def test_state_since_updates_on_transition(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            before = rt._state_since
            await rt.start()
            try:
                assert rt._state_since > before
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_failed_state_is_sticky_cannot_start_again(self, tmp_path):
        # Force into FAILED by making start() fail after STARTING transition.
        rt = _runtime(tmp_path)
        rt._transition(RuntimeState.STARTING)
        rt._transition(RuntimeState.FAILED)
        # FAILED → STARTING is not legal
        with pytest.raises(RuntimeError, match="Illegal runtime state transition"):
            rt._transition(RuntimeState.STARTING)

    def test_failed_can_transition_to_stopped(self, tmp_path):
        # FAILED → STOPPED is the cleanup path before a restart.
        rt = _runtime(tmp_path)
        rt._transition(RuntimeState.STARTING)
        rt._transition(RuntimeState.FAILED)
        rt._transition(RuntimeState.STOPPED)  # must not raise
        assert rt.state == RuntimeState.STOPPED

    def test_can_restart_after_stop(self, tmp_path):
        # STOPPED → STARTING → ... → STOPPED → STARTING is legal (second start).
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            await rt.stop()
            assert rt.state == RuntimeState.STOPPED
            await rt.start()
            assert rt.state == RuntimeState.RUNNING
            await rt.stop()

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Lifecycle guards
# ---------------------------------------------------------------------------


class TestLifecycleGuards:
    def test_enqueue_before_start_returns_503(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            result = await rt.enqueue(_cmd())
            assert result.success is False
            assert result.status_code == 503
            assert "STOPPED" in (result.error or "")

        asyncio.run(_run())

    def test_health_before_start_raises(self, tmp_path):
        rt = _runtime(tmp_path)
        with pytest.raises(RuntimeError, match="started"):
            rt.health_snapshot()

    def test_start_when_not_stopped_raises(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                with pytest.raises(RuntimeError, match="STOPPED"):
                    await rt.start()
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_stop_when_not_running_raises(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            with pytest.raises(RuntimeError, match="RUNNING or FAILED"):
                await rt.stop()

        asyncio.run(_run())

    def test_start_and_stop_completes(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            await rt.stop()

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


class TestEnqueue:
    def test_valid_append_event_returns_200(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                result = await rt.enqueue(_cmd())
                assert result.success is True
                assert result.status_code == 200
                assert result.data == {"status": "accepted"}
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_valid_create_user_returns_200(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                cmd = Command(
                    request_id="req-user-001",
                    command="create_user",
                    payload={"user_id": "alice", "data": {}},
                    timestamp=Command.now_utc(),
                )
                result = await rt.enqueue(cmd)
                assert result.success is True
                assert result.status_code == 200
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_blank_request_id_returns_400(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                cmd = Command(
                    request_id="   ",
                    command="append_event",
                    payload={},
                    timestamp=Command.now_utc(),
                )
                result = await rt.enqueue(cmd)
                assert result.success is False
                assert result.status_code == 400
                assert result.error is not None
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_blank_command_returns_400(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                cmd = Command(
                    request_id="req-001",
                    command="",
                    payload={},
                    timestamp=Command.now_utc(),
                )
                result = await rt.enqueue(cmd)
                assert result.success is False
                assert result.status_code == 400
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_dedup_hit_returns_cached_result(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                cmd = _cmd("req-dedup-001")
                result1 = await rt.enqueue(cmd)
                assert result1.success is True

                # Drain the queue so the worker processes cmd and populates dedup
                await rt._queue.join()

                result2 = await rt.enqueue(cmd)
                assert result2.success is True
                assert result2.request_id == "req-dedup-001"
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_log_file_contains_request_id(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                await rt.enqueue(_cmd("req-logcheck"))
            finally:
                await rt.stop()

        asyncio.run(_run())
        log_content = (tmp_path / "log.jsonl").read_text()
        assert "req-logcheck" in log_content

    def test_invalid_command_not_written_to_dedup(self, tmp_path):
        """400 responses must not be cached in the dedup store."""
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                cmd = Command(
                    request_id="   ",
                    command="append_event",
                    payload={},
                    timestamp=Command.now_utc(),
                )
                result = await rt.enqueue(cmd)
                assert result.status_code == 400
                result2 = await rt.enqueue(cmd)
                assert result2.status_code == 400
            finally:
                await rt.stop()

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Crash recovery
# ---------------------------------------------------------------------------


class TestRecover:
    def test_empty_log_returns_zero(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                count = await rt.recover()
                assert count == 0
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_replays_unapplied_entries_on_start(self, tmp_path):
        """Write two entries to the log directly; fresh runtime must replay them."""
        log_path = tmp_path / "log.jsonl"
        writer = AppendLogWriter(log_path)
        writer.append(_cmd("req-replay-1"))
        writer.append(_cmd("req-replay-2"))
        writer.close()

        projection = MockProjection()
        rt = Runtime(
            log_path=log_path,
            dedup_path=tmp_path / "dedup.db",
            checkpoint_path=tmp_path / "checkpoint.db",
            projection=projection,
        )

        async def _run():
            await rt.start()
            await rt._queue.join()
            await rt.stop()

        asyncio.run(_run())
        applied_ids = {c[1] for c in projection.calls}
        assert "req-replay-1" in applied_ids
        assert "req-replay-2" in applied_ids

    def test_already_applied_entries_are_not_replayed(self, tmp_path):
        """Entries checkpointed in a previous run must not be re-applied."""
        log_path = tmp_path / "log.jsonl"
        dedup_path = tmp_path / "dedup.db"
        checkpoint_path = tmp_path / "checkpoint.db"

        projection1 = MockProjection()
        rt1 = Runtime(
            log_path=log_path,
            dedup_path=dedup_path,
            checkpoint_path=checkpoint_path,
            projection=projection1,
        )

        async def _run1():
            await rt1.start()
            await rt1.enqueue(_cmd("req-persisted"))
            await rt1._queue.join()
            await rt1.stop()

        asyncio.run(_run1())
        assert any(c[1] == "req-persisted" for c in projection1.calls)

        projection2 = MockProjection()
        rt2 = Runtime(
            log_path=log_path,
            dedup_path=dedup_path,
            checkpoint_path=checkpoint_path,
            projection=projection2,
        )

        async def _run2():
            await rt2.start()
            await rt2._queue.join()
            await rt2.stop()

        asyncio.run(_run2())
        assert not any(c[1] == "req-persisted" for c in projection2.calls)


# ---------------------------------------------------------------------------
# Health snapshot
# ---------------------------------------------------------------------------


class TestHealthSnapshot:
    def test_worker_running_after_start(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                snap = rt.health_snapshot()
                assert snap.worker_running is True
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_queue_depth_zero_at_rest(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                snap = rt.health_snapshot()
                assert snap.queue_depth == 0
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_queue_max_matches_construction_arg(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path, queue_size=42)
            await rt.start()
            try:
                snap = rt.health_snapshot()
                assert snap.queue_max == 42
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_is_healthy_true_when_running(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                assert rt.health_snapshot().is_healthy is True
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_replay_pending_zero_after_start(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                snap = rt.health_snapshot()
                assert snap.replay_pending == 0
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_state_is_running_in_snapshot(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                snap = rt.health_snapshot()
                assert snap.state == RuntimeState.RUNNING
            finally:
                await rt.stop()

        asyncio.run(_run())

    def test_state_since_is_iso_string(self, tmp_path):
        async def _run():
            rt = _runtime(tmp_path)
            await rt.start()
            try:
                snap = rt.health_snapshot()
                assert isinstance(snap.state_since, str)
                assert "T" in snap.state_since  # ISO 8601 format
            finally:
                await rt.stop()

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Backpressure — queue pressure contracts (Issue #1)
# ---------------------------------------------------------------------------


def _slow_projection(gate: asyncio.Event) -> ProjectionStore:
    """ProjectionStore that blocks every apply() until gate is set.

    Used to hold the write worker at a known point so tests can observe
    queue depth without racing against the drain loop.
    """

    class _Slow(ProjectionStore):
        async def apply(self, command: Command) -> Result:
            await gate.wait()
            return Result(
                success=True, status_code=200, request_id=command.request_id
            )

        async def health(self) -> Result:
            return Result(success=True, status_code=200)

        async def close(self) -> None:
            pass

    return _Slow()


class TestBackpressure:
    def test_queue_full_returns_503(self, tmp_path):
        """Overflowing the write queue returns 503 and error='queue full'."""
        async def _run():
            gate = asyncio.Event()
            # Worker takes item 0, holds it. Items 1-2 fill the queue (maxsize=2).
            # Item 3 overflows -> 503.
            rt = _runtime(tmp_path, projection=_slow_projection(gate), queue_size=2)
            await rt.start()
            try:
                for i in range(3):
                    r = await rt.enqueue(_cmd(f"bp-fill-{i}"))
                    assert r.success is True
                overflow = await rt.enqueue(_cmd("bp-overflow"))
                assert overflow.success is False
                assert overflow.status_code == 503
                assert "queue full" in (overflow.error or "").lower()
            finally:
                gate.set()
                await rt.stop()

        asyncio.run(_run())

    def test_queue_depth_nonzero_under_pressure(self, tmp_path):
        """health_snapshot().queue_depth is non-zero when the worker is blocked."""
        async def _run():
            gate = asyncio.Event()
            rt = _runtime(tmp_path, projection=_slow_projection(gate), queue_size=10)
            await rt.start()
            try:
                for i in range(5):
                    await rt.enqueue(_cmd(f"bp-d{i}"))
                # Worker holds 1 item; at least 1 more is in the queue.
                snap = rt.health_snapshot()
                assert snap.queue_depth >= 1
                assert snap.queue_pct > 0.0
            finally:
                gate.set()
                await rt.stop()

        asyncio.run(_run())

    def test_queue_warning_fires_at_threshold(self, tmp_path):
        """is_queue_warning is True when occupancy >= WRITE_QUEUE_WARNING_PCT."""
        async def _run():
            gate = asyncio.Event()
            queue_size = 10
            rt = _runtime(
                tmp_path, projection=_slow_projection(gate), queue_size=queue_size
            )
            await rt.start()
            try:
                # 9 enqueues: worker takes 1, 8 remain -> 80% of 10.
                for i in range(9):
                    await rt.enqueue(_cmd(f"bp-w{i}"))
                snap = rt.health_snapshot()
                assert snap.is_queue_warning is True
                assert snap.queue_pct >= WRITE_QUEUE_WARNING_PCT
            finally:
                gate.set()
                await rt.stop()

        asyncio.run(_run())

    def test_queue_full_entry_written_to_log(self, tmp_path):
        """A 503 from queue-full still persists the entry to the append log."""
        log_path = tmp_path / "log.jsonl"

        async def _run():
            gate = asyncio.Event()
            rt = Runtime(
                log_path=log_path,
                dedup_path=tmp_path / "dedup.db",
                checkpoint_path=tmp_path / "checkpoint.db",
                projection=_slow_projection(gate),
                queue_size=2,
            )
            await rt.start()
            try:
                for i in range(3):
                    await rt.enqueue(_cmd(f"bp-{i}"))
                r = await rt.enqueue(_cmd("bp-log-durability"))
                assert r.status_code == 503
            finally:
                gate.set()
                await rt.stop()

        asyncio.run(_run())
        assert "bp-log-durability" in log_path.read_text()
