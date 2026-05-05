"""Tests for WriteWorker.

All async tests use asyncio.run() — no external async test framework needed.
"""

import asyncio

from quackserver.core.checkpoint import ReplayCheckpoint
from quackserver.core.commands import Command
from quackserver.core.dedup import DedupStore
from quackserver.core.projection import ProjectionStore
from quackserver.core.worker import WriteWorker
from quackserver.storage.interface import Result


# ---------------------------------------------------------------------------
# Test double — minimal concrete ProjectionStore
# ---------------------------------------------------------------------------


class _MockProjection(ProjectionStore):
    """Records calls and returns configurable results.

    Unknown commands return terminal failure (retryable=False) so tests
    that exercise the non-checkpoint path work correctly.
    """

    _KNOWN = frozenset({"append_event", "create_user"})

    def __init__(self, result: Result | None = None) -> None:
        self._result = result or Result(success=True, status_code=200)
        self.calls: list[tuple[str, str]] = []  # (command_name, request_id)

    async def apply(self, command: Command) -> Result:
        if command.command not in self._KNOWN:
            return Result(
                success=False,
                status_code=400,
                error=f"unknown command: {command.command!r}",
                request_id=command.request_id,
                retryable=False,
            )
        self.calls.append((command.command, command.request_id))
        return Result(
            success=self._result.success,
            status_code=self._result.status_code,
            request_id=command.request_id,
            data={"status": "accepted"} if self._result.success else None,
        )

    async def health(self) -> Result:
        return Result(success=True, status_code=200)

    async def close(self) -> None:
        pass


def _make_cmd(request_id: str, command: str = "append_event") -> Command:
    payload = (
        {"event_type": "test.event", "data": {}}
        if command == "append_event"
        else {"user_id": "u1", "data": {}}
    )
    return Command(request_id, command, payload, Command.now_utc())


def _run_worker(queue: asyncio.Queue, projection, checkpoint, dedup) -> None:
    """Run the worker until the queue is drained, then stop it."""

    async def _inner():
        worker = WriteWorker(queue, projection, checkpoint, dedup)
        task = asyncio.create_task(worker.run())
        await queue.join()
        worker.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWriteWorker:
    def test_processes_append_event(self, tmp_path):
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        queue.put_nowait(_make_cmd("r1", "append_event"))
        _run_worker(queue, projection, cp, dedup)
        assert ("append_event", "r1") in projection.calls
        cp.close()
        dedup.close()

    def test_processes_create_user(self, tmp_path):
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        queue.put_nowait(_make_cmd("r1", "create_user"))
        _run_worker(queue, projection, cp, dedup)
        assert ("create_user", "r1") in projection.calls
        cp.close()
        dedup.close()

    def test_marks_checkpoint_after_success(self, tmp_path):
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        queue.put_nowait(_make_cmd("r1"))
        _run_worker(queue, projection, cp, dedup)
        assert cp.is_applied("r1") is True
        cp.close()
        dedup.close()

    def test_stores_result_in_dedup_after_success(self, tmp_path):
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        queue.put_nowait(_make_cmd("r1"))
        _run_worker(queue, projection, cp, dedup)
        assert dedup.get("r1") is not None
        cp.close()
        dedup.close()

    def test_skips_already_applied_entry(self, tmp_path):
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        cp.mark_applied("r1")
        queue.put_nowait(_make_cmd("r1"))
        _run_worker(queue, projection, cp, dedup)
        assert projection.calls == []
        cp.close()
        dedup.close()

    def test_processes_multiple_commands_in_order(self, tmp_path):
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        for i in range(5):
            queue.put_nowait(_make_cmd(f"r{i}"))
        _run_worker(queue, projection, cp, dedup)
        applied_ids = [call[1] for call in projection.calls]
        assert applied_ids == [f"r{i}" for i in range(5)]
        cp.close()
        dedup.close()

    def test_unknown_command_not_checkpointed(self, tmp_path):
        """Unknown commands must not be checkpointed — terminal failure."""
        queue: asyncio.Queue = asyncio.Queue()
        projection = _MockProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        bad = Command("r-bad", "drop_table", {}, Command.now_utc())
        queue.put_nowait(bad)
        _run_worker(queue, projection, cp, dedup)
        assert cp.is_applied("r-bad") is False
        cp.close()
        dedup.close()

    def test_timeout_does_not_mark_checkpoint(self, tmp_path):
        """An entry that always times out must not be marked applied."""

        class _SlowProjection(_MockProjection):
            async def apply(self, command: Command) -> Result:
                await asyncio.sleep(999)
                return Result(success=True, status_code=200)

        queue: asyncio.Queue = asyncio.Queue()
        projection = _SlowProjection()
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        dedup = DedupStore(tmp_path / "dedup.sqlite")
        queue.put_nowait(_make_cmd("r-slow"))
        _run_worker(queue, projection, cp, dedup)
        assert cp.is_applied("r-slow") is False
        cp.close()
        dedup.close()
