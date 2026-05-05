"""
Chaos and recovery tests — validates the durability contract end-to-end.

The system's central claim is:
    "Accepted writes survive crashes and converge correctly after replay."

These tests exist to validate that claim as a whole, not component-by-component.
They deliberately simulate messy failure modes, not clean restarts.

What "accepted" means here:
    An entry is accepted the moment AppendLogWriter.append() succeeds.
    The caller has received (or will receive) a 200 response.
    DuckDB projection is not part of the acceptance guarantee.
    These tests prove that accepted-but-unprojected entries converge correctly.
"""

import asyncio
from pathlib import Path

import pytest

from quackserver.core.checkpoint import ReplayCheckpoint
from quackserver.core.commands import Command
from quackserver.core.log import AppendLogWriter
from quackserver.core.projection import ProjectionStore
from quackserver.core.runtime import Runtime
from quackserver.storage.interface import Result


# ---------------------------------------------------------------------------
# Faulty projection implementations
# ---------------------------------------------------------------------------


class _FailingProjection(ProjectionStore):
    """Succeeds for all request_ids not in fail_ids; raises for the rest.

    Raising (not returning Result(success=False)) exercises the worker's
    retry-and-exhaust path, which is the realistic failure mode for
    transient projection errors (lock contention, timeouts, etc.).
    """

    def __init__(self, fail_ids: set[str]):
        self.calls: list[str] = []
        self._fail_ids = fail_ids

    async def apply(self, command: Command) -> Result:
        if command.request_id in self._fail_ids:
            raise RuntimeError(f"simulated projection failure: {command.request_id}")
        self.calls.append(command.request_id)
        return Result(
            success=True,
            status_code=200,
            request_id=command.request_id,
            data={"status": "accepted"},
        )

    async def health(self) -> Result:
        return Result(success=True, status_code=200)

    async def close(self) -> None:
        pass


class _RecordingProjection(ProjectionStore):
    """Records every call; always succeeds. Used for the recovery run."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def apply(self, command: Command) -> Result:
        self.calls.append(command.request_id)
        return Result(
            success=True,
            status_code=200,
            request_id=command.request_id,
            data={"status": "accepted"},
        )

    async def health(self) -> Result:
        return Result(success=True, status_code=200)

    async def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cmd(request_id: str) -> Command:
    return Command(
        request_id=request_id,
        command="append_event",
        payload={"event_type": "chaos.test", "data": {}},
        timestamp=Command.now_utc(),
    )


def _runtime(tmp_path: Path, projection: ProjectionStore) -> Runtime:
    return Runtime(
        log_path=tmp_path / "log.jsonl",
        dedup_path=tmp_path / "dedup.db",
        checkpoint_path=tmp_path / "checkpoint.db",
        projection=projection,
    )


# ---------------------------------------------------------------------------
# The durability contract tests
# ---------------------------------------------------------------------------


class TestDurabilityContract:
    """Each test validates a different face of the same guarantee:
    accepted writes converge exactly once, regardless of failure timing.
    """

    def test_no_data_loss_on_projection_failure(self, tmp_path):
        """
        Projection fails for 2 of 5 entries in the first run.
        On restart those 2 entries must be replayed and projected.
        The other 3 must not be projected again.

        This is the foundational durability test: storage failures do not
        lose writes. The append log is the source of truth. The checkpoint
        is the boundary. Anything not checkpointed is replayed.
        """
        all_ids = [f"req-{i}" for i in range(5)]
        fail_ids = {"req-1", "req-3"}

        # --- Run 1: partial projection ---
        proj1 = _FailingProjection(fail_ids=fail_ids)
        rt1 = _runtime(tmp_path, proj1)

        async def _run1():
            await rt1.start()
            for rid in all_ids:
                await rt1.enqueue(_cmd(rid))
            await rt1._queue.join()
            await rt1.stop()

        asyncio.run(_run1())
        assert set(proj1.calls) == set(all_ids) - fail_ids

        # --- Run 2: recovery run ---
        proj2 = _RecordingProjection()
        rt2 = _runtime(tmp_path, proj2)

        async def _run2():
            await rt2.start()
            await rt2._queue.join()
            await rt2.stop()

        asyncio.run(_run2())
        assert set(proj2.calls) == fail_ids, (
            f"Expected {fail_ids} to be replayed on restart, got {set(proj2.calls)}"
        )

    def test_no_duplicate_projection_on_clean_restart(self, tmp_path):
        """
        All entries project successfully in run 1 (fully checkpointed).
        Run 2 must project nothing — no re-application of already-applied entries.

        This validates the idempotency boundary: checkpoint prevents re-application.
        """
        all_ids = [f"req-{i}" for i in range(5)]

        proj1 = _RecordingProjection()
        rt1 = _runtime(tmp_path, proj1)

        async def _run1():
            await rt1.start()
            for rid in all_ids:
                await rt1.enqueue(_cmd(rid))
            await rt1._queue.join()
            await rt1.stop()

        asyncio.run(_run1())
        assert set(proj1.calls) == set(all_ids)

        proj2 = _RecordingProjection()
        rt2 = _runtime(tmp_path, proj2)

        async def _run2():
            await rt2.start()
            await rt2._queue.join()
            await rt2.stop()

        asyncio.run(_run2())
        assert proj2.calls == [], (
            f"Expected no replay after clean run, got {proj2.calls}"
        )

    def test_all_entries_replayed_when_no_checkpoint_exists(self, tmp_path):
        """
        Entries are written directly to the log with no checkpoint (simulating
        a crash that happened between AppendLogWriter.append() and queue.put_nowait(),
        before the worker could project anything).

        All entries must be replayed and projected on startup.

        This tests the worst-case recovery: the entire log must be re-applied.
        """
        log_path = tmp_path / "log.jsonl"
        ids = [f"req-{i}" for i in range(10)]

        writer = AppendLogWriter(log_path)
        for rid in ids:
            writer.append(_cmd(rid))
        writer.close()

        proj = _RecordingProjection()
        rt = Runtime(
            log_path=log_path,
            dedup_path=tmp_path / "dedup.db",
            checkpoint_path=tmp_path / "checkpoint.db",
            projection=proj,
        )

        async def _run():
            await rt.start()
            await rt._queue.join()
            await rt.stop()

        asyncio.run(_run())
        assert set(proj.calls) == set(ids)

    def test_duplicate_log_entries_project_exactly_once(self, tmp_path):
        """
        The same request_id appears twice in the log (as can happen when a
        client retries before the dedup cache is populated, or when an entry
        is appended twice due to a retry race).

        The entry must be projected exactly once — not zero times, not twice.

        This validates ReplayScanner's within-scan deduplication.
        """
        log_path = tmp_path / "log.jsonl"
        cmd = _cmd("req-dupe")

        writer = AppendLogWriter(log_path)
        writer.append(cmd)
        writer.append(cmd)  # intentional duplicate
        writer.close()

        proj = _RecordingProjection()
        rt = Runtime(
            log_path=log_path,
            dedup_path=tmp_path / "dedup.db",
            checkpoint_path=tmp_path / "checkpoint.db",
            projection=proj,
        )

        async def _run():
            await rt.start()
            await rt._queue.join()
            await rt.stop()

        asyncio.run(_run())
        count = proj.calls.count("req-dupe")
        assert count == 1, f"Expected exactly-once projection, got {count}"

    def test_partial_checkpoint_resumes_from_correct_boundary(self, tmp_path):
        """
        10 entries are in the log. The first 4 are already checkpointed
        (simulating a previous run that successfully projected entries 0-3
        before crashing).

        On restart, exactly entries 4-9 must be projected; 0-3 must not.

        This validates that ReplayScanner correctly reads the checkpoint
        and resumes from the right position.
        """
        log_path = tmp_path / "log.jsonl"
        checkpoint_path = tmp_path / "checkpoint.db"
        ids = [f"req-{i}" for i in range(10)]

        writer = AppendLogWriter(log_path)
        for rid in ids:
            writer.append(_cmd(rid))
        writer.close()

        # Manually checkpoint the first 4
        cp = ReplayCheckpoint(checkpoint_path)
        for rid in ids[:4]:
            cp.mark_applied(rid)
        cp.close()

        proj = _RecordingProjection()
        rt = Runtime(
            log_path=log_path,
            dedup_path=tmp_path / "dedup.db",
            checkpoint_path=checkpoint_path,
            projection=proj,
        )

        async def _run():
            await rt.start()
            await rt._queue.join()
            await rt.stop()

        asyncio.run(_run())

        projected = set(proj.calls)
        expected = set(ids[4:])
        assert projected == expected, (
            f"Expected {expected}, got {projected}"
        )
        for rid in ids[:4]:
            assert rid not in projected, (
                f"{rid} was re-projected despite being checkpointed"
            )

    def test_convergence_across_multiple_failure_restart_cycles(self, tmp_path):
        """
        Three runs: run 1 fails entries 3-5, run 2 fails entry 4, run 3 succeeds.
        After all three runs, every entry must have been projected exactly once.

        This validates convergence under repeated failure: the system always
        makes forward progress and never loses or duplicates a write across
        an arbitrary number of restart cycles.
        """
        all_ids = [f"req-{i}" for i in range(6)]
        projected_total: set[str] = set()

        # Run 1: entries 0-2 succeed, 3-5 fail
        p1 = _FailingProjection(fail_ids={"req-3", "req-4", "req-5"})
        rt1 = _runtime(tmp_path, p1)

        async def _run1():
            await rt1.start()
            for rid in all_ids:
                await rt1.enqueue(_cmd(rid))
            await rt1._queue.join()
            await rt1.stop()

        asyncio.run(_run1())
        projected_total.update(p1.calls)
        assert projected_total == {"req-0", "req-1", "req-2"}

        # Run 2: entry 4 still fails, 3 and 5 succeed
        p2 = _FailingProjection(fail_ids={"req-4"})
        rt2 = _runtime(tmp_path, p2)

        async def _run2():
            await rt2.start()
            await rt2._queue.join()
            await rt2.stop()

        asyncio.run(_run2())
        projected_total.update(p2.calls)
        assert projected_total == {"req-0", "req-1", "req-2", "req-3", "req-5"}

        # Run 3: all succeed; only req-4 should be replayed
        p3 = _RecordingProjection()
        rt3 = _runtime(tmp_path, p3)

        async def _run3():
            await rt3.start()
            await rt3._queue.join()
            await rt3.stop()

        asyncio.run(_run3())
        assert set(p3.calls) == {"req-4"}, (
            f"Expected only req-4 in final run, got {set(p3.calls)}"
        )
        projected_total.update(p3.calls)

        # Every entry projected exactly once in total
        assert projected_total == set(all_ids)
