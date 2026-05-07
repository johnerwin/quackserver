"""
Runtime — the central coordinator for the quackserver write path.

This object owns all stateful components and their lifecycle:
  - JSONL append log (writer + reader)
  - Dedup store
  - Replay checkpoint
  - asyncio write queue
  - Write worker task

Why this exists:
  Without it, FastAPI lifecycle hooks become the orchestration layer — ad
  hoc globals, startup spaghetti, and queue ownership confusion. This class
  is the answer to "where does the queue live?" and "who starts the worker?"

The runtime is designed to be started and exercised without HTTP. Tests,
CLI tools, and the startup replay path all use the same enqueue() entrypoint
as HTTP route handlers. There is no separate "internal" write path.

Startup sequence (call order matters):
  1. runtime = Runtime(...)
  2. await runtime.start()          ← recovery + worker start, blocks until done
  3. # HTTP server begins accepting requests
  4. await runtime.stop()           ← graceful drain on shutdown

The HTTP server must not accept write traffic before start() returns.

State machine:
  STOPPED → STARTING → RECOVERING → RUNNING → STOPPING → STOPPED
  Any state → FAILED (sticky; requires stop() then start() to recover)
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from quackserver.config import MAX_CONCURRENT_READS, MAX_WRITE_QUEUE_SIZE, WRITE_QUEUE_WARNING_PCT
from quackserver.core.checkpoint import ReplayCheckpoint
from quackserver.core.commands import Command
from quackserver.core.dedup import DedupStore
from quackserver.core.health import HealthSnapshot, RuntimeState
from quackserver.core.log import AppendLogReader, AppendLogWriter
from quackserver.core.projection import ProjectionStore
from quackserver.core.read_store import ReadStore
from quackserver.core.replay import ReplayScanner
from quackserver.core.structured_log import StructuredLogger, get_logger
from quackserver.core.worker import WriteWorker
from quackserver.storage.interface import Result

_log: StructuredLogger = get_logger(__name__)

# Legal state transitions. Any transition not listed here is a programming error.
_LEGAL_TRANSITIONS: dict[RuntimeState, frozenset[RuntimeState]] = {
    RuntimeState.STOPPED:    frozenset({RuntimeState.STARTING}),
    RuntimeState.STARTING:   frozenset({RuntimeState.RECOVERING, RuntimeState.FAILED}),
    RuntimeState.RECOVERING: frozenset({RuntimeState.RUNNING,    RuntimeState.FAILED}),
    RuntimeState.RUNNING:    frozenset({RuntimeState.STOPPING,   RuntimeState.FAILED}),
    RuntimeState.STOPPING:   frozenset({RuntimeState.STOPPED,    RuntimeState.FAILED}),
    RuntimeState.FAILED:     frozenset({RuntimeState.STOPPED}),
}


class Runtime:
    """Owns and coordinates all write-path components.

    All keyword-only arguments to keep construction explicit and prevent
    positional argument confusion when calling from tests or the HTTP layer.
    """

    def __init__(
        self,
        *,
        log_path: Path,
        dedup_path: Path,
        checkpoint_path: Path,
        projection: ProjectionStore,
        read_store: ReadStore | None = None,
        queue_size: int = MAX_WRITE_QUEUE_SIZE,
    ) -> None:
        self._log_path = log_path
        self._dedup_path = dedup_path
        self._checkpoint_path = checkpoint_path
        self._projection = projection
        self._read_store = read_store
        self._queue_max = queue_size
        self._read_semaphore = asyncio.Semaphore(MAX_CONCURRENT_READS)

        # Initialised in start(). None before start() or after stop().
        self._log_writer: AppendLogWriter | None = None
        self._reader: AppendLogReader | None = None
        self._dedup: DedupStore | None = None
        self._checkpoint: ReplayCheckpoint | None = None
        self._queue: asyncio.Queue[Command] | None = None
        self._worker: WriteWorker | None = None
        self._worker_task: asyncio.Task | None = None

        self._state = RuntimeState.STOPPED
        self._state_since: datetime = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    @property
    def state(self) -> RuntimeState:
        return self._state

    def _transition(self, new_state: RuntimeState) -> None:
        """Advance to new_state, enforcing the legal transition graph.

        Every state change goes through here. This is the single place for:
          - transition validation (illegal transitions raise immediately)
          - timestamp recording
          - structured logging

        Idempotent for same-state transitions (no-op, no log).
        """
        if self._state == new_state:
            return
        allowed = _LEGAL_TRANSITIONS.get(self._state, frozenset())
        if new_state not in allowed:
            raise RuntimeError(
                f"Illegal runtime state transition: {self._state.name} → {new_state.name}"
            )
        old_state = self._state
        self._state = new_state
        self._state_since = datetime.now(timezone.utc)
        _log.info(
            "runtime_state_transition",
            from_state=old_state.name,
            to_state=new_state.name,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialise components, replay the log, start the write worker.

        Blocks until crash recovery is complete. The HTTP server must not
        accept write traffic before this returns.

        Transitions: STOPPED → STARTING → RECOVERING → RUNNING
        On any failure: → FAILED (sticky; call stop() to clean up)
        """
        if self._state != RuntimeState.STOPPED:
            raise RuntimeError(
                f"Runtime.start() requires STOPPED state (current: {self._state.name})"
            )

        self._transition(RuntimeState.STARTING)

        try:
            self._log_writer = AppendLogWriter(self._log_path)
            self._reader = AppendLogReader(self._log_path)
            self._dedup = DedupStore(self._dedup_path)
            self._checkpoint = ReplayCheckpoint(self._checkpoint_path)
            self._queue = asyncio.Queue(maxsize=self._queue_max)
            self._worker = WriteWorker(
                queue=self._queue,
                projection=self._projection,
                checkpoint=self._checkpoint,
                dedup=self._dedup,
            )
            self._worker_task = asyncio.create_task(self._worker.run())
            await self._projection.initialize()

            _log.info(
                "runtime_started",
                log_path=str(self._log_path),
                queue_max=self._queue_max,
            )

            self._transition(RuntimeState.RECOVERING)
            replayed = await self.recover()
            if replayed > 0:
                _log.critical("crash_recovery_completed", replayed_count=replayed)

            self._transition(RuntimeState.RUNNING)

        except Exception as exc:
            self._transition(RuntimeState.FAILED)
            _log.error("runtime_start_failed", error=str(exc))
            raise

    async def stop(self) -> None:
        """Drain the queue, stop the worker, close all resources.

        Valid from RUNNING or FAILED. FAILED → STOPPED cleans up resources
        so the runtime can be restarted via start().
        """
        if self._state not in {RuntimeState.RUNNING, RuntimeState.FAILED}:
            raise RuntimeError(
                f"Runtime.stop() requires RUNNING or FAILED state (current: {self._state.name})"
            )

        if self._state == RuntimeState.RUNNING:
            self._transition(RuntimeState.STOPPING)

        try:
            if self._queue is not None:
                await self._queue.join()

            if self._worker is not None:
                self._worker.stop()

            if self._worker_task is not None:
                # Cancel to unblock the idle worker immediately rather than
                # waiting for its queue.get() timeout. Queue is already
                # drained at this point so cancellation during idle wait is safe.
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
        finally:
            if self._log_writer is not None:
                self._log_writer.close()
            if self._dedup is not None:
                self._dedup.close()
            if self._checkpoint is not None:
                self._checkpoint.close()

            self._transition(RuntimeState.STOPPED)
            _log.info("runtime_stopped")

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def enqueue(self, cmd: Command) -> Result:
        """Full write path: validate → dedup → log → queue → return accepted.

        This is the single entrypoint for all writes: HTTP, replay, CLI.
        Everything goes through here.

        Only callable in RUNNING state. Attempting to enqueue during startup,
        recovery, shutdown, or failure returns an error rather than silently
        queuing into an uncertain state.

        Return value semantics:
          200  — entry is durably on disk; DuckDB projection is eventual.
          200  — duplicate request_id within TTL; cached result returned.
          400  — structural validation failed (empty request_id, etc.).
          503  — write queue is at capacity; retry after back-off.
          503  — runtime is not in RUNNING state.
        """
        if self._state != RuntimeState.RUNNING:
            return Result(
                success=False,
                status_code=503,
                error=f"runtime not accepting writes (state: {self._state.name})",
            )
        assert self._dedup is not None
        assert self._log_writer is not None
        assert self._queue is not None

        # 1. Structural validation.
        errors = cmd.validate()
        if errors:
            _log.warning(
                "write_rejected_invalid",
                request_id=cmd.request_id,
                command=cmd.command,
                errors=errors,
            )
            return Result(
                success=False,
                status_code=400,
                error="; ".join(errors),
                request_id=cmd.request_id,
            )

        # 2. Dedup check — before log write (spec §8.2).
        cached = self._dedup.get(cmd.request_id)
        if cached is not None:
            _log.info(
                "dedup_hit",
                request_id=cmd.request_id,
                command=cmd.command,
            )
            return cached

        # 3. Queue warning — emit before the log write so operators see load
        #    signal even if the write is ultimately rejected.
        depth = self._queue.qsize()
        if depth / self._queue_max * 100 >= WRITE_QUEUE_WARNING_PCT:
            _log.warning(
                "write_queue_warning",
                queue_depth=depth,
                queue_max=self._queue_max,
                pct=round(depth / self._queue_max * 100, 1),
            )

        # 4. Append to log — durability boundary.
        #    After this returns without exception, 200 is safe to return.
        #    Uses run_in_executor because os.fsync() is blocking I/O.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._log_writer.append, cmd)

        # 5. Enqueue. The queue is bounded; put_nowait raises QueueFull if
        #    it filled up between the warning check and now (rare race).
        #    The entry is already durable in the log — replay handles it.
        try:
            self._queue.put_nowait(cmd)
        except asyncio.QueueFull:
            _log.warning(
                "write_queue_full",
                request_id=cmd.request_id,
                queue_depth=self._queue.qsize(),
                queue_max=self._queue_max,
            )
            return Result(
                success=False,
                status_code=503,
                error="write queue full",
                request_id=cmd.request_id,
            )

        _log.info(
            "write_accepted",
            request_id=cmd.request_id,
            command=cmd.command,
            queue_depth=self._queue.qsize(),
        )
        return Result(
            success=True,
            status_code=200,
            data={"status": "accepted"},
            request_id=cmd.request_id,
        )

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    async def recover(self) -> int:
        """Replay log entries not yet projected into DuckDB.

        Streams entries into the queue and waits for the worker to drain
        them before returning. Called by start() — not by HTTP handlers.

        Returns the number of entries replayed.
        """
        if self._state == RuntimeState.STOPPED:
            raise RuntimeError("Runtime must be started before calling recover()")
        assert self._reader is not None
        assert self._checkpoint is not None
        assert self._queue is not None

        scanner = ReplayScanner(self._reader, self._checkpoint)
        count = 0

        for cmd in scanner.scan_pending():
            if count == 0:
                _log.critical(
                    "crash_recovery_started",
                    malformed_log_entries=self._reader.malformed_count,
                )
            await self._queue.put(cmd)
            count += 1

        if count > 0:
            await self._queue.join()

        if self._reader.malformed_count > 0:
            _log.error(
                "append_log_has_malformed_entries",
                malformed_count=self._reader.malformed_count,
            )

        return count

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    async def execute_read(self, query_fn, timeout_ms: int) -> Result:
        """Execute a read coroutine with state guard, semaphore, and timeout.

        query_fn: zero-argument async callable returning Result.
        timeout_ms: wall-clock limit in milliseconds; exceeded -> 504.

        Return value semantics:
          200  — query succeeded; data in result.data.
          503  — runtime not running, read store absent, or pool exhausted.
          504  — query exceeded timeout_ms.
        """
        if self._state != RuntimeState.RUNNING:
            return Result(
                success=False,
                status_code=503,
                error=f"runtime not accepting reads (state: {self._state.name})",
            )
        if self._read_store is None:
            return Result(
                success=False,
                status_code=503,
                error="read store not configured",
            )
        if self._read_semaphore.locked():
            return Result(
                success=False,
                status_code=503,
                error="read pool exhausted — too many concurrent reads",
            )
        async with self._read_semaphore:
            try:
                return await asyncio.wait_for(
                    query_fn(), timeout=timeout_ms / 1000
                )
            except asyncio.TimeoutError:
                return Result(
                    success=False,
                    status_code=504,
                    error=f"read query exceeded {timeout_ms}ms timeout",
                )

    def health_snapshot(self) -> HealthSnapshot:
        """Return a point-in-time view of runtime state.

        Synchronous and non-blocking. Safe to call from any context,
        including the /health fast path (spec §9.2).

        Available in any state except STOPPED (components not initialised).
        """
        if self._state == RuntimeState.STOPPED:
            raise RuntimeError(
                "Runtime must be started before calling health_snapshot()"
            )
        return HealthSnapshot(
            queue_depth=self._queue.qsize() if self._queue is not None else 0,
            queue_max=self._queue_max,
            worker_running=(
                self._worker_task is not None and not self._worker_task.done()
            ),
            replay_pending=0,  # Always 0 after start() completes
            malformed_log_entries=(
                self._reader.malformed_count if self._reader is not None else 0
            ),
            state=self._state,
            state_since=self._state_since.isoformat(),
        )
