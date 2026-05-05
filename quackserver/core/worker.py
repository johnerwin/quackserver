"""
WriteWorker — the sole consumer of the asyncio write queue.

Owns the single-writer guarantee: exactly one running instance of this
class may call ProjectionStore.apply() at any time.

Durability contract (spec §7.1) — THIS IS THE EXPLICIT DECISION:
  The HTTP handler returns 200 AFTER the entry is written to the append
  log (AppendLogWriter.append() succeeded). DuckDB projection is eventual.
  The caller does not wait for the worker to apply the entry.

  Consequence: a client that receives 200 knows the write is durable on
  disk. It does NOT know whether the DuckDB read projection reflects the
  write yet. Reads immediately after a write may not see the new data.
  This is intentional. Operational simplicity and crash safety are worth
  the eventual-consistency read window.

Crash recovery:
  On process restart, the startup sequence calls ReplayScanner.scan_pending()
  and feeds each unapplied entry back through this worker before the HTTP
  server begins accepting requests. No writes are lost.

Retry semantics:
  The projection store communicates retryability via Result.retryable.
  Transient failures (retryable=True: timeout, DuckDB lock) are retried up
  to MAX_WRITE_RETRIES times. Terminal failures (retryable=False: unknown
  command, malformed payload) are logged and dropped immediately — retrying
  cannot help. After MAX_WRITE_RETRIES the entry is logged as failed and
  dropped from this run; the append log still holds it for the next restart.

  Transparent retry-with-backoff is on the spec's explicit forbidden list
  (§12.1). Failure is logged loudly. Operators see it.
"""

import asyncio
import time

from quackserver.config import MAX_WRITE_RETRIES, MAX_WRITE_TIME_MS
from quackserver.core.checkpoint import ReplayCheckpoint
from quackserver.core.commands import Command
from quackserver.core.dedup import DedupStore
from quackserver.core.projection import ProjectionStore
from quackserver.core.structured_log import StructuredLogger, get_logger
from quackserver.storage.interface import Result

log: StructuredLogger = get_logger(__name__)


class WriteWorker:
    """Single asyncio coroutine that owns the write connection.

    Lifecycle:
        task = asyncio.create_task(worker.run())
        ...
        worker.stop()
        task.cancel()
        await task

    The worker processes one entry at a time. It does not parallelise
    writes — that would violate the single-writer guarantee.
    """

    def __init__(
        self,
        queue: asyncio.Queue,
        projection: ProjectionStore,
        checkpoint: ReplayCheckpoint,
        dedup: DedupStore,
    ) -> None:
        self._queue = queue
        self._projection = projection
        self._checkpoint = checkpoint
        self._dedup = dedup
        self._running = False

    async def run(self) -> None:
        """Main loop. Runs until cancelled."""
        self._running = True
        log.info("write_worker_started")
        while self._running:
            try:
                entry: Command = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            try:
                await self._process(entry)
            finally:
                self._queue.task_done()
        log.info("write_worker_stopped")

    def stop(self) -> None:
        """Signal the worker to exit after processing the current entry."""
        self._running = False

    async def _process(self, entry: Command) -> None:
        """Apply one log entry to the projection store, with retry and checkpoint."""
        if self._checkpoint.is_applied(entry.request_id):
            log.info(
                "write_skipped_already_applied",
                request_id=entry.request_id,
                command=entry.command,
            )
            return

        last_exc: Exception | None = None
        for attempt in range(1, MAX_WRITE_RETRIES + 1):
            start = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    self._projection.apply(entry),
                    timeout=MAX_WRITE_TIME_MS / 1000,
                )
                elapsed_ms = int((time.monotonic() - start) * 1000)

                if result.success:
                    self._checkpoint.mark_applied(entry.request_id)
                    self._dedup.put(entry.request_id, result)
                    log.info(
                        "write_applied",
                        request_id=entry.request_id,
                        command=entry.command,
                        duration_ms=elapsed_ms,
                        attempt=attempt,
                    )
                    return

                if not result.retryable:
                    # Terminal failure — projection store says retrying cannot help.
                    log.error(
                        "write_terminal_failure",
                        request_id=entry.request_id,
                        command=entry.command,
                        status_code=result.status_code,
                        error=result.error,
                    )
                    return

                # Retryable failure — projection store reported a transient error.
                log.warning(
                    "write_retryable_failure",
                    request_id=entry.request_id,
                    command=entry.command,
                    status_code=result.status_code,
                    error=result.error,
                    attempt=attempt,
                    max_retries=MAX_WRITE_RETRIES,
                )
                last_exc = RuntimeError(result.error or "retryable failure")

            except asyncio.TimeoutError as exc:
                elapsed_ms = int((time.monotonic() - start) * 1000)
                last_exc = exc
                log.warning(
                    "write_timeout",
                    request_id=entry.request_id,
                    command=entry.command,
                    elapsed_ms=elapsed_ms,
                    limit_ms=MAX_WRITE_TIME_MS,
                    attempt=attempt,
                    max_retries=MAX_WRITE_RETRIES,
                )

            except Exception as exc:
                elapsed_ms = int((time.monotonic() - start) * 1000)
                last_exc = exc
                log.warning(
                    "write_error",
                    request_id=entry.request_id,
                    command=entry.command,
                    error=str(exc),
                    elapsed_ms=elapsed_ms,
                    attempt=attempt,
                    max_retries=MAX_WRITE_RETRIES,
                )

        # All retries exhausted. Entry remains in log; will replay on restart.
        log.error(
            "write_failed_all_retries",
            request_id=entry.request_id,
            command=entry.command,
            max_retries=MAX_WRITE_RETRIES,
            error=str(last_exc),
        )
