"""
ProjectionStore — the write boundary between the worker and derived state.

The worker calls apply(command) exactly once per log entry (after checkpoint
and dedup checks). The projection store owns all decisions about how a command
becomes persistent state: which table, which SQL, UPSERT vs. insert, etc.

The worker does not know about tables, SQL, or DuckDB. It knows only:
  - apply() succeeded → checkpoint it
  - apply() failed, retryable=True → retry (transient; lock, timeout)
  - apply() failed, retryable=False → log and drop (terminal; bad payload, unknown command)

Idempotency contract:
  Applying the same command twice must produce equivalent state. This is
  layered resilience, separate from the runtime's request_id dedup. Bugs
  happen; replays get interrupted. Projection must be independently defensive.

  For create_user: UPSERT semantics (same user_id → overwrite, same
    request_id → no-op via _applied set).
  For append_event: unique constraint by request_id (same request_id → no-op,
    different request_id → append new row even for same event content).
  For unknown/malformed: terminal failure (retryable=False). Do not retry.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from quackserver.core.commands import Command
from quackserver.storage.interface import Result


class ProjectionStore(ABC):
    """Minimal contract for applying write commands to derived state."""

    async def initialize(self) -> None:
        """Called once by the Runtime during startup, before any apply() calls.

        Implementations should open connections, create schemas, and do any
        other work that would otherwise add latency to the first write.
        Default is a no-op (suitable for in-memory and test implementations).
        """

    @abstractmethod
    async def apply(self, command: Command) -> Result:
        """Apply one command to the projection.

        Must be idempotent for a given command.request_id.
        Returns retryable=True only for transient failures (lock, timeout).
        Returns retryable=False for terminal failures (bad payload, unknown command).
        """
        ...

    @abstractmethod
    async def health(self) -> Result:
        """Return projection layer health status."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release all resources. No further calls are valid after this."""
        ...


class InMemoryProjectionStore(ProjectionStore):
    """Strict in-memory fake — used for projection idempotency tests.

    Intentionally strict: malformed payloads and unknown commands are
    terminal failures. Idempotency is enforced via _applied set.
    No concurrency, no persistence — tests only.
    """

    def __init__(self) -> None:
        self._users: dict[str, dict] = {}  # user_id → data payload
        self._events: list[dict] = []      # append-only event rows
        self._applied: set[str] = set()    # request_ids successfully applied

    # ------------------------------------------------------------------
    # ProjectionStore interface
    # ------------------------------------------------------------------

    async def apply(self, command: Command) -> Result:
        if command.command == "create_user":
            return self._create_user(command)
        if command.command == "append_event":
            return self._append_event(command)
        return Result(
            success=False,
            status_code=400,
            error=f"unknown command: {command.command!r}",
            request_id=command.request_id,
            retryable=False,
        )

    async def health(self) -> Result:
        return Result(success=True, status_code=200)

    async def close(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Projection state accessors (tests only)
    # ------------------------------------------------------------------

    @property
    def users(self) -> dict[str, dict]:
        return dict(self._users)

    @property
    def events(self) -> list[dict]:
        return list(self._events)

    @property
    def applied(self) -> frozenset[str]:
        return frozenset(self._applied)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _create_user(self, command: Command) -> Result:
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
        # Idempotent by request_id: same request produces same result, no mutation.
        if command.request_id in self._applied:
            return Result(
                success=True,
                status_code=200,
                data={"status": "accepted"},
                request_id=command.request_id,
            )
        # UPSERT by user_id: same user may be re-created (idempotent on content).
        self._users[user_id] = p.get("data", {})
        self._applied.add(command.request_id)
        return Result(
            success=True,
            status_code=200,
            data={"status": "accepted"},
            request_id=command.request_id,
        )

    def _append_event(self, command: Command) -> Result:
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
        # Idempotent by request_id: same request does not append a duplicate row.
        if command.request_id in self._applied:
            return Result(
                success=True,
                status_code=200,
                data={"status": "accepted"},
                request_id=command.request_id,
            )
        self._events.append({
            "request_id": command.request_id,
            "event_type": event_type,
            "data": p.get("data", {}),
        })
        self._applied.add(command.request_id)
        return Result(
            success=True,
            status_code=200,
            data={"status": "accepted"},
            request_id=command.request_id,
        )
