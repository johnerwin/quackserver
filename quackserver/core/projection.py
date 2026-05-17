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
        self._users: dict[str, dict] = {}        # user_id → data payload
        self._events: list[dict] = []             # append-only event rows
        self._gov_passes: dict[str, dict] = {}    # pass_id → pass record
        self._gov_sessions: list[dict] = []       # session records (ordered)
        self._applied: set[str] = set()           # request_ids successfully applied

    # ------------------------------------------------------------------
    # ProjectionStore interface
    # ------------------------------------------------------------------

    async def apply(self, command: Command) -> Result:
        if command.command == "create_user":
            return self._create_user(command)
        if command.command == "append_event":
            return self._append_event(command)
        if command.command == "LOG_EXPLORATORY_PASS":
            return self._log_exploratory_pass(command)
        if command.command == "LOG_SESSION_STARTED":
            return self._log_session_started(command)
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
    def gov_passes(self) -> dict[str, dict]:
        return dict(self._gov_passes)

    @property
    def gov_sessions(self) -> list[dict]:
        return list(self._gov_sessions)

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

    def _log_exploratory_pass(self, command: Command) -> Result:
        p = command.payload
        metric_family = p.get("metric_family")
        cohort = p.get("cohort")
        pass_number = p.get("pass_number")
        script_name = p.get("script_name")
        if not metric_family or not cohort or not script_name:
            return Result(
                success=False,
                status_code=400,
                error="LOG_EXPLORATORY_PASS: missing required field(s) "
                      "(metric_family, cohort, script_name)",
                request_id=command.request_id,
                retryable=False,
            )
        if not isinstance(pass_number, int) or not (1 <= pass_number <= 3):
            return Result(
                success=False,
                status_code=400,
                error=f"LOG_EXPLORATORY_PASS: pass_number must be 1–3, "
                      f"got {pass_number!r}",
                request_id=command.request_id,
                retryable=False,
            )
        # Idempotent by request_id (pass_id).
        if command.request_id in self._applied:
            return Result(
                success=True,
                status_code=200,
                data={"status": "accepted"},
                request_id=command.request_id,
            )
        self._gov_passes[command.request_id] = {
            "pass_id": command.request_id,
            "metric_family": metric_family,
            "cohort": cohort,
            "pass_number": pass_number,
            "script_name": script_name,
            "exp_id": p.get("exp_id"),
            "event_version": p.get("event_version", "1.0"),
            "notes": p.get("notes"),
        }
        self._applied.add(command.request_id)
        return Result(
            success=True,
            status_code=200,
            data={"status": "accepted"},
            request_id=command.request_id,
        )

    def _log_session_started(self, command: Command) -> Result:
        # Idempotent by request_id (session_id).
        if command.request_id in self._applied:
            return Result(
                success=True,
                status_code=200,
                data={"status": "accepted"},
                request_id=command.request_id,
            )
        p = command.payload
        self._gov_sessions.append({
            "session_id": command.request_id,
            "event_version": int(float(p.get("event_version", 1))),
            "notes": p.get("notes"),
        })
        self._applied.add(command.request_id)
        return Result(
            success=True,
            status_code=200,
            data={"status": "accepted"},
            request_id=command.request_id,
        )
