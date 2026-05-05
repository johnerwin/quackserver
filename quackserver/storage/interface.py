"""
StorageInterface — the only public contract for all storage operations.

Application logic depends exclusively on this interface.
DuckDB is an implementation detail hidden behind it (spec §2.3, §13).
The sole DuckDB import in this codebase lives in storage/duckdb_impl.py.

This boundary is the migration exit ramp: replacing DuckDB with Postgres
means writing a new implementation of this class — nothing else changes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class Result:
    """Uniform return value for every StorageInterface method.

    Every outcome — success or failure — carries an HTTP status code so the
    transport layer never has to infer it. This keeps failure semantics
    (spec §4) machine-readable from the start and makes idempotency cache
    entries serialisable without extra mapping.

    Fields:
        success:     True iff the operation completed without error.
        status_code: HTTP status code to return to the caller (200, 400,
                     503, etc.). Must align with the failure semantics table
                     in spec §4.
        data:        Operation result payload. None on failure.
        error:       Human-readable error message. None on success.
        request_id:  Echoed from the inbound command for log correlation
                     (spec §10). None for read operations.
        duration_ms: Wall-clock ms for the operation; used in structured
                     log fields (spec §10.1).

    slots=True: this object is created on every storage call; avoiding
    __dict__ overhead matters at the volume the write worker produces.
    """

    success: bool
    status_code: int
    data: dict[str, Any] | None = None
    error: str | None = None
    # Echoed from the inbound write command for log correlation (spec §10).
    request_id: str | None = None
    # Integer ms, not float — sub-ms precision is noise at this layer.
    duration_ms: int | None = None
    # Retry hint from the failure producer. False = terminal (don't retry);
    # True = transient (worker may retry up to MAX_WRITE_RETRIES).
    # Only meaningful when success=False.
    retryable: bool = False


class StorageInterface(ABC):
    """Abstract base class for all storage backends.

    All methods are async. Implementations must be safe to call from an
    asyncio event loop running alongside the write worker and read pool.

    Write methods are called by the write worker after a log entry is
    dequeued — they must not be called directly from HTTP handlers.

    Read methods are called under the asyncio.Semaphore in the read pool.
    """

    # ------------------------------------------------------------------
    # Write operations
    # Called exclusively by the write worker (spec §2.1, §11.1).
    # Each method receives the client-generated request_id so the
    # implementation can enforce idempotency at the storage layer too.
    # ------------------------------------------------------------------

    @abstractmethod
    async def create_user(
        self, request_id: str, user_id: str, payload: dict[str, Any]
    ) -> Result:
        """Create or register a user record.

        Implementations must be idempotent for a given request_id.
        """
        ...

    @abstractmethod
    async def append_event(
        self, request_id: str, event_type: str, payload: dict[str, Any]
    ) -> Result:
        """Append an immutable event to the event store.

        Append-only — implementations must never update existing rows (spec §6.1).
        Implementations must be idempotent for a given request_id.
        """
        ...

    # ------------------------------------------------------------------
    # Read operations
    # Called from the read pool under asyncio.Semaphore (spec §9.1).
    # Implementations must respect per-endpoint timeout and row limits
    # defined in config.py (spec §9.2).
    # ------------------------------------------------------------------

    @abstractmethod
    async def get_dashboard_metrics(self, filters: dict[str, Any]) -> Result:
        """Return pre-aggregated dashboard metrics.

        Must complete within DASHBOARD_QUERY_TIMEOUT_MS.
        Must not return more than DASHBOARD_MAX_ROWS rows.
        """
        ...

    @abstractmethod
    async def query_events(self, filters: dict[str, Any], limit: int) -> Result:
        """Query events with caller-supplied filters and a hard row cap.

        filters must include a time-range constraint (spec §9.2).
        limit must not exceed EVENT_MAX_ROWS.
        Must complete within EVENT_QUERY_TIMEOUT_MS.
        """
        ...

    # ------------------------------------------------------------------
    # Health and observability
    # ------------------------------------------------------------------

    @abstractmethod
    async def check_health(self) -> Result:
        """Return storage layer health status.

        Must complete within HEALTH_QUERY_TIMEOUT_MS (spec §9.2).
        Must never block under any load condition.
        """
        ...

    # ------------------------------------------------------------------
    # Lifecycle and migration
    # ------------------------------------------------------------------

    @abstractmethod
    async def bulk_import(self, source_path: str) -> Result:
        """Onboard an existing DuckDB database as initial state.

        Intended for importing a pre-existing database (e.g. a large
        production file) before the append log takes over as canonical
        truth. This is a one-time operation performed before the server
        begins accepting write traffic.

        Implementations must:
        - Accept a filesystem path to the source .duckdb file.
        - Produce a clean, compacted output (reclaims deleted-row space).
        - Be safe to re-run (idempotent outcome, not necessarily cheap).
        - Complete before the HTTP server is started.

        This is not a live migration path — it is an onboarding ramp.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release all connections and resources gracefully.

        Called during shutdown. After close() returns, no further method
        calls are valid on this instance.
        """
        ...
