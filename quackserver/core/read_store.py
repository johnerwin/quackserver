"""
ReadStore — the read boundary between HTTP handlers and derived state.

Separate from ProjectionStore (write-only). Read methods return Result
objects with pre-shaped data ready for HTTP serialisation.

Why separate from ProjectionStore:
  The write worker needs apply() / health() / close(). Read handlers need
  dashboard() and report queries. Merging them would couple the read and
  write interfaces, making it harder to swap implementations independently.

Idempotency note:
  Reads are always eventual — DuckDB may not reflect the very latest writes.
  Callers must not assume read-after-write consistency (spec §7.1).
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from quackserver.storage.interface import Result


class ReadStore(ABC):
    """Minimal contract for read-only projection queries."""

    @abstractmethod
    async def dashboard(self) -> Result:
        """Return pre-aggregated dashboard metrics.

        Must complete within DASHBOARD_QUERY_TIMEOUT_MS.
        Must not return more than DASHBOARD_MAX_ROWS aggregate rows.
        """
        ...

    @abstractmethod
    async def events(self, limit: int) -> Result:
        """Return recent events ordered by received_at descending.

        limit is enforced by the caller (runtime); implementors must
        respect it as a hard cap.
        """
        ...

    @abstractmethod
    async def users(self, limit: int) -> Result:
        """Return users ordered by user_id ascending.

        limit is enforced by the caller (runtime).
        """
        ...
