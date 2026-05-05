"""
ReplayScanner — identifies append-log entries not yet projected into DuckDB.

Used exclusively during startup recovery. The HTTP server must not accept
requests until scan_pending() has been drained through the write worker
(spec §7.3, §11.1).

Guarantees:
  - Entries are yielded in log order (write order is preserved).
  - An entry is pending iff its request_id is absent from the checkpoint.
  - If the same request_id appears more than once in the log (duplicate
    write due to retry before dedup caching completes), only the first
    occurrence is yielded. The checkpoint makes subsequent occurrences
    redundant.
"""

from typing import Iterator

from quackserver.core.checkpoint import ReplayCheckpoint
from quackserver.core.commands import Command
from quackserver.core.log import AppendLogReader


class ReplayScanner:
    """Yields pending log entries in write order.

    Instantiate fresh for each startup scan — it reads the log from
    the beginning each time and tracks seen request_ids only for the
    duration of a single scan.
    """

    def __init__(self, reader: AppendLogReader, checkpoint: ReplayCheckpoint) -> None:
        self._reader = reader
        self._checkpoint = checkpoint

    def scan_pending(self) -> Iterator[Command]:
        """Yield Commands that have not been applied to DuckDB, in log order.

        The caller (startup sequence) should pass each yielded Command to
        the write worker and wait for it to finish before accepting HTTP
        traffic.
        """
        seen: set[str] = set()
        for cmd in self._reader.entries():
            if cmd.request_id in seen:
                continue
            if not self._checkpoint.is_applied(cmd.request_id):
                seen.add(cmd.request_id)
                yield cmd

    def count_pending(self) -> int:
        """Count pending Commands without consuming them.

        Used for the startup log line: 'crash_recovery_started entries=N'.
        Performs a full log scan — call once, not in a hot path.
        """
        return sum(1 for _ in self.scan_pending())
