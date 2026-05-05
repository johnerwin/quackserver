"""
Command — the universal in-memory write command envelope.

This is the single type that flows through the entire write path:

  HTTP handler  →  Command  →  AppendLogWriter (disk serialisation)
  AppendLogReader (disk)  →  Command  →  ReplayScanner  →  WriteWorker

Why separate from the on-disk format:
  - Transport types (FastAPI request/response models) must never leak into
    the runtime. The HTTP layer creates Commands; it doesn't hand its own
    schema objects to the log or the queue.
  - The log serialisation format can evolve (compression, versioning) without
    changing what the runtime passes around.
  - Validation lives here, not scattered across HTTP handlers or the worker.
  - The runtime is fully testable without HTTP or disk I/O.

frozen=True: Commands are immutable after creation. The write path creates
one Command per inbound request and passes it unchanged through every stage.
Mutability here would be a class of bug, not a feature.

Note on payload immutability: frozen=True prevents attribute reassignment
but cannot make the payload dict itself immutable. Callers must not mutate
the payload after construction. Treat it as read-only by convention.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True, slots=True)
class Command:
    """A write command flowing through the quackserver runtime.

    Fields:
        request_id: client-generated idempotency key (spec §8.1). Must be
                    non-empty. Missing or empty request_id is a hard 400.
        command:    StorageInterface method name to invoke. Must be a known
                    write command. Unknown commands are logged and dropped.
        payload:    Command arguments. Schema is command-specific; see
                    WriteWorker._dispatch for the expected keys per command.
        timestamp:  ISO 8601 UTC, set at intake time (HTTP handler or replay
                    startup). Reflects when the command was received, not
                    when it was written to disk or applied to DuckDB.
    """

    request_id: str
    command: str
    payload: dict[str, Any]
    timestamp: str

    @staticmethod
    def now_utc() -> str:
        """Return the current UTC time as an ISO 8601 string."""
        return datetime.now(timezone.utc).isoformat()

    def validate(self) -> list[str]:
        """Return a list of validation errors. Empty list means valid.

        Checks structural requirements only — not payload schema, which is
        command-specific and validated in the HTTP handler.

        Called before writing to the append log so invalid commands never
        reach the queue or the worker.
        """
        errors: list[str] = []
        if not self.request_id or not self.request_id.strip():
            errors.append("request_id is required and must not be blank")
        if not self.command or not self.command.strip():
            errors.append("command is required and must not be blank")
        return errors
