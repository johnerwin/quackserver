"""
JSONL append log — canonical source of truth for all writes.

Design rules:
  - The file is append-only. Lines are never modified or deleted in place.
  - Applied state lives in ReplayCheckpoint, not here.
  - A caller that receives no exception from AppendLogWriter.append() may
    return 200 to the HTTP client. The entry is fsync'd to disk. DuckDB
    projection happens asynchronously via the write worker (spec §7.1).
  - Malformed lines are skipped during read, not raised. A single corrupt
    line must not brick recovery of the entries that follow it. But silence
    is not acceptable — every malformed line is logged at ERROR level with
    the line number and a raw excerpt, and counted for startup telemetry.

Entry shape (one JSON object per line):
  {
    "request_id": "<client-generated UUID/ULID>",
    "command":    "<StorageInterface method name>",
    "payload":    { ... },
    "timestamp":  "<ISO 8601 UTC>"
  }

Public types:
  AppendLogWriter — appends Command objects to a JSONL file with fsync.
  AppendLogReader — reads Command objects back in write order.

AppendLogEntry is an internal serialisation detail. External callers use
Command (from quackserver.core.commands). The log module translates between
them at the boundary.
"""

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator

from quackserver.core.commands import Command
from quackserver.core.structured_log import get_logger

_log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Internal serialisation type — not exported
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _AppendLogEntry:
    """Internal on-disk representation. One-to-one with a JSONL line.

    Kept separate from Command so the serialisation format can evolve
    (e.g. add a schema version field) without changing the runtime type.
    """

    request_id: str
    command: str
    payload: dict[str, Any]
    timestamp: str


def _entry_to_command(e: _AppendLogEntry) -> Command:
    return Command(
        request_id=e.request_id,
        command=e.command,
        payload=e.payload,
        timestamp=e.timestamp,
    )


def _command_to_entry(cmd: Command) -> _AppendLogEntry:
    return _AppendLogEntry(
        request_id=cmd.request_id,
        command=cmd.command,
        payload=cmd.payload,
        timestamp=cmd.timestamp,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class AppendLogWriter:
    """Appends Command objects to a JSONL file with fsync durability.

    Each call to append() flushes the OS write buffer and calls fsync
    before returning. The caller may treat a successful return as a
    durability guarantee: the entry will survive a process crash.

    This is synchronous blocking I/O. In the async HTTP phase it is called
    via loop.run_in_executor so it does not block the event loop.

    Thread safety: not thread-safe. Designed for single-writer use — the
    HTTP handler serialises through the asyncio event loop.
    """

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._fh = path.open("a", encoding="utf-8")

    def append(self, cmd: Command) -> None:
        entry = _command_to_entry(cmd)
        line = json.dumps(asdict(entry), ensure_ascii=False)
        self._fh.write(line + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "AppendLogWriter":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AppendLogReader:
    """Reads Command objects from a JSONL append log in write order.

    Malformed lines are skipped and logged at ERROR level with the line
    number and a raw excerpt. The malformed_count property accumulates
    across calls to entries() for use in startup telemetry.
    """

    _EXCERPT_LIMIT = 120  # chars of raw line to include in error log

    def __init__(self, path: Path) -> None:
        self._path = path
        self.malformed_count: int = 0

    def entries(self) -> Iterator[Command]:
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, start=1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    entry = _AppendLogEntry(
                        request_id=data["request_id"],
                        command=data["command"],
                        payload=data["payload"],
                        timestamp=data["timestamp"],
                    )
                    yield _entry_to_command(entry)
                except (json.JSONDecodeError, KeyError) as exc:
                    self.malformed_count += 1
                    excerpt = line[: self._EXCERPT_LIMIT]
                    _log.error(
                        "append_log_malformed_line",
                        log_lineno=lineno,  # 'lineno' is reserved by stdlib LogRecord
                        excerpt=excerpt,
                        error=str(exc),
                        log_path=str(self._path),
                    )
