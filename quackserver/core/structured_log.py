"""
Structured JSON logging — spec §10.

Every log line is a single JSON object, parseable without regex.
Field names match the spec §10.1 event table.
request_id is passed as a keyword argument where available.

Usage:
    from quackserver.core.structured_log import get_logger
    log = get_logger(__name__)
    log.info("write_accepted", request_id="abc", queue_depth=5)

Output:
    {"ts": "2026-05-05T14:43:00Z", "level": "INFO", "logger": "...",
     "event": "write_accepted", "request_id": "abc", "queue_depth": 5}

Call configure_logging() once at process startup. Tests that don't call
it get stdlib default behaviour (stderr, unformatted) — which is fine
because pytest captures stderr.
"""

import json
import logging
import sys
import time
from typing import Any


# Stdlib LogRecord attributes that are not extra fields.
_RESERVED = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname",
    "filename", "module", "exc_info", "exc_text", "stack_info",
    "lineno", "funcName", "created", "msecs", "relativeCreated",
    "thread", "threadName", "processName", "process", "message",
    "taskName",
})


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                doc[key] = value
        return json.dumps(doc, default=str)


class StructuredLogger:
    """Thin wrapper around stdlib Logger that accepts keyword extra fields.

    Methods mirror stdlib levels. Keyword arguments become top-level
    fields in the JSON output alongside 'event'.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _emit(self, level: int, event: str, **kwargs: Any) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(level, event, extra=kwargs)

    def debug(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.DEBUG, event, **kwargs)

    def info(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.INFO, event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.WARNING, event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, event, **kwargs)

    def critical(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.CRITICAL, event, **kwargs)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger with JSON output to stdout.

    Call once at process startup before any other module creates a logger.
    Idempotent: clears existing handlers before adding the JSON handler.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> StructuredLogger:
    """Return a StructuredLogger for the given module name."""
    return StructuredLogger(logging.getLogger(name))
