"""Tests for structured JSON logging."""

import io
import json
import logging

import pytest

from quackserver.core.structured_log import (
    StructuredLogger,
    _JsonFormatter,
    configure_logging,
    get_logger,
)


def _capture_log(level: int = logging.DEBUG) -> tuple[StructuredLogger, io.StringIO]:
    """Return a StructuredLogger wired to a StringIO buffer for inspection."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(_JsonFormatter())
    # Use an isolated logger so tests don't interfere with each other.
    inner = logging.getLogger(f"test_capture_{id(buf)}")
    inner.handlers.clear()
    inner.addHandler(handler)
    inner.setLevel(level)
    inner.propagate = False
    return StructuredLogger(inner), buf


class TestJsonFormatter:
    def test_output_is_valid_json(self):
        logger, buf = _capture_log()
        logger.info("hello")
        line = buf.getvalue().strip()
        obj = json.loads(line)
        assert isinstance(obj, dict)

    def test_required_fields_present(self):
        logger, buf = _capture_log()
        logger.info("test_event")
        obj = json.loads(buf.getvalue().strip())
        assert "ts" in obj
        assert "level" in obj
        assert "logger" in obj
        assert "event" in obj

    def test_event_field_matches_message(self):
        logger, buf = _capture_log()
        logger.info("write_accepted")
        obj = json.loads(buf.getvalue().strip())
        assert obj["event"] == "write_accepted"

    def test_level_field_matches_log_level(self):
        logger, buf = _capture_log()
        logger.warning("queue_depth_warning")
        obj = json.loads(buf.getvalue().strip())
        assert obj["level"] == "WARNING"

    def test_extra_kwargs_become_top_level_fields(self):
        logger, buf = _capture_log()
        logger.info("write_accepted", request_id="abc", queue_depth=5)
        obj = json.loads(buf.getvalue().strip())
        assert obj["request_id"] == "abc"
        assert obj["queue_depth"] == 5

    def test_ts_field_is_iso8601_utc(self):
        logger, buf = _capture_log()
        logger.info("startup")
        obj = json.loads(buf.getvalue().strip())
        ts = obj["ts"]
        assert "T" in ts
        assert ts.endswith("Z")

    def test_each_call_produces_one_line(self):
        logger, buf = _capture_log()
        logger.info("event_one")
        logger.info("event_two")
        lines = [l for l in buf.getvalue().strip().splitlines() if l]
        assert len(lines) == 2

    def test_multiple_events_are_all_valid_json(self):
        logger, buf = _capture_log()
        for i in range(5):
            logger.info(f"event_{i}", idx=i)
        for line in buf.getvalue().strip().splitlines():
            obj = json.loads(line)
            assert "event" in obj
            assert "idx" in obj


class TestStructuredLogger:
    def test_debug_level(self):
        logger, buf = _capture_log(logging.DEBUG)
        logger.debug("low_level", x=1)
        obj = json.loads(buf.getvalue().strip())
        assert obj["level"] == "DEBUG"
        assert obj["x"] == 1

    def test_error_level(self):
        logger, buf = _capture_log()
        logger.error("write_failed", error="boom")
        obj = json.loads(buf.getvalue().strip())
        assert obj["level"] == "ERROR"
        assert obj["error"] == "boom"

    def test_critical_level(self):
        logger, buf = _capture_log()
        logger.critical("crash_recovery_started", entries=42)
        obj = json.loads(buf.getvalue().strip())
        assert obj["level"] == "CRITICAL"
        assert obj["entries"] == 42

    def test_below_threshold_produces_no_output(self):
        logger, buf = _capture_log(logging.WARNING)
        logger.debug("should_not_appear")
        logger.info("also_not_appear")
        assert buf.getvalue() == ""


class TestGetLogger:
    def test_returns_structured_logger_instance(self):
        assert isinstance(get_logger("quackserver.test"), StructuredLogger)

    def test_same_name_returns_equivalent_logger(self):
        a = get_logger("quackserver.core.worker")
        b = get_logger("quackserver.core.worker")
        assert a._logger is b._logger
