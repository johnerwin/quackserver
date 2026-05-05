"""Tests for the JSONL append log (AppendLogWriter / AppendLogReader)."""

import json
import logging
from pathlib import Path

import pytest

from quackserver.core.commands import Command
from quackserver.core.log import AppendLogReader, AppendLogWriter


def _cmd(request_id: str, command: str = "append_event", **payload_kw) -> Command:
    return Command(request_id, command, dict(payload_kw) or {}, Command.now_utc())


# ---------------------------------------------------------------------------
# AppendLogWriter
# ---------------------------------------------------------------------------


class TestAppendLogWriter:
    def test_creates_file_on_first_write(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        assert not path.exists()
        with AppendLogWriter(path) as w:
            w.append(_cmd("r1"))
        assert path.exists()

    def test_creates_parent_directories(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "journal.jsonl"
        with AppendLogWriter(path) as w:
            w.append(_cmd("r1"))
        assert path.exists()

    def test_each_entry_is_valid_json(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        with AppendLogWriter(path) as w:
            w.append(_cmd("r1", a=1))
            w.append(_cmd("r2", "create_user", b=2))
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            obj = json.loads(line)
            assert {"request_id", "command", "payload", "timestamp"} <= obj.keys()

    def test_append_preserves_payload(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        payload = {"event_type": "page.view", "data": {"url": "/dashboard"}}
        cmd = Command("r1", "append_event", payload, Command.now_utc())
        with AppendLogWriter(path) as w:
            w.append(cmd)
        raw = json.loads(path.read_text(encoding="utf-8").strip())
        assert raw["payload"] == payload

    def test_successive_appends_grow_file(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        with AppendLogWriter(path) as w:
            for i in range(5):
                w.append(_cmd(f"r{i}"))
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 5

    def test_reopening_appends_not_truncates(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        with AppendLogWriter(path) as w:
            w.append(_cmd("r1"))
        with AppendLogWriter(path) as w:
            w.append(_cmd("r2"))
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2

    def test_context_manager_closes_file(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        with AppendLogWriter(path) as w:
            pass
        assert w._fh.closed


# ---------------------------------------------------------------------------
# AppendLogReader
# ---------------------------------------------------------------------------


class TestAppendLogReader:
    def test_nonexistent_file_yields_nothing(self, tmp_path):
        reader = AppendLogReader(tmp_path / "missing.jsonl")
        assert list(reader.entries()) == []

    def test_reads_commands_in_order(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        written = [_cmd(f"r{i}", event_type=f"e{i}") for i in range(4)]
        with AppendLogWriter(path) as w:
            for c in written:
                w.append(c)
        read = list(AppendLogReader(path).entries())
        assert len(read) == 4
        for orig, got in zip(written, read):
            assert got.request_id == orig.request_id
            assert got.command == orig.command
            assert got.payload == orig.payload

    def test_yields_command_objects(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        with AppendLogWriter(path) as w:
            w.append(_cmd("r1"))
        (result,) = list(AppendLogReader(path).entries())
        assert isinstance(result, Command)

    def test_skips_blank_lines(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        path.write_text(
            '{"request_id":"r1","command":"append_event","payload":{},"timestamp":"t"}\n'
            "\n"
            '{"request_id":"r2","command":"append_event","payload":{},"timestamp":"t"}\n',
            encoding="utf-8",
        )
        entries = list(AppendLogReader(path).entries())
        assert len(entries) == 2

    def test_skips_malformed_json_and_counts(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        path.write_text(
            '{"request_id":"r1","command":"append_event","payload":{},"timestamp":"t"}\n'
            "NOT VALID JSON\n"
            '{"request_id":"r2","command":"append_event","payload":{},"timestamp":"t"}\n',
            encoding="utf-8",
        )
        reader = AppendLogReader(path)
        entries = list(reader.entries())
        assert len(entries) == 2
        assert entries[0].request_id == "r1"
        assert entries[1].request_id == "r2"
        assert reader.malformed_count == 1

    def test_skips_entries_with_missing_keys_and_counts(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        path.write_text(
            '{"request_id":"r1","command":"append_event","payload":{},"timestamp":"t"}\n'
            '{"only_key":"oops"}\n'
            '{"request_id":"r3","command":"append_event","payload":{},"timestamp":"t"}\n',
            encoding="utf-8",
        )
        reader = AppendLogReader(path)
        entries = list(reader.entries())
        assert [e.request_id for e in entries] == ["r1", "r3"]
        assert reader.malformed_count == 1

    def test_malformed_lines_produce_error_log(self, tmp_path, caplog):
        path = tmp_path / "journal.jsonl"
        path.write_text("NOT VALID JSON\n", encoding="utf-8")
        with caplog.at_level(logging.ERROR, logger="quackserver.core.log"):
            list(AppendLogReader(path).entries())
        assert any("append_log_malformed_line" in r.message for r in caplog.records)

    def test_malformed_count_accumulates_across_calls(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        path.write_text("BAD\nBAD\n", encoding="utf-8")
        reader = AppendLogReader(path)
        list(reader.entries())
        list(reader.entries())
        assert reader.malformed_count == 4

    def test_roundtrip_preserves_all_fields(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        original = Command(
            request_id="unique-id-42",
            command="create_user",
            payload={"user_id": "u99", "data": {"name": "Alice"}},
            timestamp="2026-05-05T10:00:00+00:00",
        )
        with AppendLogWriter(path) as w:
            w.append(original)
        (recovered,) = list(AppendLogReader(path).entries())
        assert recovered.request_id == original.request_id
        assert recovered.command == original.command
        assert recovered.payload == original.payload
        assert recovered.timestamp == original.timestamp
