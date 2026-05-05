"""Tests for the Command envelope."""

import pytest

from quackserver.core.commands import Command


class TestCommand:
    def test_fields_accessible(self):
        c = Command("r1", "append_event", {"a": 1}, "2026-05-05T00:00:00+00:00")
        assert c.request_id == "r1"
        assert c.command == "append_event"
        assert c.payload == {"a": 1}
        assert c.timestamp == "2026-05-05T00:00:00+00:00"

    def test_is_frozen(self):
        c = Command("r1", "append_event", {}, Command.now_utc())
        with pytest.raises((AttributeError, TypeError)):
            c.request_id = "mutated"  # type: ignore[misc]

    def test_uses_slots(self):
        assert hasattr(Command, "__slots__")

    def test_now_utc_returns_nonempty_string(self):
        ts = Command.now_utc()
        assert isinstance(ts, str)
        assert len(ts) > 0
        assert "T" in ts

    def test_now_utc_is_utc(self):
        ts = Command.now_utc()
        # ISO 8601 UTC is indicated by +00:00 suffix
        assert ts.endswith("+00:00") or ts.endswith("Z")

    def test_successive_now_utc_calls_are_nondecreasing(self):
        t1 = Command.now_utc()
        t2 = Command.now_utc()
        assert t2 >= t1


class TestCommandValidate:
    def test_valid_command_has_no_errors(self):
        c = Command("req-1", "append_event", {}, Command.now_utc())
        assert c.validate() == []

    def test_empty_request_id_is_invalid(self):
        c = Command("", "append_event", {}, Command.now_utc())
        errors = c.validate()
        assert len(errors) == 1
        assert "request_id" in errors[0]

    def test_blank_request_id_is_invalid(self):
        c = Command("   ", "append_event", {}, Command.now_utc())
        errors = c.validate()
        assert any("request_id" in e for e in errors)

    def test_empty_command_is_invalid(self):
        c = Command("r1", "", {}, Command.now_utc())
        errors = c.validate()
        assert len(errors) == 1
        assert "command" in errors[0]

    def test_both_fields_empty_returns_two_errors(self):
        c = Command("", "", {}, Command.now_utc())
        errors = c.validate()
        assert len(errors) == 2

    def test_validate_does_not_check_payload_schema(self):
        # Payload validation is command-specific; validate() only checks structure.
        c = Command("r1", "append_event", {"anything": "goes"}, Command.now_utc())
        assert c.validate() == []

    def test_validate_does_not_check_unknown_commands(self):
        # Unknown command detection is the worker's responsibility.
        c = Command("r1", "drop_everything", {}, Command.now_utc())
        assert c.validate() == []


class TestCommandIsolation:
    def test_command_does_not_import_duckdb(self):
        import ast
        from pathlib import Path

        path = Path(__file__).parent.parent / "quackserver" / "core" / "commands.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported.append(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imported.append(node.module.split(".")[0])
        assert "duckdb" not in imported
