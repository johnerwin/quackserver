"""Tests for ReplayScanner."""

from quackserver.core.checkpoint import ReplayCheckpoint
from quackserver.core.commands import Command
from quackserver.core.log import AppendLogReader, AppendLogWriter
from quackserver.core.replay import ReplayScanner


def _cmd(request_id: str, command: str = "append_event") -> Command:
    return Command(request_id, command, {}, Command.now_utc())


def _write_commands(path, cmds) -> None:
    with AppendLogWriter(path) as w:
        for c in cmds:
            w.append(c)


class TestReplayScanner:
    def test_empty_log_yields_nothing(self, tmp_path):
        reader = AppendLogReader(tmp_path / "missing.jsonl")
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        scanner = ReplayScanner(reader, cp)
        assert list(scanner.scan_pending()) == []
        cp.close()

    def test_all_pending_when_checkpoint_empty(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        cmds = [_cmd(f"r{i}") for i in range(3)]
        _write_commands(path, cmds)

        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        scanner = ReplayScanner(AppendLogReader(path), cp)
        pending = list(scanner.scan_pending())
        assert [c.request_id for c in pending] == ["r0", "r1", "r2"]
        cp.close()

    def test_already_applied_entries_are_skipped(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        cmds = [_cmd(f"r{i}") for i in range(4)]
        _write_commands(path, cmds)

        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        cp.mark_applied("r1")
        cp.mark_applied("r3")

        scanner = ReplayScanner(AppendLogReader(path), cp)
        pending = list(scanner.scan_pending())
        assert [c.request_id for c in pending] == ["r0", "r2"]
        cp.close()

    def test_all_applied_yields_nothing(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        cmds = [_cmd(f"r{i}") for i in range(3)]
        _write_commands(path, cmds)

        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        for c in cmds:
            cp.mark_applied(c.request_id)

        scanner = ReplayScanner(AppendLogReader(path), cp)
        assert list(scanner.scan_pending()) == []
        cp.close()

    def test_duplicate_request_ids_in_log_yields_once(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        dup = _cmd("dup-id")
        _write_commands(path, [dup, dup, _cmd("other")])

        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        scanner = ReplayScanner(AppendLogReader(path), cp)
        pending = list(scanner.scan_pending())
        assert [c.request_id for c in pending] == ["dup-id", "other"]
        cp.close()

    def test_preserves_log_order(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        ids = ["z9", "a1", "m5", "b2"]
        _write_commands(path, [_cmd(i) for i in ids])

        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        scanner = ReplayScanner(AppendLogReader(path), cp)
        pending = list(scanner.scan_pending())
        assert [c.request_id for c in pending] == ids
        cp.close()

    def test_count_pending_matches_scan_pending(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        _write_commands(path, [_cmd(f"r{i}") for i in range(5)])

        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        cp.mark_applied("r2")

        scanner = ReplayScanner(AppendLogReader(path), cp)
        assert scanner.count_pending() == 4
        cp.close()

    def test_scan_yields_command_objects(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        _write_commands(path, [_cmd("r1")])
        cp = ReplayCheckpoint(tmp_path / "cp.sqlite")
        scanner = ReplayScanner(AppendLogReader(path), cp)
        pending = list(scanner.scan_pending())
        assert len(pending) == 1
        assert isinstance(pending[0], Command)
        cp.close()
