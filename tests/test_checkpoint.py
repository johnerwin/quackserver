"""Tests for ReplayCheckpoint."""

from quackserver.core.checkpoint import ReplayCheckpoint


class TestReplayCheckpoint:
    def test_unknown_request_id_is_not_applied(self, tmp_path):
        with ReplayCheckpoint(tmp_path / "cp.sqlite") as cp:
            assert cp.is_applied("never-seen") is False

    def test_mark_then_check(self, tmp_path):
        with ReplayCheckpoint(tmp_path / "cp.sqlite") as cp:
            cp.mark_applied("r1")
            assert cp.is_applied("r1") is True

    def test_mark_is_idempotent(self, tmp_path):
        with ReplayCheckpoint(tmp_path / "cp.sqlite") as cp:
            cp.mark_applied("r1")
            cp.mark_applied("r1")  # second call must not raise
            assert cp.is_applied("r1") is True

    def test_multiple_entries(self, tmp_path):
        with ReplayCheckpoint(tmp_path / "cp.sqlite") as cp:
            for i in range(10):
                cp.mark_applied(f"r{i}")
            for i in range(10):
                assert cp.is_applied(f"r{i}") is True
            assert cp.is_applied("r99") is False

    def test_persists_across_reopen(self, tmp_path):
        path = tmp_path / "cp.sqlite"
        with ReplayCheckpoint(path) as cp:
            cp.mark_applied("r1")
        with ReplayCheckpoint(path) as cp:
            assert cp.is_applied("r1") is True

    def test_does_not_expire(self, tmp_path):
        path = tmp_path / "cp.sqlite"
        with ReplayCheckpoint(path) as cp:
            cp.mark_applied("r1")
            # Backdate applied_at to zero — unlike dedup store, this must not expire.
            cp._conn.execute(
                "UPDATE replay_checkpoint SET applied_at = 0 WHERE request_id = ?",
                ("r1",),
            )
            cp._conn.commit()
            assert cp.is_applied("r1") is True

    def test_creates_parent_directories(self, tmp_path):
        path = tmp_path / "deep" / "dir" / "cp.sqlite"
        with ReplayCheckpoint(path) as cp:
            cp.mark_applied("r1")
        assert path.exists()
