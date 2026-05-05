"""Tests for DedupStore — idempotency cache."""

import time
from pathlib import Path
from unittest.mock import patch

from quackserver.core.dedup import DedupStore
from quackserver.storage.interface import Result


def _result(success: bool = True, code: int = 200, **kw) -> Result:
    return Result(success=success, status_code=code, **kw)


class TestDedupStore:
    def test_get_returns_none_for_unknown(self, tmp_path):
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            assert d.get("not-there") is None

    def test_put_and_get_roundtrip(self, tmp_path):
        r = _result(data={"rows": 7}, request_id="abc", duration_ms=12)
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            d.put("abc", r)
            got = d.get("abc")
        assert got is not None
        assert got.success is True
        assert got.status_code == 200
        assert got.data == {"rows": 7}
        assert got.request_id == "abc"
        assert got.duration_ms == 12

    def test_failure_result_roundtrip(self, tmp_path):
        r = _result(success=False, code=503, error="queue full", request_id="fail1")
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            d.put("fail1", r)
            got = d.get("fail1")
        assert got is not None
        assert got.success is False
        assert got.status_code == 503
        assert got.error == "queue full"

    def test_none_data_roundtrip(self, tmp_path):
        r = _result(data=None, request_id="no-data")
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            d.put("no-data", r)
            got = d.get("no-data")
        assert got is not None
        assert got.data is None

    def test_get_respects_ttl(self, tmp_path):
        r = _result(request_id="old")
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            d.put("old", r)
            # Backdating stored_at past the TTL window.
            d._conn.execute(
                "UPDATE dedup_store SET stored_at = 0 WHERE request_id = ?", ("old",)
            )
            d._conn.commit()
            assert d.get("old") is None

    def test_purge_expired_removes_stale_entries(self, tmp_path):
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            d.put("fresh", _result(request_id="fresh"))
            d.put("stale", _result(request_id="stale"))
            d._conn.execute(
                "UPDATE dedup_store SET stored_at = 0 WHERE request_id = ?", ("stale",)
            )
            d._conn.commit()
            count = d.purge_expired()
            assert count == 1
            assert d.get("fresh") is not None
            assert d.get("stale") is None

    def test_put_overwrites_existing(self, tmp_path):
        with DedupStore(tmp_path / "dedup.sqlite") as d:
            d.put("r1", _result(code=200))
            d.put("r1", _result(code=503, success=False))
            got = d.get("r1")
        assert got is not None
        assert got.status_code == 503

    def test_creates_parent_directories(self, tmp_path):
        path = tmp_path / "deep" / "dir" / "dedup.sqlite"
        with DedupStore(path) as d:
            d.put("x", _result())
        assert path.exists()

    def test_persists_across_reopen(self, tmp_path):
        path = tmp_path / "dedup.sqlite"
        with DedupStore(path) as d:
            d.put("r1", _result(request_id="r1"))
        with DedupStore(path) as d:
            assert d.get("r1") is not None
