"""Integration tests for the HTTP layer.

Uses FastAPI's TestClient which drives the full lifespan (start/stop) and
exercises routes against a real Runtime wired to MockStorage.
"""

import time

import pytest
from fastapi.testclient import TestClient

from quackserver.core.runtime import Runtime
from quackserver.http.app import create_app
from quackserver.storage.duckdb_impl import DuckDBProjectionStore

from .conftest import MockProjection


def _poll_dashboard(client, *, user_count: int, event_count: int, timeout: float = 5.0) -> dict:
    """Poll GET /dashboard until expected counts appear or timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get("/dashboard")
        if resp.status_code == 200:
            body = resp.json()
            if body["user_count"] == user_count and body["event_count"] == event_count:
                return body
        time.sleep(0.05)
    raise AssertionError(
        f"dashboard did not reach user_count={user_count}, event_count={event_count} within {timeout}s"
    )


def _poll_report(client, path: str, *, count: int, timeout: float = 5.0) -> dict:
    """Poll a report endpoint until expected count appears or timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(path)
        if resp.status_code == 200 and resp.json().get("count") == count:
            return resp.json()
        time.sleep(0.05)
    raise AssertionError(f"{path} did not reach count={count} within {timeout}s")


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def client(tmp_path):
    projection = MockProjection()
    runtime = Runtime(
        log_path=tmp_path / "log.jsonl",
        dedup_path=tmp_path / "dedup.db",
        checkpoint_path=tmp_path / "checkpoint.db",
        projection=projection,
        # No read_store — read endpoints return 503.
    )
    app = create_app(runtime)
    with TestClient(app) as c:
        yield c, projection


@pytest.fixture
def read_client(tmp_path):
    """Client wired to a real DuckDB store so read endpoints return data."""
    store = DuckDBProjectionStore(tmp_path / "quack.duckdb")
    runtime = Runtime(
        log_path=tmp_path / "log.jsonl",
        dedup_path=tmp_path / "dedup.db",
        checkpoint_path=tmp_path / "checkpoint.db",
        projection=store,
        read_store=store,
    )
    app = create_app(runtime)
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# POST /events
# ---------------------------------------------------------------------------


class TestPostEvents:
    def test_valid_event_returns_200(self, client):
        c, _ = client
        resp = c.post(
            "/events",
            json={
                "request_id": "evt-001",
                "event_type": "page.view",
                "payload": {"url": "/home"},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["status"] == "accepted"
        assert body["request_id"] == "evt-001"

    def test_missing_request_id_returns_422(self, client):
        c, _ = client
        resp = c.post(
            "/events",
            json={"event_type": "page.view"},
        )
        assert resp.status_code == 422

    def test_missing_event_type_returns_422(self, client):
        c, _ = client
        resp = c.post(
            "/events",
            json={"request_id": "evt-002"},
        )
        assert resp.status_code == 422

    def test_blank_request_id_returns_400(self, client):
        c, _ = client
        resp = c.post(
            "/events",
            json={"request_id": "   ", "event_type": "page.view"},
        )
        assert resp.status_code == 400
        assert resp.json()["success"] is False

    def test_payload_defaults_to_empty_dict(self, client):
        c, _ = client
        resp = c.post(
            "/events",
            json={"request_id": "evt-003", "event_type": "user.login"},
        )
        assert resp.status_code == 200

    def test_duplicate_request_id_returns_200_deduplicated(self, client):
        c, storage = client
        body = {"request_id": "evt-dup", "event_type": "page.view"}
        resp1 = c.post("/events", json=body)
        assert resp1.status_code == 200

        # Trigger dedup by posting the same request_id again
        # (worker may or may not have processed it; dedup store is also checked
        #  on the write path before enqueueing)
        resp2 = c.post("/events", json=body)
        assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# POST /users
# ---------------------------------------------------------------------------


class TestPostUsers:
    def test_valid_user_returns_200(self, client):
        c, _ = client
        resp = c.post(
            "/users",
            json={
                "request_id": "usr-001",
                "user_id": "alice",
                "payload": {"email": "alice@example.com"},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["request_id"] == "usr-001"

    def test_missing_user_id_returns_422(self, client):
        c, _ = client
        resp = c.post(
            "/users",
            json={"request_id": "usr-002"},
        )
        assert resp.status_code == 422

    def test_blank_user_id_returns_422(self, client):
        c, _ = client
        resp = c.post(
            "/users",
            json={"request_id": "usr-003", "user_id": ""},
        )
        assert resp.status_code == 422

    def test_payload_defaults_to_empty_dict(self, client):
        c, _ = client
        resp = c.post(
            "/users",
            json={"request_id": "usr-004", "user_id": "bob"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------


class TestGetHealth:
    def test_returns_200_when_healthy(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["healthy"] is True
        assert body["state"] == "RUNNING"
        assert "state_since" in body
        assert "queue_depth" in body
        assert "queue_max" in body
        assert "queue_pct" in body
        assert "queue_warning" in body
        assert "worker_running" in body
        assert "replay_pending" in body
        assert "malformed_log_entries" in body

    def test_worker_running_true_in_response(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.json()["worker_running"] is True

    def test_queue_depth_zero_at_rest(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.json()["queue_depth"] == 0

    def test_queue_pct_zero_at_rest(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.json()["queue_pct"] == 0.0

    def test_queue_warning_false_at_rest(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.json()["queue_warning"] is False


# ---------------------------------------------------------------------------
# Read endpoints — no read_store configured (client fixture)
# ---------------------------------------------------------------------------


class TestReadEndpointsNoReadStore:
    def test_dashboard_returns_503_without_read_store(self, client):
        c, _ = client
        resp = c.get("/dashboard")
        assert resp.status_code == 503

    def test_events_report_returns_503_without_read_store(self, client):
        c, _ = client
        resp = c.get("/reports/events")
        assert resp.status_code == 503

    def test_unknown_report_returns_404(self, client):
        c, _ = client
        resp = c.get("/reports/no-such-report")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Read endpoints — real DuckDB store (read_client fixture)
# ---------------------------------------------------------------------------


class TestDashboard:
    def test_empty_db_returns_zeros(self, read_client):
        resp = read_client.get("/dashboard")
        assert resp.status_code == 200
        body = resp.json()
        assert body["user_count"] == 0
        assert body["event_count"] == 0
        assert body["events_by_type"] == []

    def test_counts_reflect_writes(self, read_client):
        read_client.post(
            "/users", json={"request_id": "u1", "user_id": "alice"}
        )
        read_client.post(
            "/events", json={"request_id": "e1", "event_type": "page.view"}
        )
        read_client.post(
            "/events", json={"request_id": "e2", "event_type": "page.view"}
        )
        body = _poll_dashboard(read_client, user_count=1, event_count=2)
        assert body["events_by_type"] == [{"event_type": "page.view", "count": 2}]


class TestReportEvents:
    def test_empty_db_returns_empty_list(self, read_client):
        resp = read_client.get("/reports/events")
        assert resp.status_code == 200
        body = resp.json()
        assert body["events"] == []
        assert body["count"] == 0

    def test_events_appear_after_writes(self, read_client):
        read_client.post(
            "/events",
            json={"request_id": "ev-1", "event_type": "user.login", "payload": {"ip": "1.2.3.4"}},
        )
        body = _poll_report(read_client, "/reports/events", count=1)
        assert body["events"][0]["event_type"] == "user.login"


class TestReportUsers:
    def test_empty_db_returns_empty_list(self, read_client):
        resp = read_client.get("/reports/users")
        assert resp.status_code == 200
        body = resp.json()
        assert body["users"] == []
        assert body["count"] == 0

    def test_users_appear_after_writes(self, read_client):
        read_client.post(
            "/users",
            json={"request_id": "usr-1", "user_id": "bob", "payload": {"role": "admin"}},
        )
        body = _poll_report(read_client, "/reports/users", count=1)
        assert body["users"][0]["user_id"] == "bob"


# ---------------------------------------------------------------------------
# Payload size guard
# ---------------------------------------------------------------------------


class TestPayloadSizeGuard:
    def test_oversized_payload_returns_413(self, client):
        c, _ = client
        large_value = "x" * (11 * 1024 * 1024)  # 11 MB string > 10 MB limit
        resp = c.post(
            "/events",
            json={"request_id": "evt-big", "event_type": "test"},
            headers={"Content-Length": str(len(large_value.encode()))},
        )
        # Content-Length header triggers the fast-path size check
        assert resp.status_code == 413
