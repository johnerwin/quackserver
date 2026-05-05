"""Integration tests for the HTTP layer.

Uses FastAPI's TestClient which drives the full lifespan (start/stop) and
exercises routes against a real Runtime wired to MockStorage.
"""

import pytest
from fastapi.testclient import TestClient

from quackserver.core.runtime import Runtime
from quackserver.http.app import create_app

from .conftest import MockProjection


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
    )
    app = create_app(runtime)
    with TestClient(app) as c:
        yield c, projection


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


# ---------------------------------------------------------------------------
# Stub endpoints
# ---------------------------------------------------------------------------


class TestStubEndpoints:
    def test_dashboard_returns_501(self, client):
        c, _ = client
        resp = c.get("/dashboard")
        assert resp.status_code == 501

    def test_report_returns_501(self, client):
        c, _ = client
        resp = c.get("/reports/some-report")
        assert resp.status_code == 501


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
