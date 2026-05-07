"""
Route handlers — the HTTP shell around the runtime.

Each handler does exactly five things and nothing else:
  1. Receive and validate the request (Pydantic, size guard)
  2. Translate the request model into a Command
  3. Call runtime.enqueue() or runtime.health_snapshot()
  4. Map the Result to an HTTP response
  5. Return

No business logic. No storage access. No direct queue manipulation.
No framework objects below this layer.

Read endpoints (/dashboard, /reports) return 501 until DuckDB projection
is implemented. Adding them now would require live storage access and
live DuckDB connections — Phase 4 work.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from quackserver.config import (
    DASHBOARD_QUERY_TIMEOUT_MS,
    MAX_PAYLOAD_SIZE_MB,
    REPORT_MAX_ROWS,
    REPORT_QUERY_TIMEOUT_MS,
)
from quackserver.core.commands import Command
from quackserver.core.runtime import Runtime
from quackserver.http.models import (
    AppendEventRequest,
    CreateUserRequest,
    DashboardResponse,
    EventsByTypeItem,
    EventItem,
    HealthResponse,
    ReportEventsResponse,
    ReportUsersResponse,
    UserItem,
    WriteResponse,
)

router = APIRouter()

_MAX_BODY_BYTES = MAX_PAYLOAD_SIZE_MB * 1024 * 1024


def _runtime(request: Request) -> Runtime:
    return request.app.state.runtime


def _result_to_response(result) -> JSONResponse:
    status = "accepted" if result.success else "error"
    if result.success and result.data and result.data.get("status") == "deduplicated":
        status = "deduplicated"
    body = WriteResponse(
        success=result.success,
        status=status,
        request_id=result.request_id,
        error=result.error,
    )
    return JSONResponse(status_code=result.status_code, content=body.model_dump())


async def _check_size(request: Request) -> None:
    """Reject requests exceeding MAX_PAYLOAD_SIZE_MB before body parsing.

    Uses Content-Length for the fast path. Clients that omit Content-Length
    are not rejected here — Pydantic's body parsing imposes its own limit.
    """
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="payload too large")


# ---------------------------------------------------------------------------
# Write endpoints
# ---------------------------------------------------------------------------


@router.post("/events", response_model=WriteResponse)
async def post_events(request: Request, body: AppendEventRequest) -> JSONResponse:
    await _check_size(request)
    cmd = Command(
        request_id=body.request_id,
        command="append_event",
        payload={"event_type": body.event_type, "data": body.payload},
        timestamp=Command.now_utc(),
    )
    result = await _runtime(request).enqueue(cmd)
    return _result_to_response(result)


@router.post("/users", response_model=WriteResponse)
async def post_users(request: Request, body: CreateUserRequest) -> JSONResponse:
    await _check_size(request)
    cmd = Command(
        request_id=body.request_id,
        command="create_user",
        payload={"user_id": body.user_id, "data": body.payload},
        timestamp=Command.now_utc(),
    )
    result = await _runtime(request).enqueue(cmd)
    return _result_to_response(result)


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------

_READ_REPORTS = {"events", "users"}


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(request: Request) -> JSONResponse:
    rt = _runtime(request)
    # _read_store None-check is inside execute_read; lambda is never called if absent.
    result = await rt.execute_read(
        lambda: rt._read_store.dashboard(),
        DASHBOARD_QUERY_TIMEOUT_MS,
    )
    if not result.success:
        raise HTTPException(status_code=result.status_code, detail=result.error)
    d = result.data
    body = DashboardResponse(
        user_count=d["user_count"],
        event_count=d["event_count"],
        events_by_type=[
            EventsByTypeItem(event_type=r["event_type"], count=r["count"])
            for r in d["events_by_type"]
        ],
    )
    return JSONResponse(status_code=200, content=body.model_dump())


@router.get("/reports/{report_id}")
async def get_report(request: Request, report_id: str) -> JSONResponse:
    if report_id not in _READ_REPORTS:
        raise HTTPException(
            status_code=404,
            detail=f"unknown report '{report_id}'; valid: {sorted(_READ_REPORTS)}",
        )
    rt = _runtime(request)
    if report_id == "events":
        result = await rt.execute_read(
            lambda: rt._read_store.events(REPORT_MAX_ROWS),
            REPORT_QUERY_TIMEOUT_MS,
        )
        if not result.success:
            raise HTTPException(status_code=result.status_code, detail=result.error)
        d = result.data
        body = ReportEventsResponse(
            events=[
                EventItem(
                    request_id=e["request_id"],
                    event_type=e["event_type"],
                    data=e["data"],
                    received_at=e["received_at"],
                )
                for e in d["events"]
            ],
            count=d["count"],
        )
        return JSONResponse(status_code=200, content=body.model_dump())

    # report_id == "users"
    result = await rt.execute_read(
        lambda: rt._read_store.users(REPORT_MAX_ROWS),
        REPORT_QUERY_TIMEOUT_MS,
    )
    if not result.success:
        raise HTTPException(status_code=result.status_code, detail=result.error)
    d = result.data
    body = ReportUsersResponse(
        users=[
            UserItem(
                user_id=u["user_id"],
                data=u["data"],
                created_at=u["created_at"],
            )
            for u in d["users"]
        ],
        count=d["count"],
    )
    return JSONResponse(status_code=200, content=body.model_dump())


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health", response_model=HealthResponse)
async def get_health(request: Request) -> JSONResponse:
    snap = _runtime(request).health_snapshot()
    body = HealthResponse(
        healthy=snap.is_healthy,
        state=snap.state.name,
        state_since=snap.state_since,
        queue_depth=snap.queue_depth,
        queue_max=snap.queue_max,
        queue_pct=snap.queue_pct,
        queue_warning=snap.is_queue_warning,
        worker_running=snap.worker_running,
        replay_pending=snap.replay_pending,
        malformed_log_entries=snap.malformed_log_entries,
    )
    status_code = 200 if snap.is_healthy else 503
    return JSONResponse(status_code=status_code, content=body.model_dump())
