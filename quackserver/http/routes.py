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

from quackserver.config import MAX_PAYLOAD_SIZE_MB
from quackserver.core.commands import Command
from quackserver.core.runtime import Runtime
from quackserver.http.models import (
    AppendEventRequest,
    CreateUserRequest,
    HealthResponse,
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
# Read endpoints — stubs until DuckDB projection is implemented (Phase 4)
# ---------------------------------------------------------------------------


@router.get("/dashboard")
async def get_dashboard(request: Request) -> JSONResponse:
    raise HTTPException(status_code=501, detail="not implemented — DuckDB projection pending")


@router.get("/reports/{report_id}")
async def get_report(request: Request, report_id: str) -> JSONResponse:
    raise HTTPException(status_code=501, detail="not implemented — DuckDB projection pending")


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
