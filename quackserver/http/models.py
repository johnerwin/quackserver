"""
HTTP request and response models — Pydantic schemas for the HTTP layer only.

These types never cross the module boundary into core runtime code.
Route handlers translate them into Command objects before touching the runtime.
The runtime never imports from this module.

This is where transport concerns live: field naming, JSON serialisation,
OpenAPI documentation strings. None of that belongs in Command or Result.
"""

from typing import Any

from pydantic import BaseModel, Field


class WriteRequest(BaseModel):
    """Base for all write command requests.

    request_id is required on every write. Missing or blank → 400.
    The server enforces this structurally; clients must generate a UUID or
    ULID before sending (spec §8.1).
    """

    request_id: str = Field(
        ...,
        description="Client-generated idempotency key (UUID or ULID). Required.",
        min_length=1,
    )


class AppendEventRequest(WriteRequest):
    """POST /events — append an immutable event to the event store."""

    event_type: str = Field(
        ...,
        description="Event type identifier (e.g. 'user.signup', 'page.view').",
        min_length=1,
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary event metadata. Schema is caller-defined.",
    )


class CreateUserRequest(WriteRequest):
    """POST /users — create or register a user record."""

    user_id: str = Field(
        ...,
        description="Application-assigned user identifier.",
        min_length=1,
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary user metadata.",
    )


class WriteResponse(BaseModel):
    """Uniform response for all write commands.

    success=True, status='accepted': durably written to log; projection pending.
    success=True, status='deduplicated': prior result returned from cache.
    success=False: error details in the error field; status_code reflects type.
    """

    success: bool
    status: str
    request_id: str | None = None
    error: str | None = None


class HealthResponse(BaseModel):
    """Response for GET /health."""

    healthy: bool
    state: str
    state_since: str
    queue_depth: int
    queue_max: int
    queue_pct: float
    queue_warning: bool
    worker_running: bool
    replay_pending: int
    malformed_log_entries: int


class EventsByTypeItem(BaseModel):
    event_type: str
    count: int


class DashboardResponse(BaseModel):
    """Response for GET /dashboard."""

    user_count: int
    event_count: int
    events_by_type: list[EventsByTypeItem]


class EventItem(BaseModel):
    request_id: str
    event_type: str
    data: dict[str, Any]
    received_at: str


class UserItem(BaseModel):
    user_id: str
    data: dict[str, Any]
    created_at: str


class ReportEventsResponse(BaseModel):
    """Response for GET /reports/events."""

    events: list[EventItem]
    count: int


class ReportUsersResponse(BaseModel):
    """Response for GET /reports/users."""

    users: list[UserItem]
    count: int
