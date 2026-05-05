"""
FastAPI application factory.

The app knows nothing about storage, queues, or log paths.
It receives a fully-constructed Runtime and hangs it on app.state.

This separation keeps the runtime testable without starting an HTTP server
and keeps the HTTP layer swappable without touching the runtime.

Usage:
    runtime = Runtime(log_path=..., dedup_path=..., checkpoint_path=..., projection=...)
    app = create_app(runtime)
    uvicorn.run(app, host="0.0.0.0", port=8000)
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from quackserver.core.runtime import Runtime
from quackserver.core.structured_log import get_logger
from quackserver.http.routes import router

_log = get_logger(__name__)


def create_app(runtime: Runtime) -> FastAPI:
    """Return a FastAPI application wired to the given Runtime.

    Lifespan events start and stop the runtime cleanly. The HTTP server
    will not accept requests until runtime.start() returns (recovery done).
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        _log.info("http_server_starting")
        await runtime.start()
        _log.info("http_server_ready")
        yield
        _log.info("http_server_stopping")
        await runtime.stop()
        _log.info("http_server_stopped")

    app = FastAPI(
        title="quackserver",
        description="Constrained DuckDB application server",
        lifespan=_lifespan,
    )
    app.state.runtime = runtime
    app.include_router(router)
    return app
