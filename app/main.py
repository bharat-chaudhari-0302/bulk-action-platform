"""FastAPI application factory."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import bulk_actions, crm, health
from app.core.config import settings
from app.core.db import dispose_engine
from app.core.errors import register_exception_handlers
from app.core.logging import configure_logging, get_logger
from app.core.redis import close_redis, get_arq_pool, get_redis
from app.domain.actions.registry import discover_actions
from app.domain.entities.registry import discover_entities

log = get_logger(__name__)

DESCRIPTION = """
A bulk action platform for CRM entities.

**Entity-agnostic by construction.** The core knows about *entity descriptors*
and *action handlers*, never about contacts or updates. `GET /bulk-actions/registry`
renders the live registry: every entity x action combination it lists works, and
the list grows when a module is added under `app/domain/`.

**Submit, then poll.** `POST /bulk-actions` validates exhaustively, persists,
enqueues and returns `202` with an id. Progress is available by polling
`GET /bulk-actions/{id}` or streaming `GET /bulk-actions/{id}/events`.
"""


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    # Populate the registries at boot so an unknown entity/action is a 422 from
    # the first request rather than a lazy import failure mid-flight.
    discover_entities()
    discover_actions()
    await get_arq_pool()
    await get_redis()
    log.info("api_started", environment=settings.environment)
    yield
    await dispose_engine()
    await close_redis()
    log.info("api_stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Bulk Action Platform",
        description=DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        """Bind a request id to every log line emitted while handling it."""
        request_id = request.headers.get("X-Request-ID") or str(time.monotonic_ns())
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id, method=request.method, path=request.url.path
        )
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        log.info(
            "request_completed",
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
        )
        return response

    register_exception_handlers(app)

    app.include_router(health.router)
    app.include_router(bulk_actions.router)
    app.include_router(crm.router)

    @app.get("/", include_in_schema=False)
    async def root() -> dict[str, str]:
        return {
            "name": "Bulk Action Platform",
            "docs": "/docs",
            "registry": "/bulk-actions/registry",
        }

    return app


app = create_app()
