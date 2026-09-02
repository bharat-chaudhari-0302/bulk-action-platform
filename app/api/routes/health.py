"""Liveness and readiness probes."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import db_session, redis_client
from app.core.config import settings

router = APIRouter(tags=["health"])


@router.get("/healthz", summary="Liveness")
async def healthz() -> dict[str, str]:
    """Process is up. Deliberately touches no dependency: a database blip must
    not cause the orchestrator to kill an otherwise healthy pod."""
    return {"status": "ok", "app": settings.app_name}


@router.get("/readyz", summary="Readiness")
async def readyz(
    response: Response,
    session: Annotated[AsyncSession, Depends(db_session)],
    redis: Annotated[Redis, Depends(redis_client)],
) -> dict[str, Any]:
    """Dependencies are reachable, so this instance can serve traffic."""
    checks: dict[str, Any] = {}
    try:
        await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
    try:
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    healthy = all(v == "ok" for v in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if healthy else "degraded", "checks": checks}
