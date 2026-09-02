"""Shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncIterator

from arq.connections import ArqRedis
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import session_scope
from app.core.redis import get_arq_pool, get_redis


async def db_session() -> AsyncIterator[AsyncSession]:
    async with session_scope() as session:
        yield session


async def arq_pool() -> ArqRedis:
    return await get_arq_pool()


async def redis_client() -> Redis:
    return await get_redis()
