"""Redis connection helpers, shared by the API and the workers."""

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings
from redis.asyncio import Redis

from app.core.config import settings

_arq_pool: ArqRedis | None = None
_redis: Redis | None = None


def redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(settings.redis_url)


async def get_arq_pool() -> ArqRedis:
    """Pool used to enqueue jobs. Reused across requests."""
    global _arq_pool
    if _arq_pool is None:
        _arq_pool = await create_pool(redis_settings())
    return _arq_pool


async def get_redis() -> Redis:
    """Plain Redis client, used by the rate limiter and progress pub/sub."""
    global _redis
    if _redis is None:
        _redis = Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def close_redis() -> None:
    global _arq_pool, _redis
    if _arq_pool is not None:
        await _arq_pool.aclose()
        _arq_pool = None
    if _redis is not None:
        await _redis.aclose()
        _redis = None
