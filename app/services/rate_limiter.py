"""Per-account token-bucket rate limiting, backed by Redis.

Two limits are enforced:

* **Processing** -- entities/minute per account (default 10 000, as the
  assignment specifies, overridable per account). Consumed by workers *before*
  a batch runs, at entity granularity, so the limit caps real throughput rather
  than merely the number of API calls.
* **Submission** -- bulk actions/minute per account, guarding the write path.

A token bucket rather than a fixed window because a fixed window lets an account
burn 10 000 entities at 11:59:59 and another 10 000 at 12:00:00 -- 20 000 inside
one second. The bucket refills continuously, so the sustained rate is the limit.

The check-and-consume must be atomic across many worker processes, so it runs as
a Lua script inside Redis: one round trip, no read-modify-write race.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from redis.asyncio import Redis

from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)

# KEYS[1] = bucket key
# ARGV    = capacity, refill_per_second, now_ms, requested
_TOKEN_BUCKET_LUA = """
local capacity  = tonumber(ARGV[1])
local refill    = tonumber(ARGV[2])
local now_ms    = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

if tokens == nil or ts == nil then
  tokens = capacity
  ts = now_ms
end

-- Refill for the time that has passed, capped at the bucket size.
local elapsed_s = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed_s * refill)

local allowed = 0
local retry_after = 0.0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retry_after = (requested - tokens) / refill
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
-- Idle buckets expire once they would have refilled completely.
redis.call('PEXPIRE', KEYS[1], math.ceil((capacity / refill) * 1000) + 60000)

return {allowed, tostring(retry_after), tostring(tokens)}
"""


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: float
    tokens_remaining: float

    @property
    def retry_after_ms(self) -> int:
        # Round up and add a small cushion so a retry does not land a
        # microsecond early and bounce again.
        return int(self.retry_after_seconds * 1000) + 50


class RateLimiter:
    """Token bucket over Redis. One instance per process is enough."""

    def __init__(self, redis: Redis, key_prefix: str = "rl") -> None:
        self._redis = redis
        self._prefix = key_prefix
        self._script = redis.register_script(_TOKEN_BUCKET_LUA)

    def _key(self, scope: str, identity: str) -> str:
        return f"{self._prefix}:{scope}:{identity}"

    async def consume(
        self, scope: str, identity: str, *, limit_per_minute: int, amount: int = 1
    ) -> RateLimitDecision:
        """Try to take `amount` tokens. Never blocks."""
        if amount <= 0:
            return RateLimitDecision(True, 0.0, float(limit_per_minute))
        if amount > limit_per_minute:
            # Unsatisfiable no matter how long we wait. Callers size their
            # batches against the limit (see BatchPlanner) so this is a
            # configuration error, not a runtime condition.
            raise ValueError(
                f"Cannot consume {amount} tokens from a bucket of {limit_per_minute}."
            )
        refill_per_second = limit_per_minute / 60.0
        allowed, retry_after, remaining = await self._script(
            keys=[self._key(scope, identity)],
            args=[limit_per_minute, refill_per_second, int(time.time() * 1000), amount],
        )
        return RateLimitDecision(
            allowed=bool(int(allowed)),
            retry_after_seconds=float(retry_after),
            tokens_remaining=float(remaining),
        )

    async def consume_entities(
        self, account_id: str, *, limit_per_minute: int, amount: int
    ) -> RateLimitDecision:
        """Processing limit: entities/minute for an account."""
        return await self.consume(
            "entities", account_id, limit_per_minute=limit_per_minute, amount=amount
        )

    async def consume_submission(self, account_id: str) -> RateLimitDecision:
        """Submission limit: bulk actions/minute for an account."""
        return await self.consume(
            "submit",
            account_id,
            limit_per_minute=settings.api_rate_limit_per_minute,
            amount=1,
        )

    async def peek(self, scope: str, identity: str, *, limit_per_minute: int) -> float:
        """Tokens currently available, without consuming any."""
        tokens, ts = await self._redis.hmget(self._key(scope, identity), ["tokens", "ts"])
        if tokens is None or ts is None:
            return float(limit_per_minute)
        elapsed_s = max(0.0, time.time() * 1000 - float(ts)) / 1000.0
        return min(float(limit_per_minute), float(tokens) + elapsed_s * limit_per_minute / 60.0)
