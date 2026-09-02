"""Integration fixtures: wire the application's global engine/redis to the test
instances, then drive the real worker tasks as plain coroutines.

The tasks under test are the production ones -- `plan_bulk_action` and
`process_batch` are imported, not reimplemented -- so these tests cover the
actual planning, rate-limiting, de-duplication, logging and finalisation paths.
Only the queue is replaced, and only so that execution order is deterministic.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core import db as core_db
from app.core import redis as core_redis
from app.schemas.bulk_action import BulkActionCreate
from app.services import bulk_action_service as svc
from app.workers.tasks import plan_bulk_action, process_batch


@pytest_asyncio.fixture(autouse=True)
async def wire_globals(engine, redis):
    """Point `session_scope()` and the shared Redis client at the test instances."""
    core_db._engine = engine
    core_db._session_factory = async_sessionmaker(
        engine, expire_on_commit=False, class_=core_db.AsyncSession
    )
    core_redis._redis = redis
    yield
    core_db._engine = None
    core_db._session_factory = None
    core_redis._redis = None


class Driver:
    """Runs a submitted action to completion, one batch at a time."""

    def __init__(self, arq, redis) -> None:
        self.arq = arq
        self.redis = redis
        self.ctx: dict[str, Any] = {"redis": arq, "plain_redis": redis, "job_try": 1}

    async def submit(self, session, **kwargs) -> Any:
        action, _ = await svc.create_bulk_action(
            session, self.arq, self.redis, BulkActionCreate(**kwargs)
        )
        await session.commit()
        return action

    async def plan(self, action_id: uuid.UUID) -> dict[str, Any]:
        return await plan_bulk_action(self.ctx, str(action_id))

    async def run_batches(self, max_rounds: int = 50) -> list[dict[str, Any]]:
        """Drain the queue, honouring re-enqueues from the rate limiter."""
        results = []
        for _ in range(max_rounds):
            jobs = self.arq.pop("process_batch")
            if not jobs:
                break
            for job in jobs:
                results.append(await process_batch(self.ctx, *job["args"]))
        return results

    async def run(self, action_id: uuid.UUID) -> list[dict[str, Any]]:
        await self.plan(action_id)
        return await self.run_batches()


@pytest.fixture
def driver(arq, redis) -> Driver:
    return Driver(arq, redis)
