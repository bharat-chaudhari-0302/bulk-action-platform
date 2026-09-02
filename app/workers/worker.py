"""arq worker entrypoint.

Run with::

    arq app.workers.worker.WorkerSettings

Scaling out is `docker compose up -d --scale worker=4`: every replica pulls from
the same Redis queue, and correctness under concurrency comes from the database
(unique batch keys, atomic counter arithmetic, the de-duplication ledger) rather
than from any assumption about how many workers exist.

Why arq rather than Celery:

* `_defer_until` gives future scheduling natively, with no beat/cron sidecar;
* it is asyncio-native, so it shares an event loop and a connection pool style
  with FastAPI and asyncpg instead of bridging sync and async;
* `_job_id` makes enqueueing idempotent, which is what lets the planner be
  safely re-run.

Celery's per-task `rate_limit` is per-worker and so cannot express "10 000
entities per minute per account" across a fleet; that limit is implemented
against Redis directly (see services/rate_limiter.py) and would have been
hand-rolled either way.
"""

from __future__ import annotations

from typing import Any

from app.core.config import settings
from app.core.db import dispose_engine
from app.core.logging import configure_logging, get_logger
from app.core.redis import close_redis, get_redis, redis_settings
from app.domain.actions.registry import discover_actions
from app.domain.entities.registry import discover_entities
from app.workers.tasks import plan_bulk_action, process_batch

log = get_logger(__name__)


async def startup(ctx: dict[str, Any]) -> None:
    configure_logging()
    # Populate the registries once per process rather than per job.
    discover_entities()
    discover_actions()
    # arq puts its own pool on ctx["redis"]; the rate limiter and progress
    # pub/sub need a plain client with decoded responses.
    ctx["plain_redis"] = await get_redis()
    log.info("worker_started", concurrency=settings.worker_concurrency)


async def shutdown(ctx: dict[str, Any]) -> None:
    await dispose_engine()
    await close_redis()
    log.info("worker_stopped")


class WorkerSettings:
    functions = [plan_bulk_action, process_batch]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = redis_settings()

    #: Jobs executed concurrently in one process. Batches are I/O bound (they
    #: wait on Postgres), so a value well above the CPU count is correct.
    max_jobs = settings.worker_concurrency
    job_timeout = settings.job_timeout_seconds
    max_tries = settings.job_max_tries
    #: Exponential backoff between attempts, so a struggling database is not
    #: hammered by a retry storm.
    retry_jobs = True
    #: Results are small status dicts; keep them briefly for debugging.
    keep_result = 300
    health_check_interval = 30
