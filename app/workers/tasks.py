"""Background tasks: planning and batch execution.

Two tasks make up the whole pipeline.

`plan_bulk_action` runs once per submission and turns a target set into batch
rows plus one job each. `process_batch` runs once per batch and is the only
thing that touches CRM data.

Both are written to be safely re-runnable, because at-least-once delivery is the
only guarantee a queue can actually give:

* batch rows are inserted with ON CONFLICT DO NOTHING against
  `(bulk_action_id, batch_index)`, so re-planning cannot duplicate work;
* totals are written absolutely, never incrementally, so re-planning cannot
  double-count;
* `process_batch` returns immediately if its batch is already `completed`;
* each batch commits its entity writes, its audit logs and its counter deltas in
  one transaction, so a crash leaves no half-applied batch.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.db import session_scope
from app.core.logging import get_logger
from app.domain.actions.base import EntityOutcome
from app.domain.actions.registry import get_action
from app.domain.entities.registry import get_entity
from app.models.account import Account
from app.models.bulk_action import BulkAction, BulkActionBatch
from app.models.enums import BatchStatus, BulkActionStatus, LogReason
from app.services import logs, progress
from app.services.batch_runner import run_batch
from app.services.batching import plan_batches
from app.services.rate_limiter import RateLimiter

log = get_logger(__name__)

# Batch rows are committed in groups before their jobs are enqueued, so a worker
# can never dequeue a job whose row is not yet visible.
_PLAN_FLUSH_EVERY = 100


async def _load_action(session: AsyncSession, action_id: uuid.UUID) -> BulkAction | None:
    return (
        await session.execute(select(BulkAction).where(BulkAction.id == action_id))
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


async def plan_bulk_action(ctx: dict[str, Any], action_id: str) -> dict[str, Any]:
    """Enumerate the target set into batches and enqueue one job per batch."""
    aid = uuid.UUID(action_id)
    arq = ctx["redis"]

    async with session_scope() as session:
        action = await _load_action(session, aid)
        if action is None:
            log.warning("plan_action_missing", action_id=action_id)
            return {"status": "missing"}
        if BulkActionStatus(action.status).is_terminal:
            log.info("plan_skipped_terminal", action_id=action_id, status=action.status)
            return {"status": action.status}

        entity = get_entity(action.entity_type)
        handler = get_action(action.action_type, action.entity_type)
        config = handler.ConfigModel(**action.configuration)

        await session.execute(
            update(BulkAction)
            .where(BulkAction.id == aid)
            .values(status=BulkActionStatus.PLANNING.value, started_at=datetime.now(UTC))
        )

    total_entities = 0
    total_batches = 0
    pending: list[dict[str, Any]] = []

    async def flush(rows: list[dict[str, Any]]) -> None:
        """Commit a group of batch rows, then enqueue their jobs."""
        if not rows:
            return
        async with session_scope() as session:
            await session.execute(
                pg_insert(BulkActionBatch)
                .values(rows)
                .on_conflict_do_nothing(index_elements=["bulk_action_id", "batch_index"])
            )
        for row in rows:
            await arq.enqueue_job(
                "process_batch",
                action_id,
                row["batch_index"],
                _job_id=f"batch:{action_id}:{row['batch_index']}",
            )

    try:
        async with session_scope() as session:
            async for planned in plan_batches(
                session,
                entity,
                action.account_id,
                batch_size=action.batch_size,
                filters=handler.selection_filters(config),
                entity_ids=handler.explicit_ids(config),
                include_deleted=handler.include_deleted,
            ):
                pending.append(
                    {
                        "id": uuid.uuid4(),
                        "bulk_action_id": aid,
                        "batch_index": planned.index,
                        "status": BatchStatus.PENDING.value,
                        "entity_ids": planned.entity_ids,
                        "cursor_start": planned.cursor_start,
                        "cursor_end": planned.cursor_end,
                        "entity_count": planned.entity_count,
                    }
                )
                total_entities += planned.entity_count
                total_batches += 1

                if len(pending) >= _PLAN_FLUSH_EVERY:
                    await flush(pending)
                    pending = []

        await flush(pending)
    except Exception as exc:
        log.exception("plan_failed", action_id=action_id, error=str(exc))
        async with session_scope() as session:
            await progress.fail_action(session, aid, f"Planning failed: {exc}")
        raise

    async with session_scope() as session:
        # Absolute assignment, so a retried planning pass converges instead of
        # accumulating.
        await session.execute(
            update(BulkAction)
            .where(BulkAction.id == aid)
            .values(total_entities=total_entities, total_batches=total_batches)
        )
        if total_batches == 0:
            # Nothing matched. Close it out now rather than leaving it to a
            # batch that will never run.
            await session.execute(
                update(BulkAction)
                .where(BulkAction.id == aid)
                .where(BulkAction.status == BulkActionStatus.PLANNING.value)
                .values(
                    status=BulkActionStatus.COMPLETED.value,
                    finished_at=datetime.now(UTC),
                )
            )
        else:
            await progress.mark_started(session, aid)

    log.info(
        "plan_completed",
        action_id=action_id,
        total_entities=total_entities,
        total_batches=total_batches,
    )
    return {"total_entities": total_entities, "total_batches": total_batches}


# ---------------------------------------------------------------------------
# Batch execution
# ---------------------------------------------------------------------------


async def process_batch(
    ctx: dict[str, Any], action_id: str, batch_index: int, deferrals: int = 0
) -> dict[str, Any]:
    """Execute one batch: rate-limit gate, then the generic pipeline."""
    aid = uuid.UUID(action_id)
    arq = ctx["redis"]
    redis = ctx["plain_redis"]
    job_try: int = ctx.get("job_try", 1)

    async with session_scope() as session:
        action = await _load_action(session, aid)
        if action is None:
            return {"status": "missing"}

        batch = (
            await session.execute(
                select(BulkActionBatch)
                .where(BulkActionBatch.bulk_action_id == aid)
                .where(BulkActionBatch.batch_index == batch_index)
            )
        ).scalar_one_or_none()
        if batch is None:
            log.warning("batch_missing", action_id=action_id, batch_index=batch_index)
            return {"status": "missing"}

        # Idempotency guard: a redelivered job for finished work is a no-op.
        if batch.status == BatchStatus.COMPLETED.value:
            return {"status": "already_completed"}

        if action.status == BulkActionStatus.CANCELLED.value:
            await session.execute(
                update(BulkActionBatch)
                .where(BulkActionBatch.id == batch.id)
                .values(status=BatchStatus.CANCELLED.value, finished_at=datetime.now(UTC))
            )
            return {"status": "cancelled"}

        account = (
            await session.execute(
                select(Account).where(Account.id == action.account_id)
            )
        ).scalar_one()
        limit_per_minute = account.rate_limit_per_minute

        entity_count = batch.entity_count
        batch_id = batch.id

    # --- Rate limit gate --------------------------------------------------
    # Consumed at entity granularity, before any work is done. On denial the
    # job is re-scheduled for when the bucket will have refilled: nothing is
    # burned, nothing is lost, and the retry budget is untouched because this
    # is a fresh job rather than a failure.
    limiter = RateLimiter(redis)
    decision = await limiter.consume_entities(
        str(action.account_id), limit_per_minute=limit_per_minute, amount=entity_count
    )
    if not decision.allowed:
        log.info(
            "batch_rate_limited",
            action_id=action_id,
            batch_index=batch_index,
            retry_after_ms=decision.retry_after_ms,
        )
        await arq.enqueue_job(
            "process_batch",
            action_id,
            batch_index,
            deferrals + 1,
            _job_id=f"batch:{action_id}:{batch_index}:d{deferrals + 1}",
            _defer_by=decision.retry_after_ms / 1000.0,
        )
        return {"status": "rate_limited", "retry_after_ms": decision.retry_after_ms}

    # Record the attempt in its own transaction. Folding it into the work
    # transaction would mean a rollback erases the evidence that the attempt
    # ever happened, leaving a batch that has failed repeatedly reading as
    # `pending, attempts=0`.
    async with session_scope() as session:
        await session.execute(
            update(BulkActionBatch)
            .where(BulkActionBatch.id == batch_id)
            .where(BulkActionBatch.status != BatchStatus.COMPLETED.value)
            .values(
                status=BatchStatus.PROCESSING.value,
                attempts=BulkActionBatch.attempts + 1,
                started_at=datetime.now(UTC),
            )
        )

    # --- Execute ----------------------------------------------------------
    try:
        async with session_scope() as session:
            action = await _load_action(session, aid)
            if action is None:
                # Deleted between the pre-flight check and here.
                return {"status": "missing"}
            batch = (
                await session.execute(
                    select(BulkActionBatch).where(BulkActionBatch.id == batch_id)
                )
            ).scalar_one()
            if batch.status == BatchStatus.COMPLETED.value:
                return {"status": "already_completed"}

            entity = get_entity(action.entity_type)
            handler = get_action(action.action_type, action.entity_type)
            config = handler.ConfigModel(**action.configuration)

            result = await run_batch(session, action, batch, entity, handler, config)

        async with session_scope() as session:
            await progress.finalize_if_complete(session, aid)
            refreshed = await _load_action(session, aid)
            if refreshed is not None:
                await progress.publish_progress(redis, progress.progress_payload(refreshed))

        log.info(
            "batch_completed",
            action_id=action_id,
            batch_index=batch_index,
            success=result.success_count,
            failed=result.failure_count,
            skipped=result.skipped_count,
        )
        return {
            "status": "completed",
            "success": result.success_count,
            "failed": result.failure_count,
            "skipped": result.skipped_count,
        }

    except Exception as exc:
        log.exception(
            "batch_failed",
            action_id=action_id,
            batch_index=batch_index,
            attempt=job_try,
            error=str(exc),
        )
        if job_try < settings.job_max_tries:
            # Let arq retry with exponential backoff. The transaction rolled
            # back, so the batch is untouched.
            raise

        # Retries exhausted. Record the whole batch as failed so the action can
        # still finish: one bad batch must not strand the other 999.
        await _mark_batch_failed(aid, batch_id, entity_count, str(exc))
        async with session_scope() as session:
            await progress.finalize_if_complete(session, aid)
        return {"status": "failed", "error": str(exc)}


async def _mark_batch_failed(
    action_id: uuid.UUID, batch_id: uuid.UUID, entity_count: int, error: str
) -> None:
    """Terminal batch failure: log every entity as failed and advance counters.

    Entity ids are not recorded here because the batch never loaded them --
    claiming to know which entities failed would be a lie. The count is exact,
    which is what keeps `processed_count` able to reach `total_entities`.
    """
    async with session_scope() as session:
        await session.execute(
            update(BulkActionBatch)
            .where(BulkActionBatch.id == batch_id)
            .values(
                status=BatchStatus.FAILED.value,
                failure_count=entity_count,
                error=error[:2000],
                finished_at=datetime.now(UTC),
            )
        )
        await logs.write_outcomes(
            session,
            action_id,
            batch_id,
            [
                EntityOutcome.failed(
                    None,
                    LogReason.BATCH_ERROR,
                    f"Batch failed after {settings.job_max_tries} attempts: {error}"[:1000],
                )
                for _ in range(entity_count)
            ],
        )
        await progress.record_batch_completion(
            session,
            action_id,
            processed=entity_count,
            success=0,
            failure=entity_count,
            skipped=0,
        )
