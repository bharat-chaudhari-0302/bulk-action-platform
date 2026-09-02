"""Progress accounting and lifecycle transitions.

Counters live denormalised on `bulk_actions` and are moved with in-place
arithmetic (`SET n = n + :delta`) inside the same transaction that writes the
batch's logs. Two properties follow:

* concurrent workers cannot lose an update, because no worker ever reads a
  counter in order to write it;
* `GET /bulk-actions/{id}/stats` is a single-row read instead of an aggregate
  over a log table that may hold millions of rows.

The action is finalised by whichever worker's increment makes
`completed_batches` reach `total_batches`. That check happens inside the same
atomic UPDATE, so exactly one worker sees the transition -- there is no separate
"am I last?" race.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models.bulk_action import BulkAction
from app.models.enums import BulkActionStatus

log = get_logger(__name__)

PROGRESS_CHANNEL = "bulk-action-progress"


async def mark_started(session: AsyncSession, action_id: uuid.UUID) -> None:
    """Move a queued/scheduled action into `processing`, once."""
    await session.execute(
        update(BulkAction)
        .where(BulkAction.id == action_id)
        .where(
            BulkAction.status.in_(
                [
                    BulkActionStatus.QUEUED.value,
                    BulkActionStatus.SCHEDULED.value,
                    BulkActionStatus.PLANNING.value,
                ]
            )
        )
        .values(status=BulkActionStatus.PROCESSING.value, started_at=datetime.now(UTC))
    )


async def record_batch_completion(
    session: AsyncSession,
    action_id: uuid.UUID,
    *,
    processed: int,
    success: int,
    failure: int,
    skipped: int,
    batch_done: bool = True,
) -> BulkAction | None:
    """Apply one batch's counts and return the refreshed action row."""
    stmt = (
        update(BulkAction)
        .where(BulkAction.id == action_id)
        .values(
            processed_count=BulkAction.processed_count + processed,
            success_count=BulkAction.success_count + success,
            failure_count=BulkAction.failure_count + failure,
            skipped_count=BulkAction.skipped_count + skipped,
            completed_batches=BulkAction.completed_batches + (1 if batch_done else 0),
        )
        .returning(BulkAction)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def finalize_if_complete(
    session: AsyncSession, action_id: uuid.UUID
) -> BulkActionStatus | None:
    """Close the action if every batch has reported. Returns the new status.

    The WHERE clause carries the completeness test, so only the transaction that
    actually performs the transition gets a row back.
    """
    final_status = (
        select(
            BulkAction.id,
            BulkAction.failure_count,
        )
        .where(BulkAction.id == action_id)
        .where(BulkAction.completed_batches >= BulkAction.total_batches)
        .where(BulkAction.status == BulkActionStatus.PROCESSING.value)
    )
    row = (await session.execute(final_status)).first()
    if row is None:
        return None

    status = (
        BulkActionStatus.COMPLETED_WITH_ERRORS
        if row.failure_count > 0
        else BulkActionStatus.COMPLETED
    )
    result = await session.execute(
        update(BulkAction)
        .where(BulkAction.id == action_id)
        .where(BulkAction.status == BulkActionStatus.PROCESSING.value)
        .where(BulkAction.completed_batches >= BulkAction.total_batches)
        .values(status=status.value, finished_at=datetime.now(UTC))
        .returning(BulkAction.id)
    )
    return status if result.first() is not None else None


async def fail_action(session: AsyncSession, action_id: uuid.UUID, error: str) -> None:
    """Terminal failure, used when the action cannot run at all."""
    await session.execute(
        update(BulkAction)
        .where(BulkAction.id == action_id)
        .where(BulkAction.status.notin_([s.value for s in BulkActionStatus if s.is_terminal]))
        .values(
            status=BulkActionStatus.FAILED.value,
            error=error[:2000],
            finished_at=datetime.now(UTC),
        )
    )


async def is_cancelled(session: AsyncSession, action_id: uuid.UUID) -> bool:
    """Checked before each batch, so a cancel takes effect mid-flight."""
    status = (
        await session.execute(select(BulkAction.status).where(BulkAction.id == action_id))
    ).scalar_one_or_none()
    return status == BulkActionStatus.CANCELLED.value


def progress_payload(action: BulkAction) -> dict:
    total = action.total_entities or 0
    processed = action.processed_count or 0
    return {
        "id": str(action.id),
        "status": action.status,
        "total_entities": total,
        "processed_count": processed,
        "success_count": action.success_count,
        "failure_count": action.failure_count,
        "skipped_count": action.skipped_count,
        "total_batches": action.total_batches,
        "completed_batches": action.completed_batches,
        "progress_percent": round(processed / total * 100, 2) if total else 0.0,
    }


async def publish_progress(redis: Redis, payload: dict) -> None:
    """Push an update to any SSE subscriber.

    Fire-and-forget: progress streaming is a convenience, and a Redis hiccup
    must never fail a batch that has already committed its work.
    """
    try:
        await redis.publish(f"{PROGRESS_CHANNEL}:{payload['id']}", json.dumps(payload))
    except Exception as exc:  # pragma: no cover - best effort
        log.warning("progress_publish_failed", error=str(exc), action_id=payload.get("id"))
