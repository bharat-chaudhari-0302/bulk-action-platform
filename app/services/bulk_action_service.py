"""Application service for bulk actions: the write path and the read models.

Kept out of the route handlers so the same logic is reachable from a worker, a
CLI or a test without going through HTTP.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from arq.connections import ArqRedis
from redis.asyncio import Redis
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ConflictError, NotFoundError, RateLimitError, ValidationError
from app.core.logging import get_logger
from app.domain.actions.registry import get_action
from app.domain.entities.registry import get_entity
from app.models.account import Account
from app.models.bulk_action import BulkAction, BulkActionBatch, BulkActionLog
from app.models.enums import BulkActionStatus, EntityLogStatus
from app.schemas.bulk_action import (
    BulkActionCreate,
    BulkActionStats,
    Page,
)
from app.services.batching import effective_batch_size
from app.services.rate_limiter import RateLimiter

log = get_logger(__name__)


async def _get_account(session: AsyncSession, account_id: uuid.UUID) -> Account:
    account = (
        await session.execute(select(Account).where(Account.id == account_id))
    ).scalar_one_or_none()
    if account is None:
        raise NotFoundError(f"Account '{account_id}' does not exist.")
    return account


async def create_bulk_action(
    session: AsyncSession,
    arq: ArqRedis,
    redis: Redis,
    request: BulkActionCreate,
) -> tuple[BulkAction, bool]:
    """Validate, persist and enqueue a bulk action.

    Returns `(action, created)`; `created` is False when an idempotency key
    replayed an existing submission.

    Validation is exhaustive *before* anything is enqueued: entity, action,
    entity/action compatibility, the action's own payload schema, every update
    field against the entity's declared types, and the filter's fields and
    operators. A request that would fail on the millionth row fails here on the
    zeroth.
    """
    account = await _get_account(session, request.account_id)

    # Replay protection first, so a retried submission never double-charges the
    # rate limiter or double-enqueues work.
    if request.idempotency_key:
        existing = (
            await session.execute(
                select(BulkAction)
                .where(BulkAction.account_id == request.account_id)
                .where(BulkAction.idempotency_key == request.idempotency_key)
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing, False

    entity = get_entity(request.entity_type)
    handler = get_action(request.action_type, request.entity_type)
    config = handler.validate_config(entity, request.payload)

    if request.scheduled_at is not None:
        scheduled_at = request.scheduled_at
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.replace(tzinfo=UTC)
        if scheduled_at <= datetime.now(UTC):
            raise ValidationError("`scheduled_at` must be in the future.")
    else:
        scheduled_at = None

    limiter = RateLimiter(redis)
    decision = await limiter.consume_submission(str(request.account_id))
    if not decision.allowed:
        raise RateLimitError(
            f"Account exceeded {settings.api_rate_limit_per_minute} bulk action "
            f"submissions per minute.",
            retry_after_seconds=decision.retry_after_seconds,
        )

    batch_size = effective_batch_size(request.batch_size, account.rate_limit_per_minute)

    action = BulkAction(
        account_id=request.account_id,
        entity_type=entity.name,
        action_type=handler.action_type,
        status=(
            BulkActionStatus.SCHEDULED.value
            if scheduled_at
            else BulkActionStatus.QUEUED.value
        ),
        # Store the *coerced* config, not the raw request: what the worker reads
        # is exactly what was validated.
        configuration=config.model_dump(mode="json"),
        batch_size=batch_size,
        scheduled_at=scheduled_at,
        idempotency_key=request.idempotency_key,
    )
    session.add(action)
    await session.flush()

    # Enqueue inside the request. `_job_id` makes the enqueue idempotent, and
    # `_defer_until` is what implements scheduling -- no separate cron process.
    await arq.enqueue_job(
        "plan_bulk_action",
        str(action.id),
        _job_id=f"plan:{action.id}",
        _defer_until=scheduled_at,
    )

    log.info(
        "bulk_action_created",
        action_id=str(action.id),
        account_id=str(action.account_id),
        entity_type=action.entity_type,
        action_type=action.action_type,
        scheduled_at=scheduled_at.isoformat() if scheduled_at else None,
    )
    return action, True


async def get_bulk_action(session: AsyncSession, action_id: uuid.UUID) -> BulkAction:
    action = (
        await session.execute(select(BulkAction).where(BulkAction.id == action_id))
    ).scalar_one_or_none()
    if action is None:
        raise NotFoundError(f"Bulk action '{action_id}' does not exist.")
    return action


async def list_bulk_actions(
    session: AsyncSession,
    *,
    account_id: uuid.UUID | None = None,
    status: str | None = None,
    entity_type: str | None = None,
    action_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[BulkAction], int]:
    filters = []
    if account_id:
        filters.append(BulkAction.account_id == account_id)
    if status:
        filters.append(BulkAction.status == status)
    if entity_type:
        filters.append(BulkAction.entity_type == entity_type)
    if action_type:
        filters.append(BulkAction.action_type == action_type)

    stmt = select(BulkAction)
    count_stmt = select(func.count()).select_from(BulkAction)
    for clause in filters:
        stmt = stmt.where(clause)
        count_stmt = count_stmt.where(clause)

    stmt = stmt.order_by(BulkAction.created_at.desc()).limit(limit).offset(offset)
    items = list((await session.execute(stmt)).scalars().all())
    total = (await session.execute(count_stmt)).scalar_one()
    return items, total


async def get_stats(session: AsyncSession, action_id: uuid.UUID) -> BulkActionStats:
    """Summary for GET /bulk-actions/{id}/stats.

    Headline counters come straight off the action row (O(1)); the reason-code
    breakdowns are a grouped scan of the failed/skipped log rows only, which the
    (bulk_action_id, status, id) index serves directly.
    """
    action = await get_bulk_action(session, action_id)

    breakdown_rows = (
        await session.execute(
            select(
                BulkActionLog.status,
                BulkActionLog.reason_code,
                func.count().label("n"),
            )
            .where(BulkActionLog.bulk_action_id == action_id)
            .where(
                BulkActionLog.status.in_(
                    [EntityLogStatus.FAILED.value, EntityLogStatus.SKIPPED.value]
                )
            )
            .group_by(BulkActionLog.status, BulkActionLog.reason_code)
        )
    ).all()

    failure_breakdown: dict[str, int] = {}
    skip_breakdown: dict[str, int] = {}
    for row in breakdown_rows:
        target = (
            failure_breakdown if row.status == EntityLogStatus.FAILED.value else skip_breakdown
        )
        target[row.reason_code or "unspecified"] = row.n

    total = action.total_entities or 0
    duration = None
    if action.started_at:
        end = action.finished_at or datetime.now(UTC)
        duration = max((end - action.started_at).total_seconds(), 0.0)

    throughput = None
    if duration and duration > 0 and action.processed_count:
        throughput = round(action.processed_count / duration * 60, 2)

    progress_percent = round(action.processed_count / total * 100, 2) if total else 0.0
    if total == 0 and action.status in {
        BulkActionStatus.COMPLETED.value,
        BulkActionStatus.COMPLETED_WITH_ERRORS.value,
    }:
        progress_percent = 100.0

    return BulkActionStats(
        id=action.id,
        status=action.status,
        entity_type=action.entity_type,
        action_type=action.action_type,
        total_entities=total,
        processed_count=action.processed_count,
        success_count=action.success_count,
        failure_count=action.failure_count,
        skipped_count=action.skipped_count,
        pending_count=max(total - action.processed_count, 0),
        total_batches=action.total_batches,
        completed_batches=action.completed_batches,
        progress_percent=progress_percent,
        started_at=action.started_at,
        finished_at=action.finished_at,
        duration_seconds=round(duration, 3) if duration is not None else None,
        entities_per_minute=throughput,
        failure_breakdown=failure_breakdown,
        skip_breakdown=skip_breakdown,
    )


async def list_logs(
    session: AsyncSession,
    action_id: uuid.UUID,
    *,
    status: str | None = None,
    reason_code: str | None = None,
    entity_id: uuid.UUID | None = None,
    cursor: int | None = None,
    limit: int = 100,
) -> Page:
    """Keyset-paginated log retrieval.

    `cursor` is the last id seen. OFFSET would degrade linearly as a reviewer
    pages into a million-row log; `id > cursor` stays an index seek.
    """
    await get_bulk_action(session, action_id)

    stmt = select(BulkActionLog).where(BulkActionLog.bulk_action_id == action_id)
    if status:
        stmt = stmt.where(BulkActionLog.status == status)
    if reason_code:
        stmt = stmt.where(BulkActionLog.reason_code == reason_code)
    if entity_id:
        stmt = stmt.where(BulkActionLog.entity_id == entity_id)
    if cursor is not None:
        stmt = stmt.where(BulkActionLog.id > cursor)

    rows = list(
        (await session.execute(stmt.order_by(BulkActionLog.id).limit(limit + 1)))
        .scalars()
        .all()
    )
    has_more = len(rows) > limit
    rows = rows[:limit]
    return Page(
        items=rows,
        count=len(rows),
        next_cursor=str(rows[-1].id) if rows and has_more else None,
        has_more=has_more,
    )


async def list_batches(
    session: AsyncSession, action_id: uuid.UUID, *, limit: int = 200, offset: int = 0
) -> list[BulkActionBatch]:
    await get_bulk_action(session, action_id)
    stmt = (
        select(BulkActionBatch)
        .where(BulkActionBatch.bulk_action_id == action_id)
        .order_by(BulkActionBatch.batch_index)
        .limit(limit)
        .offset(offset)
    )
    return list((await session.execute(stmt)).scalars().all())


async def cancel_bulk_action(session: AsyncSession, action_id: uuid.UUID) -> BulkAction:
    """Cancel a scheduled, queued or in-flight action.

    Batches already committed are left alone -- a bulk action is not a
    transaction, and pretending otherwise would mean holding a million-row lock.
    Workers check the status before each remaining batch, so in-flight work stops
    at the next batch boundary.
    """
    action = await get_bulk_action(session, action_id)
    if not BulkActionStatus(action.status).is_cancellable:
        raise ConflictError(
            f"Bulk action '{action_id}' is already {action.status} and cannot be cancelled."
        )

    result = await session.execute(
        update(BulkAction)
        .where(BulkAction.id == action_id)
        .where(
            BulkAction.status.notin_([s.value for s in BulkActionStatus if s.is_terminal])
        )
        .values(status=BulkActionStatus.CANCELLED.value, finished_at=datetime.now(UTC))
        .returning(BulkAction)
    )
    cancelled = result.scalar_one_or_none()
    if cancelled is None:  # lost a race with a finishing worker
        raise ConflictError(f"Bulk action '{action_id}' completed before it could be cancelled.")
    log.info("bulk_action_cancelled", action_id=str(action_id))
    return cancelled


def registry_snapshot() -> dict[str, Any]:
    """GET /bulk-actions/registry.

    Renders the live registries, so the supported entity x action matrix is
    generated from the code rather than maintained by hand in documentation.
    """
    from app.domain.actions.registry import all_actions
    from app.domain.entities.registry import all_entities

    entities = all_entities()
    actions = all_actions()
    return {
        "entities": [e.describe() for e in entities.values()],
        "actions": [a.describe() for a in actions.values()],
        "supported_combinations": [
            {"entity_type": ename, "action_type": aname}
            for aname, handler in sorted(actions.items())
            for ename in sorted(entities)
            if handler.supports(ename)
        ],
    }
