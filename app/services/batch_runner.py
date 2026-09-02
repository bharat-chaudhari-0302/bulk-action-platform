"""Execution of a single batch.

This is the one place where the generic pipeline lives, and it is identical for
every action:

    cancelled?  ->  load slice  ->  de-duplicate  ->  handler.execute()
                ->  write logs  ->  advance counters  ->  finalise if last

Everything runs in one transaction, so a batch either fully lands -- entity
writes, audit rows and counters together -- or leaves no trace and is retried.
That is what makes the retry safe: there is no window in which the entities were
updated but the logs and counters were not.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.domain.actions.base import (
    ActionContext,
    BatchResult,
    BulkActionHandler,
    EntityOutcome,
)
from app.domain.entities.base import EntityDescriptor, EntityRow
from app.domain.filters import selection_clause
from app.domain.sql_utils import pk_in
from app.models.bulk_action import BulkAction, BulkActionBatch
from app.models.enums import BatchStatus, LogReason
from app.services import logs, progress
from app.services.dedup import partition_duplicates

log = get_logger(__name__)


async def load_batch_rows(
    session: AsyncSession,
    entity: type[EntityDescriptor],
    account_id: uuid.UUID,
    batch: BulkActionBatch,
    filters: dict | None,
    include_deleted: bool,
) -> list[EntityRow]:
    """Materialise the entities this batch is responsible for.

    Always tenant-scoped. For a keyset-range batch the live filter is re-applied
    inside the range, so an entity that stopped matching between planning and
    execution is not silently updated.
    """
    columns = [entity.column(name) for name in entity.readable_columns()]
    pk = entity.pk()

    if batch.entity_ids:
        where = and_(
            pk_in(pk, batch.entity_ids),
            entity.account_col() == account_id,
        )
        if entity.soft_delete_column and not include_deleted:
            where = and_(where, entity.column(entity.soft_delete_column).is_(None))
    else:
        where = and_(
            selection_clause(entity, account_id, filters, include_deleted),
            pk >= batch.cursor_start,
            pk <= batch.cursor_end,
        )

    result = await session.execute(select(*columns).where(where).order_by(pk))
    names = entity.readable_columns()
    return [
        EntityRow(id=row[0], values=dict(zip(names, row, strict=True)))
        for row in result.all()
    ]


async def run_batch(
    session: AsyncSession,
    action: BulkAction,
    batch: BulkActionBatch,
    entity: type[EntityDescriptor],
    handler: BulkActionHandler,
    config,
) -> BatchResult:
    """Execute one batch inside the caller's transaction."""
    result = BatchResult()

    filters = handler.selection_filters(config)
    rows = await load_batch_rows(
        session, entity, action.account_id, batch, filters, handler.include_deleted
    )

    # Entities counted at plan time that are no longer in the target set. We know
    # how many vanished but not which, so these are logged without an entity id
    # rather than being dropped -- otherwise processed_count could never reach
    # total_entities and the action would appear to stall at 99%.
    drift = batch.entity_count - len(rows)
    if drift > 0:
        result.outcomes.extend(
            EntityOutcome.skipped(
                None,
                LogReason.LEFT_TARGET_SET,
                "Entity was deleted or stopped matching the filter between planning "
                "and execution.",
            )
            for _ in range(drift)
        )

    # --- De-duplication ---------------------------------------------------
    dedup_field = handler.dedup_field(entity, config)
    if dedup_field and rows:
        dedup = await partition_duplicates(session, action.id, rows, dedup_field)
        rows = dedup.unique_rows
        reason = (
            LogReason.DUPLICATE_EMAIL if dedup_field == "email" else LogReason.DUPLICATE_KEY
        )
        result.outcomes.extend(
            EntityOutcome.skipped(
                row.id,
                reason,
                f"Duplicate {dedup_field} within this bulk action; "
                f"the first occurrence was processed.",
                **{dedup_field: key},
            )
            for row, key in dedup.duplicates
        )

    # --- The action itself ------------------------------------------------
    ctx = ActionContext(
        session=session,
        entity=entity,
        account_id=action.account_id,
        bulk_action_id=action.id,
        batch_id=batch.id,
        config=config,
    )
    result.extend((await handler.execute(ctx, rows)).outcomes)

    # --- Persist ----------------------------------------------------------
    await logs.write_outcomes(session, action.id, batch.id, result.outcomes)

    await session.execute(
        update(BulkActionBatch)
        .where(BulkActionBatch.id == batch.id)
        .values(
            status=BatchStatus.COMPLETED.value,
            success_count=result.success_count,
            failure_count=result.failure_count,
            skipped_count=result.skipped_count,
            finished_at=datetime.now(UTC),
            error=None,
        )
    )

    await progress.record_batch_completion(
        session,
        action.id,
        processed=result.processed_count,
        success=result.success_count,
        failure=result.failure_count,
        skipped=result.skipped_count,
    )
    return result
