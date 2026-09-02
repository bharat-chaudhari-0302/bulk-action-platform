"""Batch planning: turning a target set into units of work.

A million-entity action must never be materialised in memory, and enqueueing a
million jobs would make Redis the bottleneck. So the planner walks the target
set with **keyset pagination** -- `WHERE id > :last ORDER BY id LIMIT :n`, an
index scan whose cost does not grow with offset -- and emits one job per batch
of `batch_size` entities. Peak memory is one page of ids regardless of the
target size.

Each batch is stored as an inclusive `[cursor_start, cursor_end]` id range
rather than an id list, so a batch row costs a constant ~100 bytes. The range is
re-resolved against the live filter when the batch runs; entities that left the
target set in the meantime are reported rather than silently miscounted.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.domain.entities.base import EntityDescriptor
from app.domain.filters import selection_clause

log = get_logger(__name__)


@dataclass(slots=True)
class PlannedBatch:
    index: int
    entity_count: int
    entity_ids: list[uuid.UUID] | None = None
    cursor_start: uuid.UUID | None = None
    cursor_end: uuid.UUID | None = None


def effective_batch_size(requested: int | None, rate_limit_per_minute: int) -> int:
    """Clamp the batch size to something both useful and satisfiable.

    A batch consumes its entity count from the account's token bucket in one go,
    so a batch larger than the per-minute limit could never be admitted. Clamping
    here keeps that impossible.
    """
    size = requested or settings.batch_size
    size = max(1, min(size, settings.max_batch_size))
    return max(1, min(size, rate_limit_per_minute))


async def plan_batches(
    session: AsyncSession,
    entity: type[EntityDescriptor],
    account_id: uuid.UUID,
    *,
    batch_size: int,
    filters: dict | None = None,
    entity_ids: list[uuid.UUID] | None = None,
    include_deleted: bool = False,
) -> AsyncIterator[PlannedBatch]:
    """Yield batches covering the target set, in id order."""
    if entity_ids is not None:
        async for batch in _plan_from_ids(
            session, entity, account_id, entity_ids, batch_size, include_deleted
        ):
            yield batch
    else:
        async for batch in _plan_from_filter(
            session, entity, account_id, filters, batch_size, include_deleted
        ):
            yield batch


async def _plan_from_filter(
    session: AsyncSession,
    entity: type[EntityDescriptor],
    account_id: uuid.UUID,
    filters: dict | None,
    batch_size: int,
    include_deleted: bool,
) -> AsyncIterator[PlannedBatch]:
    where = selection_clause(entity, account_id, filters, include_deleted)
    pk = entity.pk()
    last_id: uuid.UUID | None = None
    index = 0

    while True:
        stmt = select(pk).where(where).order_by(pk).limit(batch_size)
        if last_id is not None:
            stmt = stmt.where(pk > last_id)

        ids = [row[0] for row in (await session.execute(stmt)).all()]
        if not ids:
            return

        yield PlannedBatch(
            index=index,
            entity_count=len(ids),
            cursor_start=ids[0],
            cursor_end=ids[-1],
        )
        index += 1
        last_id = ids[-1]

        if len(ids) < batch_size:
            return


async def _plan_from_ids(
    session: AsyncSession,
    entity: type[EntityDescriptor],
    account_id: uuid.UUID,
    entity_ids: list[uuid.UUID],
    batch_size: int,
    include_deleted: bool,
) -> AsyncIterator[PlannedBatch]:
    """Explicit id selection.

    The ids are stored on the batch row verbatim so the target set is frozen at
    submission time -- a client that named specific entities should get exactly
    those entities, not whatever currently matches.

    Ids are not validated against the account here; that would cost a query per
    batch at plan time. The row load at execution time is tenant-scoped, so an
    id belonging to another account simply returns no row and is reported as
    `entity_not_found` -- it can never be read or written across tenants.
    """
    # De-duplicate while preserving order, then sort so batches stay id-ordered.
    seen: set[uuid.UUID] = set()
    ordered = [i for i in entity_ids if not (i in seen or seen.add(i))]

    for index, start in enumerate(range(0, len(ordered), batch_size)):
        chunk = sorted(ordered[start : start + batch_size])
        yield PlannedBatch(
            index=index,
            entity_count=len(chunk),
            entity_ids=chunk,
            cursor_start=chunk[0],
            cursor_end=chunk[-1],
        )
