"""Per-entity audit logging.

Every entity that a bulk action touches produces exactly one row here, with a
machine-readable `reason_code` so a UI can group failures without parsing
prose. Writes go out as a single multi-row INSERT per batch: a thousand
round trips per batch would dominate the runtime of the batch itself.
"""

from __future__ import annotations

import uuid

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.actions.base import EntityOutcome
from app.models.bulk_action import BulkActionLog


async def write_outcomes(
    session: AsyncSession,
    bulk_action_id: uuid.UUID,
    batch_id: uuid.UUID | None,
    outcomes: list[EntityOutcome],
) -> None:
    if not outcomes:
        return
    await session.execute(
        insert(BulkActionLog),
        [
            {
                "bulk_action_id": bulk_action_id,
                "batch_id": batch_id,
                "entity_id": outcome.entity_id,
                "status": str(outcome.status),
                "reason_code": str(outcome.reason_code) if outcome.reason_code else None,
                "message": outcome.message,
                "details": outcome.details,
            }
            for outcome in outcomes
        ],
    )
