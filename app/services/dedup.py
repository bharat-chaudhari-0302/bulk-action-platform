"""De-duplication of entities inside a bulk action.

Requirement: identify duplicate entities by their `email` field, process the
first occurrence and record every other occurrence as *skipped* in the logs.

The hard part is that batches run concurrently on many workers, so two copies of
`ada@example.com` can be examined at the same instant in different processes,
and a retried batch must not change its mind about which copy won. An in-process
`set()` gets both cases wrong.

So the database arbitrates. `bulk_action_dedup` has a primary key of
`(bulk_action_id, dedup_key)`, and a single

    INSERT ... ON CONFLICT DO NOTHING RETURNING dedup_key

returns exactly the keys this caller claimed first. Anything absent from the
result was claimed by someone else -- or by this same batch on an earlier
attempt, which is precisely the behaviour a retry needs.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.entities.base import EntityRow
from app.models.bulk_action import BulkActionDedup


def normalise(value: object) -> str | None:
    """Canonical form of a dedup key.

    Emails are compared case-insensitively and whitespace-insensitively, so
    ` Ada@Example.COM ` and `ada@example.com` are the same entity.
    """
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


@dataclass(slots=True)
class DedupResult:
    #: Rows that won their key and should be processed.
    unique_rows: list[EntityRow]
    #: (row, key) pairs that lost, to be logged as skipped.
    duplicates: list[tuple[EntityRow, str]]
    #: Rows whose dedup field was empty; passed through rather than skipped.
    missing_key_rows: list[EntityRow]


async def partition_duplicates(
    session: AsyncSession,
    bulk_action_id: uuid.UUID,
    rows: list[EntityRow],
    dedup_field: str,
) -> DedupResult:
    """Split a batch into first-occurrences and duplicates."""
    keyed: dict[str, list[EntityRow]] = {}
    missing: list[EntityRow] = []

    for row in rows:
        key = normalise(row.get(dedup_field))
        if key is None:
            # A NULL email is not evidence of duplication. Skipping these would
            # silently drop valid entities.
            missing.append(row)
        else:
            keyed.setdefault(key, []).append(row)

    if not keyed:
        return DedupResult(unique_rows=missing, duplicates=[], missing_key_rows=missing)

    # Keys are inserted in sorted order, and that is load-bearing rather than
    # cosmetic. PostgreSQL locks each inserted row as it goes, so two concurrent
    # batches that share keys in opposite orders deadlock: batch A holds
    # 'ada@…' and waits for 'bob@…' while batch B holds 'bob@…' and waits for
    # 'ada@…'. Duplicates routinely span batches, so that overlap is the normal
    # case, not a rare one. Sorting gives every transaction the same lock
    # acquisition order, which makes the cycle impossible to form.
    stmt = (
        insert(BulkActionDedup)
        .values(
            [{"bulk_action_id": bulk_action_id, "dedup_key": k} for k in sorted(keyed)]
        )
        .on_conflict_do_nothing(index_elements=["bulk_action_id", "dedup_key"])
        .returning(BulkActionDedup.dedup_key)
    )
    claimed = {row[0] for row in (await session.execute(stmt)).all()}

    unique_rows: list[EntityRow] = list(missing)
    duplicates: list[tuple[EntityRow, str]] = []

    for key, group in keyed.items():
        if key in claimed:
            # Won the key: the first row in the group is processed, the rest are
            # duplicates *within* this batch.
            unique_rows.append(group[0])
            duplicates.extend((row, key) for row in group[1:])
        else:
            # Another batch (or an earlier attempt of this one) already claimed it.
            duplicates.extend((row, key) for row in group)

    return DedupResult(
        unique_rows=unique_rows, duplicates=duplicates, missing_key_rows=missing
    )
