"""Demo data generation.

Entity-agnostic: values are derived from the descriptor's declared field types
and names, so seeding a newly added entity needs no new code here.

Rows are written with PostgreSQL's binary COPY protocol where available --
inserting a few hundred thousand rows one statement at a time would make seeding
slower than the bulk action being demonstrated. A portable executemany path is
kept as a fallback so the seeder still works against any connection.
"""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.domain.entities.base import EntityDescriptor

log = get_logger(__name__)

_CHUNK = 5_000

_FIRST_NAMES = [
    "Ada", "Grace", "Alan", "Linus", "Ravi", "Meera", "Kiran", "Anita",
    "Rahul", "Priya", "Sofia", "Diego", "Yuki", "Chen", "Omar", "Fatima",
]
_LAST_NAMES = [
    "Lovelace", "Hopper", "Turing", "Torvalds", "Sharma", "Iyer", "Patel",
    "Desai", "Nair", "Rao", "Garcia", "Silva", "Tanaka", "Wei", "Haddad",
]
_STATUSES = ["active", "inactive", "prospect", "churned"]
_INDUSTRIES = ["logistics", "saas", "manufacturing", "retail", "fintech", "healthcare"]
_DOMAINS = ["example.com", "acme.io", "globex.net", "initech.dev", "umbrella.co"]


def _value_for(field_name: str, spec_type: str, rng: random.Random, index: int) -> Any:
    """Plausible value for a field, chosen by name first and type second."""
    if field_name == "email":
        return f"{rng.choice(_FIRST_NAMES).lower()}.{index}@{rng.choice(_DOMAINS)}"
    if field_name == "domain":
        return f"company-{index}.{rng.choice(['com', 'io', 'net'])}"
    if field_name == "name":
        return f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"
    if field_name == "status":
        return rng.choice(_STATUSES)
    if field_name == "industry":
        return rng.choice(_INDUSTRIES)
    if spec_type == "int":
        return rng.randint(18, 75)
    return f"{field_name}-{index}"


def build_rows(
    entity: type[EntityDescriptor],
    account_id: uuid.UUID,
    count: int,
    duplicate_ratio: float = 0.0,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """Generate `count` rows, reusing dedup keys for `duplicate_ratio` of them."""
    rng = random.Random(seed)
    now = datetime.now(UTC)
    dedup_field = next(iter(entity.dedup_fields), None)
    minted_keys: list[Any] = []
    rows: list[dict[str, Any]] = []

    for i in range(count):
        row: dict[str, Any] = {"id": uuid.uuid4(), "account_id": account_id}
        for fname, spec in entity.updatable_fields.items():
            row[fname] = _value_for(fname, spec.type_name, rng, i)

        # Reuse an earlier key so de-duplication has something to detect.
        if dedup_field and minted_keys and rng.random() < duplicate_ratio:
            row[dedup_field] = rng.choice(minted_keys)
        elif dedup_field:
            minted_keys.append(row[dedup_field])

        if "created_at" in entity.table.c:
            row["created_at"] = now
        if "updated_at" in entity.table.c:
            row["updated_at"] = now
        if entity.soft_delete_column:
            row[entity.soft_delete_column] = None
        rows.append(row)

    return rows


async def _copy_rows(
    session: AsyncSession, entity: type[EntityDescriptor], rows: list[dict[str, Any]]
) -> bool:
    """Fast path: asyncpg binary COPY. Returns False if unavailable."""
    try:
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        driver = getattr(raw, "driver_connection", None)
        if driver is None or not hasattr(driver, "copy_records_to_table"):
            return False
        columns = list(rows[0].keys())
        await driver.copy_records_to_table(
            entity.table.name,
            records=[tuple(row[c] for c in columns) for row in rows],
            columns=columns,
        )
        return True
    except Exception as exc:
        log.warning("copy_unavailable_falling_back", error=str(exc))
        return False


async def seed_entities(
    session: AsyncSession,
    entity: type[EntityDescriptor],
    account_id: uuid.UUID,
    *,
    count: int,
    duplicate_ratio: float = 0.0,
    seed: int | None = None,
) -> int:
    inserted = 0
    for start in range(0, count, _CHUNK):
        chunk = build_rows(
            entity,
            account_id,
            min(_CHUNK, count - start),
            duplicate_ratio=duplicate_ratio,
            seed=None if seed is None else seed + start,
        )
        if not await _copy_rows(session, entity, chunk):
            await session.execute(insert(entity.table), chunk)
        inserted += len(chunk)
    log.info("seeded", entity=entity.name, count=inserted, account_id=str(account_id))
    return inserted
