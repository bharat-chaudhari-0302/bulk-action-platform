"""Seed demo data.

Writes straight to Postgres with COPY rather than going through the API: the
point of seeding is to have data ready, not to benchmark the insert path.

    python scripts/seed.py --count 200000
    python scripts/seed.py --entity company --count 50000 --duplicate-ratio 0.3
"""

from __future__ import annotations

import argparse
import asyncio
import time
import uuid

from sqlalchemy import select

from app.core.db import dispose_engine, session_scope
from app.core.logging import configure_logging
from app.domain.entities.registry import discover_entities, get_entity
from app.models.account import Account
from app.services.seeding import seed_entities


async def _resolve_account(session, account_id: str | None, name: str) -> Account:
    if account_id:
        account = (
            await session.execute(select(Account).where(Account.id == uuid.UUID(account_id)))
        ).scalar_one_or_none()
        if account is None:
            raise SystemExit(f"Account {account_id} not found.")
        return account

    account = (
        await session.execute(select(Account).where(Account.name == name))
    ).scalar_one_or_none()
    if account is None:
        account = Account(name=name)
        session.add(account)
        await session.flush()
    return account


async def main() -> None:
    parser = argparse.ArgumentParser(description="Seed CRM demo data.")
    parser.add_argument("--entity", default="contact", help="Registered entity name.")
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument(
        "--duplicate-ratio",
        type=float,
        default=0.0,
        help="Fraction of rows reusing an earlier de-duplication key (0.0-0.9).",
    )
    parser.add_argument("--account-id", default=None)
    parser.add_argument("--account-name", default="Demo Account")
    args = parser.parse_args()

    configure_logging()
    discover_entities()
    entity = get_entity(args.entity)

    started = time.perf_counter()
    async with session_scope() as session:
        account = await _resolve_account(session, args.account_id, args.account_name)
        account_id = account.id
        inserted = await seed_entities(
            session,
            entity,
            account_id,
            count=args.count,
            duplicate_ratio=args.duplicate_ratio,
        )

    elapsed = time.perf_counter() - started
    print(f"\nSeeded {inserted:,} {entity.name} rows in {elapsed:.1f}s "
          f"({inserted / max(elapsed, 0.001):,.0f} rows/s)")
    print(f"Account id: {account_id}")
    print(f"\nNext:\n  python scripts/load_test.py --account-id {account_id}")

    await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
