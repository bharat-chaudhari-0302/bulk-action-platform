"""Accounts and CRM entity endpoints.

Supporting cast: enough to create a tenant, generate demo data and verify that a
bulk action actually changed the rows it claims to have changed.

The entity endpoints are driven by the registry rather than written per entity,
so `/entities/contact` and `/entities/company` are the same code path -- and so
is whatever entity is added next.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import db_session
from app.core.config import settings
from app.core.errors import NotFoundError
from app.domain.entities.registry import all_entities, get_entity
from app.models.account import Account
from app.schemas.bulk_action import Page
from app.services.seeding import seed_entities

router = APIRouter(tags=["crm"])

DbSession = Annotated[AsyncSession, Depends(db_session)]


# --- Accounts -------------------------------------------------------------


class AccountCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=255)
    rate_limit_per_minute: int = Field(
        default=settings.default_rate_limit_per_minute,
        ge=1,
        le=10_000_000,
        description="Processing ceiling in entities/minute for this account.",
    )


class AccountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    rate_limit_per_minute: int


@router.post(
    "/accounts",
    status_code=status.HTTP_201_CREATED,
    response_model=AccountResponse,
    summary="Create an account (tenant)",
)
async def create_account(payload: AccountCreate, session: DbSession) -> AccountResponse:
    account = Account(name=payload.name, rate_limit_per_minute=payload.rate_limit_per_minute)
    session.add(account)
    await session.flush()
    return AccountResponse.model_validate(account)


@router.get("/accounts", response_model=list[AccountResponse], summary="List accounts")
async def list_accounts(
    session: DbSession, limit: int = Query(50, ge=1, le=200)
) -> list[AccountResponse]:
    rows = (
        (await session.execute(select(Account).order_by(Account.created_at.desc()).limit(limit)))
        .scalars()
        .all()
    )
    return [AccountResponse.model_validate(a) for a in rows]


@router.patch(
    "/accounts/{account_id}",
    response_model=AccountResponse,
    summary="Change an account's rate limit",
)
async def update_account(
    account_id: uuid.UUID,
    rate_limit_per_minute: Annotated[int, Query(ge=1, le=10_000_000)],
    session: DbSession,
) -> AccountResponse:
    """Exposed so the rate limiter can be demonstrated without a redeploy."""
    account = (
        await session.execute(select(Account).where(Account.id == account_id))
    ).scalar_one_or_none()
    if account is None:
        raise NotFoundError(f"Account '{account_id}' does not exist.")
    account.rate_limit_per_minute = rate_limit_per_minute
    await session.flush()
    return AccountResponse.model_validate(account)


# --- Entities -------------------------------------------------------------


class SeedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: uuid.UUID
    count: int = Field(1000, ge=1, le=1_000_000)
    duplicate_ratio: float = Field(
        0.0,
        ge=0.0,
        le=0.9,
        description="Fraction of rows that reuse an earlier de-duplication key, "
        "so de-duplication has something to find.",
    )


@router.get("/entities", summary="Registered entity types")
async def list_entity_types() -> dict[str, Any]:
    return {"entities": [e.describe() for e in all_entities().values()]}


@router.post(
    "/entities/{entity_type}/seed",
    status_code=status.HTTP_201_CREATED,
    summary="Generate demo rows for an entity",
)
async def seed(entity_type: str, payload: SeedRequest, session: DbSession) -> dict[str, Any]:
    entity = get_entity(entity_type)
    inserted = await seed_entities(
        session,
        entity,
        payload.account_id,
        count=payload.count,
        duplicate_ratio=payload.duplicate_ratio,
    )
    return {"entity_type": entity.name, "inserted": inserted}


@router.get("/entities/{entity_type}", summary="List entities (verify an action's effect)")
async def list_entities(
    entity_type: str,
    session: DbSession,
    account_id: uuid.UUID | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    include_deleted: bool = Query(False),
    limit: int = Query(25, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Page[dict[str, Any]]:
    entity = get_entity(entity_type)
    columns = [entity.column(name) for name in entity.readable_columns()]

    stmt = select(*columns)
    count_stmt = select(func.count()).select_from(entity.table)
    if account_id:
        stmt = stmt.where(entity.account_col() == account_id)
        count_stmt = count_stmt.where(entity.account_col() == account_id)
    if status_filter and "status" in entity.table.c:
        stmt = stmt.where(entity.column("status") == status_filter)
        count_stmt = count_stmt.where(entity.column("status") == status_filter)
    if entity.soft_delete_column and not include_deleted:
        stmt = stmt.where(entity.column(entity.soft_delete_column).is_(None))
        count_stmt = count_stmt.where(entity.column(entity.soft_delete_column).is_(None))

    rows = (
        await session.execute(stmt.order_by(entity.pk()).limit(limit).offset(offset))
    ).all()
    total = (await session.execute(count_stmt)).scalar_one()
    names = entity.readable_columns()

    return Page[dict[str, Any]](
        items=[
            {k: (str(v) if isinstance(v, uuid.UUID) else v)
             for k, v in zip(names, row, strict=True)}
            for row in rows
        ],
        count=total,
        has_more=offset + len(rows) < total,
    )
