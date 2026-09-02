"""The extensibility criterion, tested rather than asserted.

`delete` was added to a finished platform, and `company` was added to a platform
that only knew about contacts. If the abstraction is real, then every
entity x action combination works through the same endpoints, and a brand-new
action written inside this test file -- never imported by any other module --
runs on the shared pipeline with de-duplication, logging, counters and
finalisation intact.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from app.domain.actions.base import (
    ActionContext,
    BatchResult,
    BulkActionHandler,
    EntityOutcome,
)
from app.domain.actions.registry import register_action
from app.domain.entities.base import EntityRow
from app.domain.entities.registry import get_entity
from app.models.bulk_action import BulkAction, BulkActionLog
from app.models.crm import Company, Contact
from app.models.enums import BulkActionStatus, EntityLogStatus, LogReason
from app.services import bulk_action_service as svc
from app.services.seeding import seed_entities
from tests.conftest import requires_infra

pytestmark = [requires_infra, pytest.mark.integration]


async def _seed(session, account, entity_name, count, duplicate_ratio=0.0):
    await seed_entities(
        session,
        get_entity(entity_name),
        account.id,
        count=count,
        duplicate_ratio=duplicate_ratio,
        seed=7,
    )
    await session.commit()


async def _refresh(session, action_id) -> BulkAction:
    """Re-read the action as the workers left it.

    The worker tasks commit through their own sessions, so this session has to
    drop its identity map and begin a new transaction to see their writes.
    Expiring instead of expunging would leave expired ORM instances behind,
    whose attribute access then triggers a lazy refresh outside the async
    context and raises MissingGreenlet.
    """
    session.expunge_all()
    await session.rollback()
    return (
        await session.execute(select(BulkAction).where(BulkAction.id == action_id))
    ).scalar_one()


# ---------------------------------------------------------------------------
# A second entity, through the same core
# ---------------------------------------------------------------------------


async def test_the_same_update_action_works_on_companies(session, account, driver):
    await _seed(session, account, "company", 80)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="company",
        action_type="update",
        payload={"updates": {"industry": "bulk-logistics"}, "filter": {}},
        batch_size=30,
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.status == BulkActionStatus.COMPLETED.value
    assert final.success_count == 80

    off_target = (
        await session.execute(
            select(func.count())
            .select_from(Company)
            .where(Company.account_id == account.id)
            .where(Company.industry != "bulk-logistics")
        )
    ).scalar_one()
    assert off_target == 0


async def test_dedup_uses_the_entity_declared_key_not_a_hard_coded_email(
    session, account, driver
):
    """Company de-duplicates on `domain`; the pipeline never mentions either field."""
    await _seed(session, account, "company", 100, duplicate_ratio=0.4)

    distinct_domains = (
        await session.execute(
            select(func.count(func.distinct(func.lower(Company.domain)))).where(
                Company.account_id == account.id
            )
        )
    ).scalar_one()

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="company",
        action_type="update",
        payload={
            "updates": {"status": "reviewed"},
            "filter": {},
            "deduplicate_by": "domain",
        },
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.success_count == distinct_domains
    assert final.skipped_count == 100 - distinct_domains

    skipped = await svc.list_logs(
        session, action.id, status=EntityLogStatus.SKIPPED.value, limit=1000
    )
    assert all(i.reason_code == LogReason.DUPLICATE_KEY.value for i in skipped.items)


# ---------------------------------------------------------------------------
# A second action, through the same core
# ---------------------------------------------------------------------------


async def test_bulk_delete_soft_deletes_and_removes_rows_from_the_working_set(
    session, account, driver
):
    await _seed(session, account, "contact", 40)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="delete",
        payload={"filter": {}},
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.success_count == 40

    live = (
        await session.execute(
            select(func.count())
            .select_from(Contact)
            .where(Contact.account_id == account.id)
            .where(Contact.deleted_at.is_(None))
        )
    ).scalar_one()
    assert live == 0

    # Soft delete: the rows are still there, which is why it is reversible.
    total = (
        await session.execute(
            select(func.count()).select_from(Contact).where(Contact.account_id == account.id)
        )
    ).scalar_one()
    assert total == 40


async def test_hard_delete_removes_rows(session, account, driver):
    await _seed(session, account, "contact", 20)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="delete",
        payload={"filter": {}, "hard": True},
    )
    await driver.run(action.id)

    total = (
        await session.execute(
            select(func.count()).select_from(Contact).where(Contact.account_id == account.id)
        )
    ).scalar_one()
    assert total == 0


async def test_delete_works_on_companies_too(session, account, driver):
    """Two entities x two actions, zero action-specific or entity-specific glue."""
    await _seed(session, account, "company", 15)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="company",
        action_type="delete",
        payload={"filter": {}},
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.success_count == 15


# ---------------------------------------------------------------------------
# A brand-new action, defined here and nowhere else
# ---------------------------------------------------------------------------


class TagConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    suffix: str = Field(..., min_length=1, max_length=20)
    filter: dict[str, Any] | None = None
    deduplicate_by: str | None = None


@register_action
class BulkTagAction(BulkActionHandler):
    """Appends a suffix to each entity's name.

    Everything this action needs to declare is on these few lines. It gets
    batching, queueing, retries, rate limiting, de-duplication, per-entity
    logging, progress counters, cancellation and stats for free.
    """

    action_type: ClassVar[str] = "tag"
    description: ClassVar[str] = "Append a suffix to the name field."
    ConfigModel: ClassVar[type[BaseModel]] = TagConfig

    async def execute(self, ctx: ActionContext, rows: list[EntityRow]) -> BatchResult:
        from sqlalchemy import update

        result = BatchResult()
        for row in rows:
            await ctx.session.execute(
                update(ctx.entity.table)
                .where(ctx.entity.pk() == row.id)
                .values(name=f"{row.get('name')}{ctx.config.suffix}")
            )
            result.outcomes.append(EntityOutcome.success(row.id, LogReason.UPDATED))
        return result


async def test_a_new_action_needs_no_change_to_the_platform(session, account, driver):
    await _seed(session, account, "contact", 25)

    # It is immediately visible to clients through the registry endpoint.
    snapshot = svc.registry_snapshot()
    assert any(a["action_type"] == "tag" for a in snapshot["actions"])
    assert {"entity_type": "contact", "action_type": "tag"} in snapshot[
        "supported_combinations"
    ]

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="tag",
        payload={"suffix": " [VIP]", "filter": {}},
        batch_size=10,
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.status == BulkActionStatus.COMPLETED.value
    assert final.success_count == 25
    assert final.total_batches == 3

    tagged = (
        await session.execute(
            select(func.count())
            .select_from(Contact)
            .where(Contact.account_id == account.id)
            .where(Contact.name.like("%[VIP]"))
        )
    ).scalar_one()
    assert tagged == 25

    # And it produced the same per-entity audit trail as every built-in action.
    log_count = (
        await session.execute(
            select(func.count())
            .select_from(BulkActionLog)
            .where(BulkActionLog.bulk_action_id == action.id)
        )
    ).scalar_one()
    assert log_count == 25
