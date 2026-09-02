"""End-to-end behaviour against a real Postgres and Redis."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.core.errors import ConflictError, RateLimitError, ValidationError
from app.domain.entities.registry import get_entity
from app.models.bulk_action import BulkAction, BulkActionBatch, BulkActionLog
from app.models.crm import Contact
from app.models.enums import BatchStatus, BulkActionStatus, EntityLogStatus, LogReason
from app.schemas.bulk_action import BulkActionCreate
from app.services import bulk_action_service as svc
from app.services.seeding import seed_entities
from tests.conftest import requires_infra

pytestmark = [requires_infra, pytest.mark.integration]


async def _seed(session, account, count, duplicate_ratio=0.0, entity="contact"):
    inserted = await seed_entities(
        session,
        get_entity(entity),
        account.id,
        count=count,
        duplicate_ratio=duplicate_ratio,
        seed=1234,
    )
    await session.commit()
    return inserted


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
# The happy path
# ---------------------------------------------------------------------------


async def test_bulk_update_processes_every_entity_and_reports_accurately(
    session, account, driver
):
    await _seed(session, account, 250)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "bulk-churned"}, "filter": {}},
        batch_size=100,
    )
    assert action.status == BulkActionStatus.QUEUED.value

    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.status == BulkActionStatus.COMPLETED.value
    assert final.total_entities == 250
    assert final.total_batches == 3  # 100 + 100 + 50
    assert final.processed_count == 250
    assert final.success_count == 250
    assert final.failure_count == 0

    # The rows really changed.
    remaining = (
        await session.execute(
            select(func.count())
            .select_from(Contact)
            .where(Contact.account_id == account.id)
            .where(Contact.status != "bulk-churned")
        )
    ).scalar_one()
    assert remaining == 0

    # One log row per entity, as the assignment requires.
    log_count = (
        await session.execute(
            select(func.count())
            .select_from(BulkActionLog)
            .where(BulkActionLog.bulk_action_id == action.id)
        )
    ).scalar_one()
    assert log_count == 250


async def test_stats_endpoint_summarises_success_failure_and_skipped(
    session, account, driver
):
    await _seed(session, account, 60, duplicate_ratio=0.5)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={
            "updates": {"status": "bulk-churned"},
            "filter": {},
            "deduplicate_by": "email",
        },
        batch_size=25,
    )
    await driver.run(action.id)

    # Drop the identity map first: without it the ORM hands back the instance
    # cached at submission time and the stats are read from pre-run values.
    await _refresh(session, action.id)

    stats = await svc.get_stats(session, action.id)
    assert stats.total_entities == 60
    assert stats.success_count + stats.failure_count + stats.skipped_count == 60
    assert stats.progress_percent == 100.0
    assert stats.skipped_count > 0
    assert LogReason.DUPLICATE_EMAIL.value in stats.skip_breakdown
    assert stats.duration_seconds is not None


async def test_filter_narrows_the_target_set(session, account, driver):
    await _seed(session, account, 100)
    active_before = (
        await session.execute(
            select(func.count())
            .select_from(Contact)
            .where(Contact.account_id == account.id)
            .where(Contact.status == "active")
        )
    ).scalar_one()
    assert 0 < active_before < 100  # the seeder spreads statuses

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "bulk-churned"}, "filter": {"status": "active"}},
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.total_entities == active_before
    assert final.success_count == active_before


async def test_explicit_entity_ids_target_exactly_those_rows(session, account, driver):
    await _seed(session, account, 50)
    ids = list(
        (
            await session.execute(
                select(Contact.id).where(Contact.account_id == account.id).limit(5)
            )
        )
        .scalars()
        .all()
    )

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "vip"}, "entity_ids": [str(i) for i in ids]},
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.total_entities == 5
    assert final.success_count == 5

    changed = (
        await session.execute(
            select(func.count()).select_from(Contact).where(Contact.status == "vip")
        )
    ).scalar_one()
    assert changed == 5


# ---------------------------------------------------------------------------
# De-duplication
# ---------------------------------------------------------------------------


async def test_duplicate_emails_are_skipped_and_logged_as_skipped(
    session, account, driver
):
    await _seed(session, account, 200, duplicate_ratio=0.4)

    total_emails = (
        await session.execute(
            select(func.count(func.distinct(func.lower(Contact.email)))).where(
                Contact.account_id == account.id
            )
        )
    ).scalar_one()

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={
            "updates": {"status": "deduped"},
            "filter": {},
            "deduplicate_by": "email",
        },
        batch_size=50,
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    # Exactly one entity per distinct email is processed; the rest are skipped.
    assert final.success_count == total_emails
    assert final.skipped_count == 200 - total_emails
    assert final.processed_count == 200

    skipped = await svc.list_logs(
        session, action.id, status=EntityLogStatus.SKIPPED.value, limit=1000
    )
    assert skipped.count == final.skipped_count
    assert all(item.reason_code == LogReason.DUPLICATE_EMAIL.value for item in skipped.items)
    # A skipped log names the offending value, so a user can act on it.
    assert "email" in (skipped.items[0].details or {})


async def test_dedup_is_stable_across_batch_retries(session, account, driver):
    """Re-running a batch must not change which copy won -- the ledger is durable."""
    await _seed(session, account, 40, duplicate_ratio=0.5)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={
            "updates": {"status": "x"},
            "filter": {},
            "deduplicate_by": "email",
        },
        batch_size=40,
    )
    await driver.run(action.id)
    first = await _refresh(session, action.id)

    # Replay the same batch job. It is already completed, so it is a no-op.
    from app.workers.tasks import process_batch

    replay = await process_batch(driver.ctx, str(action.id), 0)
    assert replay["status"] == "already_completed"

    second = await _refresh(session, action.id)
    assert (second.success_count, second.skipped_count) == (
        first.success_count,
        first.skipped_count,
    )


# ---------------------------------------------------------------------------
# Scheduling, cancellation, idempotency
# ---------------------------------------------------------------------------


async def test_scheduled_action_is_deferred_not_run(session, account, driver, arq):
    await _seed(session, account, 10)
    when = datetime.now(UTC) + timedelta(hours=3)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "later"}, "filter": {}},
        scheduled_at=when,
    )

    assert action.status == BulkActionStatus.SCHEDULED.value
    planned = arq.pop("plan_bulk_action")
    assert len(planned) == 1
    # The queue itself holds the delay; no cron process is involved.
    assert planned[0]["defer_until"] == when


async def test_scheduling_in_the_past_is_rejected(session, account, arq, redis):
    with pytest.raises(ValidationError):
        await svc.create_bulk_action(
            session,
            arq,
            redis,
            BulkActionCreate(
                account_id=account.id,
                entity_type="contact",
                action_type="update",
                payload={"updates": {"status": "x"}, "filter": {}},
                scheduled_at=datetime.now(UTC) - timedelta(minutes=1),
            ),
        )


async def test_cancelling_stops_remaining_batches(session, account, driver, arq):
    await _seed(session, account, 300)

    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "cancelled-test"}, "filter": {}},
        batch_size=100,
    )
    await driver.plan(action.id)

    jobs = arq.pop("process_batch")
    assert len(jobs) == 3

    # Run one batch, then cancel mid-flight.
    from app.workers.tasks import process_batch

    assert (await process_batch(driver.ctx, *jobs[0]["args"]))["status"] == "completed"
    await svc.cancel_bulk_action(session, action.id)
    await session.commit()

    for job in jobs[1:]:
        assert (await process_batch(driver.ctx, *job["args"]))["status"] == "cancelled"

    final = await _refresh(session, action.id)
    assert final.status == BulkActionStatus.CANCELLED.value
    # The batch that had already committed keeps its work; the rest never ran.
    assert final.processed_count == 100

    batch_states = (
        await session.execute(
            select(BulkActionBatch.status).where(BulkActionBatch.bulk_action_id == action.id)
        )
    ).scalars().all()
    assert sorted(batch_states) == sorted(
        [BatchStatus.COMPLETED.value, BatchStatus.CANCELLED.value, BatchStatus.CANCELLED.value]
    )


async def test_cancelling_a_finished_action_is_a_conflict(session, account, driver):
    await _seed(session, account, 5)
    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "done"}, "filter": {}},
    )
    await driver.run(action.id)
    with pytest.raises(ConflictError):
        await svc.cancel_bulk_action(session, action.id)


async def test_idempotency_key_replays_instead_of_duplicating_work(
    session, account, arq, redis
):
    await _seed(session, account, 5)
    request = BulkActionCreate(
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "once"}, "filter": {}},
        idempotency_key="submit-42",
    )

    first, created_first = await svc.create_bulk_action(session, arq, redis, request)
    await session.commit()
    second, created_second = await svc.create_bulk_action(session, arq, redis, request)

    assert created_first is True
    assert created_second is False
    assert first.id == second.id
    assert len(arq.pop("plan_bulk_action")) == 1


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


async def test_processing_is_capped_by_the_account_rate_limit(
    session, account, driver, arq, redis
):
    """A batch that exceeds the remaining budget is deferred, not dropped."""
    account.rate_limit_per_minute = 120
    session.add(account)
    await session.commit()

    await _seed(session, account, 200)
    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "limited"}, "filter": {}},
        batch_size=100,
    )
    await driver.plan(action.id)

    from app.workers.tasks import process_batch

    jobs = arq.pop("process_batch")
    assert len(jobs) == 2

    first = await process_batch(driver.ctx, *jobs[0]["args"])
    second = await process_batch(driver.ctx, *jobs[1]["args"])

    # The bucket holds 120 tokens; the first batch of 100 fits, the second does not.
    assert first["status"] == "completed"
    assert second["status"] == "rate_limited"
    assert second["retry_after_ms"] > 0

    # Deferred, not lost: a replacement job is queued for when tokens return.
    deferred = arq.pop("process_batch")
    assert len(deferred) == 1
    assert deferred[0]["defer_by"] > 0

    # And the batch is still pending, so no work was silently skipped.
    still_pending = (
        await session.execute(
            select(func.count())
            .select_from(BulkActionBatch)
            .where(BulkActionBatch.bulk_action_id == action.id)
            .where(BulkActionBatch.status == BatchStatus.PENDING.value)
        )
    ).scalar_one()
    assert still_pending == 1


async def test_batch_size_is_clamped_so_a_batch_can_always_be_admitted(
    session, account, arq, redis
):
    account.rate_limit_per_minute = 50
    session.add(account)
    await session.commit()

    action, _ = await svc.create_bulk_action(
        session,
        arq,
        redis,
        BulkActionCreate(
            account_id=account.id,
            entity_type="contact",
            action_type="update",
            payload={"updates": {"status": "x"}, "filter": {}},
            batch_size=1000,
        ),
    )
    assert action.batch_size == 50


async def test_submission_rate_limit_returns_429(session, account, arq, redis, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "api_rate_limit_per_minute", 2)
    request = {
        "account_id": account.id,
        "entity_type": "contact",
        "action_type": "update",
        "payload": {"updates": {"status": "x"}, "filter": {}},
    }
    for _ in range(2):
        await svc.create_bulk_action(session, arq, redis, BulkActionCreate(**request))
    with pytest.raises(RateLimitError) as exc:
        await svc.create_bulk_action(session, arq, redis, BulkActionCreate(**request))
    assert exc.value.retry_after_seconds > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


async def test_an_empty_target_set_completes_rather_than_hanging(session, account, driver):
    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "x"}, "filter": {"status": "nonexistent"}},
    )
    await driver.run(action.id)

    final = await _refresh(session, action.id)
    assert final.status == BulkActionStatus.COMPLETED.value
    assert final.total_entities == 0
    stats = await svc.get_stats(session, action.id)
    assert stats.progress_percent == 100.0


async def test_entities_that_leave_the_target_set_are_accounted_for(
    session, account, driver
):
    """Progress must still reach 100% when rows vanish mid-flight."""
    await _seed(session, account, 100)
    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={"updates": {"status": "drifted"}, "filter": {}},
        batch_size=100,
    )
    await driver.plan(action.id)

    # Delete 10 rows after planning but before the batch runs.
    doomed = list(
        (
            await session.execute(
                select(Contact.id).where(Contact.account_id == account.id).limit(10)
            )
        )
        .scalars()
        .all()
    )
    await session.execute(Contact.__table__.delete().where(Contact.id.in_(doomed)))
    await session.commit()

    await driver.run_batches()

    final = await _refresh(session, action.id)
    assert final.total_entities == 100
    assert final.processed_count == 100  # nothing unaccounted for
    assert final.success_count == 90
    assert final.skipped_count == 10
    assert final.status == BulkActionStatus.COMPLETED.value

    stats = await svc.get_stats(session, action.id)
    assert stats.skip_breakdown[LogReason.LEFT_TARGET_SET.value] == 10


async def test_no_op_updates_are_skipped_not_rewritten(session, account, driver):
    await _seed(session, account, 30)
    payload = {"updates": {"status": "settled"}, "filter": {}}

    first = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload=payload,
    )
    await driver.run(first.id)

    second = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload=payload,
    )
    await driver.run(second.id)

    final = await _refresh(session, second.id)
    assert final.skipped_count == 30
    assert final.success_count == 0
    stats = await svc.get_stats(session, second.id)
    assert stats.skip_breakdown[LogReason.NO_CHANGE.value] == 30


async def test_invalid_payload_never_reaches_the_queue(session, account, arq, redis):
    with pytest.raises(ValidationError):
        await svc.create_bulk_action(
            session,
            arq,
            redis,
            BulkActionCreate(
                account_id=account.id,
                entity_type="contact",
                action_type="update",
                payload={"updates": {"age": 500}, "filter": {}},
            ),
        )
    assert arq.jobs == []


async def test_unknown_account_is_a_404(session, arq, redis):
    from app.core.errors import NotFoundError

    with pytest.raises(NotFoundError):
        await svc.create_bulk_action(
            session,
            arq,
            redis,
            BulkActionCreate(
                account_id=uuid.uuid4(),
                entity_type="contact",
                action_type="update",
                payload={"updates": {"status": "x"}, "filter": {}},
            ),
        )


# ---------------------------------------------------------------------------
# Log retrieval
# ---------------------------------------------------------------------------


async def test_logs_are_filterable_and_keyset_paginated(session, account, driver):
    await _seed(session, account, 120, duplicate_ratio=0.3)
    action = await driver.submit(
        session,
        account_id=account.id,
        entity_type="contact",
        action_type="update",
        payload={
            "updates": {"status": "logged"},
            "filter": {},
            "deduplicate_by": "email",
        },
        batch_size=60,
    )
    await driver.run(action.id)

    page1 = await svc.list_logs(session, action.id, limit=50)
    assert page1.count == 50
    assert page1.has_more is True

    page2 = await svc.list_logs(session, action.id, cursor=int(page1.next_cursor), limit=50)
    assert page2.count == 50
    # Keyset pagination must not repeat rows.
    assert {i.id for i in page1.items}.isdisjoint({i.id for i in page2.items})

    successes = await svc.list_logs(
        session, action.id, status=EntityLogStatus.SUCCESS.value, limit=1000
    )
    assert all(i.status == EntityLogStatus.SUCCESS.value for i in successes.items)

    by_reason = await svc.list_logs(
        session, action.id, reason_code=LogReason.DUPLICATE_EMAIL.value, limit=1000
    )
    assert by_reason.count > 0
