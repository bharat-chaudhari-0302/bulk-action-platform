"""Bulk action control-plane tables.

Four tables carry the whole lifecycle:

  bulk_actions        one row per submission; holds the denormalised counters
                      that /stats reads in O(1)
  bulk_action_batches one row per unit of work; the (action, batch_index)
                      unique key is what makes retries idempotent
  bulk_action_logs    one row per entity outcome; the audit trail
  bulk_action_dedup   uniqueness ledger backing email de-duplication
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, created_at_col, uuid_pk
from app.models.enums import BatchStatus, BulkActionStatus, EntityLogStatus

_ACTION_STATUSES = ", ".join(repr(s.value) for s in BulkActionStatus)
_BATCH_STATUSES = ", ".join(repr(s.value) for s in BatchStatus)
_LOG_STATUSES = ", ".join(repr(s.value) for s in EntityLogStatus)


class BulkAction(Base):
    __tablename__ = "bulk_actions"

    id: Mapped[uuid.UUID] = uuid_pk()
    account_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False
    )

    # Resolved against the entity / action registries at submission time.
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=BulkActionStatus.QUEUED.value
    )

    # The action-specific request, already validated against the handler config
    # model. Kept as JSONB so a new action needs no schema migration.
    configuration: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")

    batch_size: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1000")
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # --- Counters -------------------------------------------------------
    # Incremented in place inside each batch transaction (SET n = n + :delta),
    # never read-modify-written, so concurrent workers cannot lose updates.
    # This is what keeps /stats an O(1) row read instead of an aggregate over
    # millions of log rows.
    total_entities: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    processed_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    skipped_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_batches: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completed_batches: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Lets a client retry a submission safely across a network failure.
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = created_at_col()
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("status IN (" + _ACTION_STATUSES + ")", name="status_valid"),
        UniqueConstraint("account_id", "idempotency_key", name="uq_bulk_actions_idempotency"),
        Index("ix_bulk_actions_account_created", "account_id", "created_at"),
        Index("ix_bulk_actions_status", "status"),
    )


class BulkActionBatch(Base):
    __tablename__ = "bulk_action_batches"

    id: Mapped[uuid.UUID] = uuid_pk()
    bulk_action_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("bulk_actions.id", ondelete="CASCADE"), nullable=False
    )
    batch_index: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=BatchStatus.PENDING.value
    )

    # A batch is defined either by an explicit id list (the client supplied the
    # entities) or by an inclusive keyset range over the entity primary key (the
    # client supplied a filter). The range form keeps planning memory-constant
    # over millions of rows.
    entity_ids: Mapped[list[uuid.UUID] | None] = mapped_column(
        ARRAY(PGUUID(as_uuid=True)), nullable=True
    )
    cursor_start: Mapped[uuid.UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    cursor_end: Mapped[uuid.UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)

    entity_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    skipped_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = created_at_col()
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("status IN (" + _BATCH_STATUSES + ")", name="status_valid"),
        # The idempotency anchor: re-planning or a duplicate enqueue cannot
        # create a second row for the same slice of work.
        UniqueConstraint("bulk_action_id", "batch_index", name="uq_batch_action_index"),
        Index("ix_batches_action_status", "bulk_action_id", "status"),
    )


class BulkActionLog(Base):
    __tablename__ = "bulk_action_logs"

    # BIGSERIAL rather than UUID: this table grows fastest, and a monotonic key
    # keeps inserts appending at the index leaf edge instead of scattering.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    bulk_action_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("bulk_actions.id", ondelete="CASCADE"), nullable=False
    )
    batch_id: Mapped[uuid.UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("bulk_action_batches.id", ondelete="CASCADE"),
        nullable=True,
    )
    entity_id: Mapped[uuid.UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = created_at_col()

    __table_args__ = (
        CheckConstraint("status IN (" + _LOG_STATUSES + ")", name="status_valid"),
        # Serves GET /bulk-actions/{id}/logs?status=failed with one index scan;
        # the trailing id gives stable keyset pagination within a status.
        Index("ix_logs_action_status_id", "bulk_action_id", "status", "id"),
        Index("ix_logs_action_entity", "bulk_action_id", "entity_id"),
    )


class BulkActionDedup(Base):
    """Uniqueness ledger for de-duplication.

    The primary key does the work: INSERT ... ON CONFLICT DO NOTHING RETURNING
    reports exactly which keys were seen first. Because the database arbitrates,
    de-duplication stays correct across concurrent workers and across job
    retries, which an in-process set could not guarantee.
    """

    __tablename__ = "bulk_action_dedup"

    bulk_action_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("bulk_actions.id", ondelete="CASCADE"),
        primary_key=True,
    )
    dedup_key: Mapped[str] = mapped_column(String(512), primary_key=True)
