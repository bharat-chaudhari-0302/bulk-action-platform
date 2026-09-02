"""Request and response models for the bulk action API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

T = TypeVar("T")


class BulkActionCreate(BaseModel):
    """POST /bulk-actions.

    `payload` is deliberately untyped here: it is validated at runtime against
    the schema the *selected action* declares, which is what lets a new action
    ship without touching this file. The per-action schema is discoverable at
    GET /bulk-actions/registry.
    """

    model_config = ConfigDict(extra="forbid")

    account_id: uuid.UUID = Field(..., description="Tenant that owns the action.")
    entity_type: str = Field(..., examples=["contact"], description="Registered entity name.")
    action_type: str = Field(..., examples=["update"], description="Registered action name.")
    payload: dict[str, Any] = Field(..., description="Action-specific configuration.")

    batch_size: int | None = Field(
        default=None,
        ge=1,
        le=10_000,
        description="Entities per batch. Clamped to the account rate limit.",
    )
    scheduled_at: datetime | None = Field(
        default=None,
        description="Start the action at this UTC time instead of immediately.",
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description="Resubmitting with the same key returns the original action.",
    )

    @field_validator("entity_type", "action_type")
    @classmethod
    def _lower(cls, value: str) -> str:
        return value.strip().lower()


class BulkActionResponse(BaseModel):
    """GET /bulk-actions/{id} and the item shape of GET /bulk-actions."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    account_id: uuid.UUID
    entity_type: str
    action_type: str
    status: str
    configuration: dict[str, Any]
    batch_size: int

    total_entities: int
    processed_count: int
    success_count: int
    failure_count: int
    skipped_count: int
    total_batches: int
    completed_batches: int

    progress_percent: float = 0.0
    error: str | None = None
    idempotency_key: str | None = None

    scheduled_at: datetime | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def from_model(cls, action: Any) -> BulkActionResponse:
        total = action.total_entities or 0
        pct = round(action.processed_count / total * 100, 2) if total else 0.0
        # A finished action with nothing to do is 100% done, not 0%.
        if total == 0 and action.status in {"completed", "completed_with_errors"}:
            pct = 100.0
        return cls.model_validate({**action.__dict__, "progress_percent": pct})


class BulkActionStats(BaseModel):
    """GET /bulk-actions/{id}/stats -- the summary the assignment asks for."""

    id: uuid.UUID
    status: str
    entity_type: str
    action_type: str

    total_entities: int
    processed_count: int
    success_count: int
    failure_count: int
    skipped_count: int
    pending_count: int

    total_batches: int
    completed_batches: int
    progress_percent: float

    started_at: datetime | None
    finished_at: datetime | None
    duration_seconds: float | None
    entities_per_minute: float | None = Field(
        default=None, description="Observed throughput over the elapsed run time."
    )
    failure_breakdown: dict[str, int] = Field(
        default_factory=dict, description="Failure counts grouped by reason code."
    )
    skip_breakdown: dict[str, int] = Field(
        default_factory=dict, description="Skip counts grouped by reason code."
    )


class BulkActionLogItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    bulk_action_id: uuid.UUID
    batch_id: uuid.UUID | None
    entity_id: uuid.UUID | None
    status: str
    reason_code: str | None
    message: str | None
    details: dict[str, Any] | None
    created_at: datetime


class BulkActionBatchItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_index: int
    status: str
    entity_count: int
    success_count: int
    failure_count: int
    skipped_count: int
    attempts: int
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None


class Page(BaseModel, Generic[T]):
    """Cursor-friendly page envelope.

    Log listings use keyset pagination (`next_cursor`) rather than OFFSET: at a
    million rows an OFFSET scan re-reads everything it skips.
    """

    items: list[T]
    count: int
    next_cursor: str | None = None
    has_more: bool = False
