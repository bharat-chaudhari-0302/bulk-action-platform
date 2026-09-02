"""Bulk action handler contract.

A handler answers two questions and nothing else:

  1. What does a valid request for this action look like?   -> ConfigModel
  2. Given a loaded slice of entities, what happened to each? -> execute()

Everything else -- batching, de-duplication, rate limiting, scheduling,
retries, logging, counters, cancellation -- is provided by the platform and is
identical for every action. That is what keeps a new action to a single file.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.entities.base import EntityDescriptor, EntityRow
from app.models.enums import EntityLogStatus, LogReason


@dataclass(slots=True)
class EntityOutcome:
    """What happened to one entity. Becomes exactly one bulk_action_logs row."""

    entity_id: uuid.UUID | None
    status: EntityLogStatus
    reason_code: LogReason | str | None = None
    message: str | None = None
    details: dict[str, Any] | None = None

    @classmethod
    def success(
        cls, entity_id: uuid.UUID, reason: LogReason, **details: Any
    ) -> EntityOutcome:
        return cls(entity_id, EntityLogStatus.SUCCESS, reason, details=details or None)

    @classmethod
    def failed(
        cls, entity_id: uuid.UUID | None, reason: LogReason, message: str, **details: Any
    ) -> EntityOutcome:
        return cls(entity_id, EntityLogStatus.FAILED, reason, message, details or None)

    @classmethod
    def skipped(
        cls, entity_id: uuid.UUID | None, reason: LogReason, message: str, **details: Any
    ) -> EntityOutcome:
        return cls(entity_id, EntityLogStatus.SKIPPED, reason, message, details or None)


@dataclass(slots=True)
class BatchResult:
    """Aggregate outcome of one batch."""

    outcomes: list[EntityOutcome] = field(default_factory=list)

    def extend(self, outcomes: list[EntityOutcome]) -> None:
        self.outcomes.extend(outcomes)

    @property
    def success_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status is EntityLogStatus.SUCCESS)

    @property
    def failure_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status is EntityLogStatus.FAILED)

    @property
    def skipped_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status is EntityLogStatus.SKIPPED)

    @property
    def processed_count(self) -> int:
        return len(self.outcomes)


@dataclass(slots=True)
class ActionContext:
    """Everything a handler may need, passed in rather than imported."""

    session: AsyncSession
    entity: type[EntityDescriptor]
    account_id: uuid.UUID
    bulk_action_id: uuid.UUID
    batch_id: uuid.UUID
    config: BaseModel


class BulkActionHandler:
    """Base class for bulk actions. Subclass, set the class vars, register."""

    #: Stable public identifier used in the API (`"update"`).
    action_type: ClassVar[str]
    #: Human-readable description, surfaced by the registry endpoint.
    description: ClassVar[str] = ""
    #: Entity names this action supports, or "*" for entity-agnostic.
    supported_entities: ClassVar[frozenset[str] | Literal["*"]] = "*"
    #: Pydantic model validating `payload` at submission time.
    ConfigModel: ClassVar[type[BaseModel]]
    #: Whether soft-deleted rows are part of the target set.
    include_deleted: ClassVar[bool] = False

    def supports(self, entity_name: str) -> bool:
        return self.supported_entities == "*" or entity_name in self.supported_entities

    def validate_config(
        self, entity: type[EntityDescriptor], payload: dict[str, Any]
    ) -> BaseModel:
        """Validate the request payload for this action against this entity.

        Override to add entity-aware checks (bulk_update validates the update
        field names against the entity descriptor here).
        """
        from pydantic import ValidationError as PydanticValidationError

        from app.core.errors import ValidationError

        try:
            return self.ConfigModel(**payload)
        except PydanticValidationError as exc:
            details = [
                {"field": ".".join(str(p) for p in e["loc"]), "error": e["msg"]}
                for e in exc.errors()
            ]
            raise ValidationError(
                f"Invalid payload for action '{self.action_type}'.", errors=details
            ) from exc

    def selection_filters(self, config: BaseModel) -> dict[str, Any] | None:
        """The filter object used to enumerate the target set."""
        return getattr(config, "filter", None)

    def explicit_ids(self, config: BaseModel) -> list[uuid.UUID] | None:
        """Explicit entity ids, when the client supplied them instead of a filter."""
        return getattr(config, "entity_ids", None)

    def dedup_field(self, entity: type[EntityDescriptor], config: BaseModel) -> str | None:
        """Field used for de-duplication, or None to disable it."""
        return getattr(config, "deduplicate_by", None)

    async def execute(self, ctx: ActionContext, rows: list[EntityRow]) -> BatchResult:
        """Apply the action to a de-duplicated, validated slice of entities."""
        raise NotImplementedError

    def describe(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "description": self.description,
            "supported_entities": (
                "*" if self.supported_entities == "*" else sorted(self.supported_entities)
            ),
            "payload_schema": self.ConfigModel.model_json_schema(),
        }
