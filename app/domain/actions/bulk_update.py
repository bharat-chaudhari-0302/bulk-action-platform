"""Bulk update: apply the same field values to every entity in the target set.

The whole action is one file. Note what it does *not* contain: no batching, no
queueing, no retry logic, no de-duplication, no rate limiting, no log writing.
Those belong to the platform and are shared by every action.
"""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, update

from app.core.errors import ValidationError
from app.domain.actions.base import (
    ActionContext,
    BatchResult,
    BulkActionHandler,
    EntityOutcome,
)
from app.domain.actions.registry import register_action
from app.domain.entities.base import EntityDescriptor, EntityRow
from app.domain.filters import validate_filter
from app.domain.sql_utils import pk_in
from app.models.enums import LogReason


class BulkUpdateConfig(BaseModel):
    """Request payload for `action_type: "update"`."""

    model_config = ConfigDict(extra="forbid")

    updates: dict[str, Any] = Field(
        ..., description="Field/value pairs applied to every matched entity."
    )
    filter: dict[str, Any] | None = Field(
        default=None,
        description="Selects the target set. `{}` means every entity in the account.",
    )
    entity_ids: list[uuid.UUID] | None = Field(
        default=None, description="Explicit target ids, as an alternative to `filter`."
    )
    deduplicate_by: str | None = Field(
        default=None,
        description="Entity field on which duplicates are detected and skipped, e.g. 'email'.",
    )

    @model_validator(mode="after")
    def _require_a_target_set(self) -> BulkUpdateConfig:
        # Refusing to guess here is deliberate: the difference between "no
        # filter" and "match everything" is a million rows.
        if self.filter is None and self.entity_ids is None:
            raise ValueError(
                "Provide either `entity_ids` or `filter` "
                "(use `filter: {}` to target every entity in the account)."
            )
        if self.filter is not None and self.entity_ids is not None:
            raise ValueError("Provide `entity_ids` or `filter`, not both.")
        if self.entity_ids is not None and not self.entity_ids:
            raise ValueError("`entity_ids` must not be empty.")
        return self


@register_action
class BulkUpdateAction(BulkActionHandler):
    action_type: ClassVar[str] = "update"
    description: ClassVar[str] = (
        "Set one or more fields to the same value across every entity in the target set."
    )
    supported_entities: ClassVar[str] = "*"
    ConfigModel: ClassVar[type[BaseModel]] = BulkUpdateConfig

    def validate_config(
        self, entity: type[EntityDescriptor], payload: dict[str, Any]
    ) -> BulkUpdateConfig:
        config: BulkUpdateConfig = super().validate_config(entity, payload)  # type: ignore[assignment]

        # Validate the update values against *this entity's* declared fields.
        # A typo or an out-of-range value is a 422 at submission time rather
        # than a million identical failures discovered an hour later.
        coerced = entity.validate_updates(config.updates)
        config = config.model_copy(update={"updates": coerced})

        validate_filter(entity, config.filter)

        if config.deduplicate_by:
            if config.deduplicate_by not in entity.updatable_fields:
                raise ValidationError(
                    f"Cannot de-duplicate '{entity.name}' by '{config.deduplicate_by}': "
                    f"unknown field."
                )
            if config.deduplicate_by not in entity.dedup_fields:
                raise ValidationError(
                    f"'{config.deduplicate_by}' is not a de-duplication key for "
                    f"'{entity.name}'. Available: {sorted(entity.dedup_fields)}."
                )
        return config

    async def execute(self, ctx: ActionContext, rows: list[EntityRow]) -> BatchResult:
        result = BatchResult()
        if not rows:
            return result

        config: BulkUpdateConfig = ctx.config  # type: ignore[assignment]
        entity = ctx.entity
        values = dict(config.updates)

        # Skip rows that already hold the target values: a no-op UPDATE still
        # writes a new row version and bloats the table.
        to_write: list[uuid.UUID] = []
        for row in rows:
            if all(row.get(k) == v for k, v in values.items()):
                result.outcomes.append(
                    EntityOutcome.skipped(
                        row.id,
                        LogReason.NO_CHANGE,
                        "Entity already holds the requested values.",
                    )
                )
            else:
                to_write.append(row.id)

        if not to_write:
            return result

        if "updated_at" in entity.table.c:
            values["updated_at"] = func.now()

        # One statement per batch. RETURNING tells us precisely which rows were
        # written, so an entity deleted between planning and execution is
        # reported rather than silently counted as a success.
        stmt = (
            update(entity.table)
            .where(pk_in(entity.pk(), to_write))
            .where(entity.account_col() == ctx.account_id)
            .values(**values)
            .returning(entity.pk())
        )
        updated = {r[0] for r in (await ctx.session.execute(stmt)).all()}

        changed = dict(config.updates)
        for entity_id in to_write:
            if entity_id in updated:
                result.outcomes.append(
                    EntityOutcome.success(entity_id, LogReason.UPDATED, updates=changed)
                )
            else:
                result.outcomes.append(
                    EntityOutcome.failed(
                        entity_id,
                        LogReason.ENTITY_NOT_FOUND,
                        "Entity no longer exists or left the target set before the update ran.",
                    )
                )
        return result
