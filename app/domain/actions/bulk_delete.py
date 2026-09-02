"""Bulk delete: soft (default) or hard removal of every entity in the target set.

This file is the extensibility proof. It was added after the platform was
complete, and adding it required no change to the API layer, the planner, the
batch worker, the rate limiter, the de-duplicator, the logging pipeline or any
migration -- only this module and its `@register_action` decorator.
"""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import delete, func, update

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


class BulkDeleteConfig(BaseModel):
    """Request payload for `action_type: "delete"`."""

    model_config = ConfigDict(extra="forbid")

    filter: dict[str, Any] | None = Field(
        default=None,
        description="Selects the target set. `{}` means every entity in the account.",
    )
    entity_ids: list[uuid.UUID] | None = Field(
        default=None, description="Explicit target ids, as an alternative to `filter`."
    )
    hard: bool = Field(
        default=False,
        description="Permanently remove rows instead of setting the soft-delete column.",
    )
    deduplicate_by: str | None = Field(
        default=None, description="Entity field on which duplicates are skipped."
    )

    @model_validator(mode="after")
    def _require_a_target_set(self) -> BulkDeleteConfig:
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
class BulkDeleteAction(BulkActionHandler):
    action_type: ClassVar[str] = "delete"
    description: ClassVar[str] = (
        "Soft-delete (or, with `hard: true`, permanently remove) every entity in the target set."
    )
    supported_entities: ClassVar[str] = "*"
    ConfigModel: ClassVar[type[BaseModel]] = BulkDeleteConfig

    def validate_config(
        self, entity: type[EntityDescriptor], payload: dict[str, Any]
    ) -> BulkDeleteConfig:
        config: BulkDeleteConfig = super().validate_config(entity, payload)  # type: ignore[assignment]
        validate_filter(entity, config.filter)

        if not config.hard and entity.soft_delete_column is None:
            raise ValidationError(
                f"Entity '{entity.name}' has no soft-delete column; pass `hard: true` "
                f"to delete permanently."
            )
        if config.deduplicate_by and config.deduplicate_by not in entity.dedup_fields:
            raise ValidationError(
                f"'{config.deduplicate_by}' is not a de-duplication key for "
                f"'{entity.name}'. Available: {sorted(entity.dedup_fields)}."
            )
        return config

    async def execute(self, ctx: ActionContext, rows: list[EntityRow]) -> BatchResult:
        result = BatchResult()
        if not rows:
            return result

        config: BulkDeleteConfig = ctx.config  # type: ignore[assignment]
        entity = ctx.entity
        ids = [row.id for row in rows]

        if config.hard:
            stmt = (
                delete(entity.table)
                .where(pk_in(entity.pk(), ids))
                .where(entity.account_col() == ctx.account_id)
                .returning(entity.pk())
            )
        else:
            values: dict[str, Any] = {entity.soft_delete_column: func.now()}
            if "updated_at" in entity.table.c:
                values["updated_at"] = func.now()
            stmt = (
                update(entity.table)
                .where(pk_in(entity.pk(), ids))
                .where(entity.account_col() == ctx.account_id)
                .values(**values)
                .returning(entity.pk())
            )

        affected = {r[0] for r in (await ctx.session.execute(stmt)).all()}

        for entity_id in ids:
            if entity_id in affected:
                result.outcomes.append(
                    EntityOutcome.success(entity_id, LogReason.DELETED, hard=config.hard)
                )
            else:
                result.outcomes.append(
                    EntityOutcome.failed(
                        entity_id,
                        LogReason.ENTITY_NOT_FOUND,
                        "Entity no longer exists or left the target set before the delete ran.",
                    )
                )
        return result
