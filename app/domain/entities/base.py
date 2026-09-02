"""Entity descriptors: the contract that makes the platform entity-agnostic.

A descriptor is the *only* thing the core knows about a CRM entity. It declares
which table backs the entity, which fields may be written, which may be
filtered, and which field identifies a duplicate. Nothing in the API layer, the
planner or the batch worker references `Contact` or `Company` by name.

Adding a new entity is therefore: one table, one descriptor, one decorator.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar, get_args, get_origin

from pydantic import BaseModel, ConfigDict, create_model
from sqlalchemy import Column, Table

from app.core.errors import ValidationError


@dataclass(frozen=True)
class FieldSpec:
    """Declares one writable/filterable field on an entity.

    `annotation` is a normal typing annotation, optionally wrapped in
    `Annotated[...]` with a Pydantic `Field` for constraints. It is reused
    verbatim to build the per-entity update model, so validation rules live in
    exactly one place.
    """

    annotation: Any
    description: str = ""
    # Whether the column accepts NULL. Controls whether `null` is a legal update.
    nullable: bool = False
    # Fields excluded from filtering (e.g. free-text blobs) can set this False.
    filterable: bool = True

    @property
    def update_annotation(self) -> Any:
        return self.annotation | None if self.nullable else self.annotation

    @property
    def type_name(self) -> str:
        ann = self.annotation
        if get_origin(ann) is Annotated:
            ann = get_args(ann)[0]
        return getattr(ann, "__name__", str(ann))


@dataclass(frozen=True)
class EntityRow:
    """A single entity as seen by an action handler.

    Deliberately a plain mapping rather than an ORM instance: batch workers read
    thousands of rows per job and never need identity-map or lazy-loading
    machinery.
    """

    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


class EntityDescriptor:
    """Base class for entity descriptors. Subclass, set the class vars, register."""

    #: Stable public identifier used in the API (`"contact"`).
    name: ClassVar[str]
    #: Human-readable plural, used in responses.
    label: ClassVar[str] = ""
    #: The SQLAlchemy table backing this entity.
    table: ClassVar[Table]
    #: Fields a bulk update is allowed to write.
    updatable_fields: ClassVar[dict[str, FieldSpec]] = {}
    #: Field names that identify a duplicate entity (e.g. `{"email"}`).
    dedup_fields: ClassVar[frozenset[str]] = frozenset()
    #: Column used for soft deletes; `None` means deletes are hard.
    soft_delete_column: ClassVar[str | None] = "deleted_at"
    #: Column scoping rows to a tenant.
    account_column: ClassVar[str] = "account_id"

    # Built lazily and cached, because create_model is not cheap.
    _update_model: ClassVar[type[BaseModel] | None] = None
    _filter_model: ClassVar[type[BaseModel] | None] = None

    # --- Introspection --------------------------------------------------

    @classmethod
    def column(cls, name: str) -> Column:
        try:
            return cls.table.c[name]
        except KeyError as exc:  # pragma: no cover - guarded by validation
            raise ValidationError(f"Unknown column '{name}' on entity '{cls.name}'.") from exc

    @classmethod
    def pk(cls) -> Column:
        return cls.table.c["id"]

    @classmethod
    def account_col(cls) -> Column:
        return cls.table.c[cls.account_column]

    @classmethod
    def filterable_fields(cls) -> dict[str, FieldSpec]:
        return {k: v for k, v in cls.updatable_fields.items() if v.filterable}

    @classmethod
    def readable_columns(cls) -> list[str]:
        """Columns loaded for each entity before an action runs."""
        cols = ["id", *cls.updatable_fields.keys()]
        if cls.soft_delete_column and cls.soft_delete_column not in cols:
            cols.append(cls.soft_delete_column)
        return cols

    # --- Derived validation models --------------------------------------

    @classmethod
    def update_model(cls) -> type[BaseModel]:
        """Pydantic model validating an `updates` payload for this entity.

        `extra="forbid"` turns a typo in a field name into a 422 at submission
        time rather than a silent no-op across a million rows.
        """
        if cls.__dict__.get("_update_model") is None:
            fields: dict[str, Any] = {
                fname: (spec.update_annotation | None, None)
                for fname, spec in cls.updatable_fields.items()
            }
            cls._update_model = create_model(
                f"{cls.name.title()}Update",
                __config__=ConfigDict(extra="forbid", str_strip_whitespace=True),
                **fields,
            )
        return cls._update_model  # type: ignore[return-value]

    @classmethod
    def validate_updates(cls, updates: dict[str, Any]) -> dict[str, Any]:
        """Validate and coerce an update payload. Raises ValidationError."""
        if not updates:
            raise ValidationError("`updates` must contain at least one field.")
        unknown = set(updates) - set(cls.updatable_fields)
        if unknown:
            raise ValidationError(
                f"Fields not updatable on '{cls.name}': {sorted(unknown)}. "
                f"Allowed: {sorted(cls.updatable_fields)}."
            )
        from pydantic import ValidationError as PydanticValidationError

        try:
            model = cls.update_model()(**updates)
        except PydanticValidationError as exc:
            details = [
                {"field": ".".join(str(p) for p in e["loc"]), "error": e["msg"]}
                for e in exc.errors()
            ]
            raise ValidationError(
                f"Invalid update values for '{cls.name}'.", errors=details
            ) from exc
        # Only the keys the caller actually sent; unset fields must not be
        # written as NULL.
        return model.model_dump(include=set(updates), exclude_unset=False)

    @classmethod
    def describe(cls) -> dict[str, Any]:
        """Self-documentation surfaced by GET /bulk-actions/registry."""
        return {
            "name": cls.name,
            "label": cls.label or cls.name.title(),
            "table": cls.table.name,
            "updatable_fields": {
                fname: {
                    "type": spec.type_name,
                    "nullable": spec.nullable,
                    "filterable": spec.filterable,
                    "description": spec.description,
                }
                for fname, spec in cls.updatable_fields.items()
            },
            "dedup_fields": sorted(cls.dedup_fields),
            "supports_soft_delete": cls.soft_delete_column is not None,
        }
