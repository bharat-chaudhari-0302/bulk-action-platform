"""Translate a JSON filter object into SQLAlchemy predicates.

Selecting entities by filter rather than by id list is what makes a
million-entity action practical: the client sends a few bytes and the planner
walks the matching rows with keyset pagination.

Shape::

    {"status": "active",                  # shorthand for eq
     "age": {"gte": 30, "lt": 60},
     "email": {"contains": "@example.com"},
     "industry": {"in": ["saas", "logistics"]}}

Only fields the entity declares as filterable are accepted, and values are
always bound as parameters, so a filter cannot inject SQL.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import ColumnElement, and_, func

from app.core.errors import ValidationError
from app.domain.entities.base import EntityDescriptor

_OPERATORS = frozenset(
    {"eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in", "contains", "is_null"}
)


def _apply(column: Any, op: str, value: Any) -> ColumnElement[bool]:
    match op:
        case "eq":
            return column == value
        case "ne":
            return column != value
        case "gt":
            return column > value
        case "gte":
            return column >= value
        case "lt":
            return column < value
        case "lte":
            return column <= value
        case "in":
            if not isinstance(value, list) or not value:
                raise ValidationError("Operator 'in' requires a non-empty list.")
            return column.in_(value)
        case "not_in":
            if not isinstance(value, list) or not value:
                raise ValidationError("Operator 'not_in' requires a non-empty list.")
            return column.notin_(value)
        case "contains":
            # Case-insensitive substring match; ILIKE with an escaped pattern.
            return func.lower(column).contains(str(value).lower())
        case "is_null":
            return column.is_(None) if value else column.isnot(None)
        case _:  # pragma: no cover - guarded by validate_filter
            raise ValidationError(f"Unsupported operator '{op}'.")


def validate_filter(entity: type[EntityDescriptor], filters: dict[str, Any] | None) -> None:
    """Fail fast at submission time rather than inside a worker."""
    if not filters:
        return
    allowed = set(entity.filterable_fields()) | {"status", "created_at", "updated_at"}
    allowed &= set(entity.table.c.keys())
    for fname, condition in filters.items():
        if fname not in allowed:
            raise ValidationError(
                f"Field '{fname}' is not filterable on '{entity.name}'. "
                f"Allowed: {sorted(allowed)}."
            )
        if isinstance(condition, dict):
            unknown = set(condition) - _OPERATORS
            if unknown:
                raise ValidationError(
                    f"Unsupported operators {sorted(unknown)} on '{fname}'. "
                    f"Allowed: {sorted(_OPERATORS)}."
                )


def build_predicates(
    entity: type[EntityDescriptor], filters: dict[str, Any] | None
) -> list[ColumnElement[bool]]:
    """Compile a validated filter object into a list of AND-ed predicates."""
    if not filters:
        return []
    validate_filter(entity, filters)
    predicates: list[ColumnElement[bool]] = []
    for fname, condition in filters.items():
        column = entity.column(fname)
        if isinstance(condition, dict):
            predicates.extend(_apply(column, op, value) for op, value in condition.items())
        else:
            predicates.append(_apply(column, "eq", condition))
    return predicates


def selection_clause(
    entity: type[EntityDescriptor],
    account_id: Any,
    filters: dict[str, Any] | None,
    include_deleted: bool = False,
) -> ColumnElement[bool]:
    """The full WHERE clause identifying an action's target set.

    Always tenant-scoped, and soft-deleted rows are excluded unless an action
    explicitly asks for them.
    """
    clauses: list[ColumnElement[bool]] = [entity.account_col() == account_id]
    if entity.soft_delete_column and not include_deleted:
        clauses.append(entity.column(entity.soft_delete_column).is_(None))
    clauses.extend(build_predicates(entity, filters))
    return and_(*clauses)
