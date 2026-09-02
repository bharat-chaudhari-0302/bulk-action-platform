"""Company entity descriptor.

Exists to prove the entity-agnostic claim: adding it required this file and a
table, and nothing else. Every registered action -- update, delete, and any
future one -- works against it immediately.
"""

from typing import Annotated, cast

from pydantic import Field
from sqlalchemy import Table

from app.domain.entities.base import EntityDescriptor, FieldSpec
from app.domain.entities.registry import register_entity
from app.models.crm import Company


@register_entity
class CompanyEntity(EntityDescriptor):
    name = "company"
    label = "Companies"
    table = cast(Table, Company.__table__)

    updatable_fields = {
        "name": FieldSpec(
            annotation=Annotated[str, Field(min_length=1, max_length=255)],
            description="Registered company name.",
        ),
        "domain": FieldSpec(
            annotation=Annotated[str, Field(min_length=3, max_length=255)],
            description="Primary web domain. Doubles as the de-duplication key.",
            nullable=True,
        ),
        "industry": FieldSpec(
            annotation=Annotated[str, Field(min_length=1, max_length=100)],
            description="Industry classification.",
            nullable=True,
        ),
        "employee_count": FieldSpec(
            annotation=Annotated[int, Field(ge=0, le=10_000_000)],
            description="Headcount.",
            nullable=True,
        ),
        "status": FieldSpec(
            annotation=Annotated[str, Field(min_length=1, max_length=50)],
            description="Lifecycle status, e.g. active / prospect / churned.",
        ),
    }

    # A different dedup key from Contact, showing the mechanism is not
    # email-specific even though the assignment only asks for email.
    dedup_fields = frozenset({"domain"})
    soft_delete_column = "deleted_at"
