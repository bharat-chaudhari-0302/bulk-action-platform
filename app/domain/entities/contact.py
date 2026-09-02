"""Contact entity descriptor.

This file is the entire definition of "Contact" as far as the bulk action
platform is concerned. Compare with company.py: the two differ only in their
declarations, and every action works against both.
"""

from typing import Annotated, cast

from pydantic import Field
from sqlalchemy import Table

from app.domain.entities.base import EntityDescriptor, FieldSpec
from app.domain.entities.registry import register_entity
from app.models.crm import Contact


@register_entity
class ContactEntity(EntityDescriptor):
    name = "contact"
    label = "Contacts"
    table = cast(Table, Contact.__table__)

    updatable_fields = {
        "name": FieldSpec(
            annotation=Annotated[str, Field(min_length=1, max_length=255)],
            description="Full name of the contact.",
        ),
        "email": FieldSpec(
            # Validated as an email at submission time, stored as text.
            annotation=Annotated[
                str,
                Field(
                    min_length=3,
                    max_length=320,
                    pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                ),
            ],
            description="Primary email address. Also the de-duplication key.",
        ),
        "age": FieldSpec(
            annotation=Annotated[int, Field(ge=0, le=150)],
            description="Age in years.",
            nullable=True,
        ),
        "status": FieldSpec(
            annotation=Annotated[str, Field(min_length=1, max_length=50)],
            description="Lifecycle status, e.g. active / inactive / churned.",
        ),
    }

    dedup_fields = frozenset({"email"})
    soft_delete_column = "deleted_at"
