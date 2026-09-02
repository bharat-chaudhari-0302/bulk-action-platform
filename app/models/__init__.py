"""SQLAlchemy models. Imported here so Alembic autogenerate sees every table."""

from app.models.account import Account
from app.models.base import Base
from app.models.bulk_action import (
    BulkAction,
    BulkActionBatch,
    BulkActionDedup,
    BulkActionLog,
)
from app.models.crm import Company, Contact

__all__ = [
    "Account",
    "Base",
    "BulkAction",
    "BulkActionBatch",
    "BulkActionDedup",
    "BulkActionLog",
    "Company",
    "Contact",
]
