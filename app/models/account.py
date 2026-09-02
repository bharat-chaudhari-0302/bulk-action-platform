"""Tenant boundary. Rate limits and bulk actions are scoped to an account."""

import uuid
from datetime import datetime

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, created_at_col, uuid_pk


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[uuid.UUID] = uuid_pk()
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Processing ceiling in entities/minute. Overrides the global default.
    rate_limit_per_minute: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="10000"
    )
    created_at: Mapped[datetime] = created_at_col()
