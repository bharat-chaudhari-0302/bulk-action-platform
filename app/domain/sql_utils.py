"""Small SQL helpers shared by action handlers."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from sqlalchemy import ColumnElement, literal
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PGUUID

_UUID_ARRAY = ARRAY(PGUUID(as_uuid=True))


def pk_in(column: ColumnElement, ids: Sequence[uuid.UUID]) -> ColumnElement[bool]:
    """`id = ANY($1)` rather than `id IN ($1, $2, ... $n)`.

    With a thousand ids per batch the IN form produces a thousand bind
    parameters and a distinct query plan per batch size; ANY(array) is a single
    parameter and one cacheable plan.
    """
    return column == literal(list(ids), _UUID_ARRAY).any_()
