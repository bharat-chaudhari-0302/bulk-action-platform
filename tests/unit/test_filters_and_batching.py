"""Filter compilation and batch sizing."""

from __future__ import annotations

import uuid

import pytest

from app.core.errors import ValidationError
from app.domain.entities.registry import get_entity
from app.domain.filters import build_predicates, selection_clause, validate_filter
from app.services.batching import _plan_from_ids, effective_batch_size
from app.services.dedup import normalise


@pytest.fixture
def contact():
    return get_entity("contact")


# --- Filters --------------------------------------------------------------


def test_shorthand_equality_compiles(contact):
    assert len(build_predicates(contact, {"status": "active"})) == 1


def test_operators_compile(contact):
    predicates = build_predicates(contact, {"age": {"gte": 30, "lt": 60}})
    assert len(predicates) == 2


def test_unknown_field_is_rejected(contact):
    with pytest.raises(ValidationError):
        validate_filter(contact, {"salary": 100})


def test_unknown_operator_is_rejected(contact):
    with pytest.raises(ValidationError):
        validate_filter(contact, {"age": {"approximately": 30}})


def test_empty_in_list_is_rejected(contact):
    with pytest.raises(ValidationError):
        build_predicates(contact, {"status": {"in": []}})


def test_filter_values_are_bound_not_interpolated(contact):
    """A filter value can never become SQL."""
    clause = build_predicates(contact, {"status": "active'; DROP TABLE contacts; --"})[0]
    compiled = str(clause.compile())
    assert "DROP TABLE" not in compiled


def test_selection_is_always_tenant_scoped_and_excludes_soft_deleted(contact):
    compiled = str(
        selection_clause(contact, uuid.uuid4(), None).compile(
            compile_kwargs={"literal_binds": False}
        )
    )
    assert "account_id" in compiled
    assert "deleted_at IS NULL" in compiled


# --- Batch sizing ---------------------------------------------------------


def test_batch_size_defaults_when_unspecified():
    assert effective_batch_size(None, 10_000) == 1000


def test_batch_size_is_clamped_to_the_rate_limit():
    """A batch larger than the per-minute budget could never be admitted."""
    assert effective_batch_size(5000, 600) == 600


def test_batch_size_is_clamped_to_the_configured_maximum():
    assert effective_batch_size(999_999, 10_000_000) == 10_000


def test_batch_size_is_never_zero():
    assert effective_batch_size(0, 10_000) == 1000
    assert effective_batch_size(10, 0) == 1


# --- Explicit-id planning -------------------------------------------------


async def _collect(agen):
    return [item async for item in agen]


@pytest.mark.asyncio
async def test_explicit_ids_are_chunked_and_deduplicated():
    ids = [uuid.uuid4() for _ in range(5)]
    batches = await _collect(_plan_from_ids(None, None, None, ids + ids, 2, False))
    assert [b.entity_count for b in batches] == [2, 2, 1]
    assert sum(b.entity_count for b in batches) == 5  # duplicates dropped


@pytest.mark.asyncio
async def test_explicit_id_batches_carry_their_ids_and_are_ordered():
    ids = [uuid.uuid4() for _ in range(4)]
    batches = await _collect(_plan_from_ids(None, None, None, ids, 10, False))
    assert batches[0].entity_ids == sorted(ids)
    assert batches[0].cursor_start == sorted(ids)[0]


# --- Dedup key normalisation ---------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  Ada@Example.COM ", "ada@example.com"),
        ("ada@example.com", "ada@example.com"),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_dedup_keys_are_normalised(raw, expected):
    assert normalise(raw) == expected
