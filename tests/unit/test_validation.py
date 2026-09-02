"""Submission-time validation.

Every one of these failures would otherwise surface a million times inside a
worker, an hour after the user pressed the button.
"""

from __future__ import annotations

import pytest

from app.core.errors import ValidationError
from app.domain.actions.registry import get_action
from app.domain.entities.registry import get_entity


@pytest.fixture
def contact():
    return get_entity("contact")


@pytest.fixture
def update_action():
    return get_action("update")


def test_valid_updates_are_coerced_to_declared_types(contact):
    assert contact.validate_updates({"age": "42"}) == {"age": 42}


def test_unknown_field_is_rejected_with_the_allowed_list(contact):
    with pytest.raises(ValidationError) as exc:
        contact.validate_updates({"nickname": "Ada"})
    assert "nickname" in str(exc.value)
    assert "status" in str(exc.value)


def test_out_of_range_value_is_rejected(contact):
    with pytest.raises(ValidationError) as exc:
        contact.validate_updates({"age": 900})
    assert exc.value.extra["errors"][0]["field"] == "age"


def test_malformed_email_is_rejected(contact):
    with pytest.raises(ValidationError):
        contact.validate_updates({"email": "not-an-email"})


def test_empty_updates_are_rejected(contact):
    with pytest.raises(ValidationError):
        contact.validate_updates({})


def test_only_supplied_fields_are_written(contact):
    """Unset fields must not be silently overwritten with NULL."""
    assert contact.validate_updates({"status": "churned"}) == {"status": "churned"}


def test_target_set_must_be_explicit(contact, update_action):
    """Neither filter nor ids means 'everything', which is never assumed."""
    with pytest.raises(ValidationError):
        update_action.validate_config(contact, {"updates": {"status": "x"}})


def test_filter_and_entity_ids_are_mutually_exclusive(contact, update_action):
    with pytest.raises(ValidationError):
        update_action.validate_config(
            contact,
            {
                "updates": {"status": "x"},
                "filter": {},
                "entity_ids": ["3f5e2b1a-0000-4000-8000-000000000000"],
            },
        )


def test_empty_filter_targets_the_whole_account(contact, update_action):
    config = update_action.validate_config(contact, {"updates": {"status": "x"}, "filter": {}})
    assert config.filter == {}


def test_dedup_key_must_be_declared_by_the_entity(contact, update_action):
    with pytest.raises(ValidationError) as exc:
        update_action.validate_config(
            contact, {"updates": {"status": "x"}, "filter": {}, "deduplicate_by": "name"}
        )
    assert "email" in str(exc.value)


def test_company_dedup_key_differs_from_contact():
    """De-duplication is a declared property of the entity, not a hard-coded
    special case for email."""
    company = get_entity("company")
    config = get_action("update").validate_config(
        company, {"updates": {"status": "x"}, "filter": {}, "deduplicate_by": "domain"}
    )
    assert config.deduplicate_by == "domain"


def test_delete_rejects_unknown_payload_keys(contact):
    with pytest.raises(ValidationError):
        get_action("delete").validate_config(contact, {"filter": {}, "hardly": True})
