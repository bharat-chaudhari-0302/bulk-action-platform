"""The registries are the extensibility contract, so they get tested hardest."""

from __future__ import annotations

import pytest

from app.core.errors import ValidationError
from app.domain.actions.registry import all_actions, get_action
from app.domain.entities.registry import all_entities, get_entity
from app.services.bulk_action_service import registry_snapshot


def test_shipped_entities_are_discovered_without_a_central_list():
    entities = all_entities()
    assert {"contact", "company"} <= set(entities)


def test_shipped_actions_are_discovered_without_a_central_list():
    actions = all_actions()
    assert {"update", "delete"} <= set(actions)


def test_unknown_entity_is_a_client_error_not_a_crash():
    with pytest.raises(ValidationError) as exc:
        get_entity("unicorn")
    # The message must name what *is* supported, so a caller can self-correct.
    assert "contact" in str(exc.value)


def test_unknown_action_is_a_client_error_not_a_crash():
    with pytest.raises(ValidationError) as exc:
        get_action("teleport")
    assert "update" in str(exc.value)


def test_entity_agnostic_actions_support_every_entity():
    """The point of the design: actions are not written per entity."""
    for action in all_actions().values():
        for entity_name in all_entities():
            assert action.supports(entity_name), (
                f"{action.action_type} unexpectedly rejects {entity_name}"
            )


def test_registry_snapshot_is_the_full_cross_product():
    snapshot = registry_snapshot()
    combos = {
        (c["entity_type"], c["action_type"]) for c in snapshot["supported_combinations"]
    }
    # Two entities x two shipped actions, all from one core implementation.
    # A superset check, not equality: the extensibility suite registers a third
    # action at import time, and that is the behaviour under test there.
    assert combos >= {
        ("contact", "update"),
        ("contact", "delete"),
        ("company", "update"),
        ("company", "delete"),
    }
    assert len(combos) == len(all_entities()) * len(all_actions())


def test_registry_snapshot_publishes_payload_schemas():
    """Clients discover how to call a new action without reading the source."""
    snapshot = registry_snapshot()
    update_action = next(a for a in snapshot["actions"] if a["action_type"] == "update")
    assert "updates" in update_action["payload_schema"]["properties"]


def test_registering_a_duplicate_action_type_is_rejected():
    from app.domain.actions.base import BulkActionHandler
    from app.domain.actions.bulk_update import BulkUpdateConfig
    from app.domain.actions.registry import register_action

    with pytest.raises(RuntimeError, match="already registered"):

        @register_action
        class Clashing(BulkActionHandler):
            action_type = "update"
            ConfigModel = BulkUpdateConfig
