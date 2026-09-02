"""Action registry.

Mirrors the entity registry: handlers self-register with a decorator and are
discovered by importing the package. Adding `app/domain/actions/bulk_archive.py`
with `@register_action` makes `"archive"` a valid `action_type` on every
compatible entity, with no change anywhere else in the codebase.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import TypeVar

from app.core.errors import ValidationError
from app.domain.actions.base import BulkActionHandler

_REGISTRY: dict[str, BulkActionHandler] = {}

T = TypeVar("T", bound=type[BulkActionHandler])


def register_action(handler_cls: T) -> T:
    """Class decorator that instantiates a handler and adds it to the registry.

    Handlers are stateless singletons: all per-request state travels in
    ActionContext, so one instance can serve every concurrent job.
    """
    action_type = getattr(handler_cls, "action_type", None)
    if not action_type:
        raise RuntimeError(f"{handler_cls.__name__} must define an `action_type`.")
    if action_type in _REGISTRY and type(_REGISTRY[action_type]) is not handler_cls:
        raise RuntimeError(f"Action '{action_type}' is already registered.")
    _REGISTRY[action_type] = handler_cls()
    return handler_cls


def get_action(action_type: str, entity_name: str | None = None) -> BulkActionHandler:
    discover_actions()
    try:
        handler = _REGISTRY[action_type]
    except KeyError:
        raise ValidationError(
            f"Unknown action_type '{action_type}'. Supported: {sorted(_REGISTRY)}."
        ) from None
    if entity_name is not None and not handler.supports(entity_name):
        raise ValidationError(
            f"Action '{action_type}' does not support entity '{entity_name}'."
        )
    return handler


def all_actions() -> dict[str, BulkActionHandler]:
    discover_actions()
    return dict(_REGISTRY)


_discovered = False


def discover_actions() -> None:
    global _discovered
    if _discovered:
        return
    _discovered = True
    package = importlib.import_module(__package__)
    for module in pkgutil.iter_modules(package.__path__):
        if module.name not in {"base", "registry"}:
            importlib.import_module(f"{__package__}.{module.name}")
