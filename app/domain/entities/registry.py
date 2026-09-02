"""Entity registry.

Descriptors register themselves with a decorator and are discovered by importing
the package, so a new entity is picked up without editing any central list.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import TypeVar

from app.core.errors import ValidationError
from app.domain.entities.base import EntityDescriptor

_REGISTRY: dict[str, type[EntityDescriptor]] = {}

T = TypeVar("T", bound=type[EntityDescriptor])


def register_entity(descriptor: T) -> T:
    """Class decorator that adds a descriptor to the registry."""
    name = getattr(descriptor, "name", None)
    if not name:
        raise RuntimeError(f"{descriptor.__name__} must define a `name`.")
    if name in _REGISTRY and _REGISTRY[name] is not descriptor:
        raise RuntimeError(f"Entity '{name}' is already registered.")
    _REGISTRY[name] = descriptor
    return descriptor


def get_entity(name: str) -> type[EntityDescriptor]:
    discover_entities()
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ValidationError(
            f"Unknown entity_type '{name}'. Supported: {sorted(_REGISTRY)}."
        ) from None


def all_entities() -> dict[str, type[EntityDescriptor]]:
    discover_entities()
    return dict(_REGISTRY)


_discovered = False


def discover_entities() -> None:
    """Import every module in this package so decorators run exactly once."""
    global _discovered
    if _discovered:
        return
    _discovered = True
    package = importlib.import_module(__package__)
    for module in pkgutil.iter_modules(package.__path__):
        if module.name not in {"base", "registry"}:
            importlib.import_module(f"{__package__}.{module.name}")
