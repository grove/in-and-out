"""Connector plugin system."""
from __future__ import annotations

from inandout.plugins.hooks import (
    ConnectorHooks,
    HookRegistry,
    _registry as HookRegistry_instance,
    apply_hooks,
    register_hooks,
)

__all__ = [
    "ConnectorHooks",
    "HookRegistry",
    "HookRegistry_instance",
    "apply_hooks",
    "register_hooks",
]
