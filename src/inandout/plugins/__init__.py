"""Connector plugin system.

Public API::

    from inandout.plugins import ConnectorHooks, register_hooks, apply_hooks, _registry
"""
from __future__ import annotations

from inandout.plugins.hooks import (
    ConnectorHooks,
    HookRegistry,
    _registry,
    apply_hooks,
    register_hooks,
)

__all__ = [
    "ConnectorHooks",
    "HookRegistry",
    "_registry",
    "apply_hooks",
    "register_hooks",
]
