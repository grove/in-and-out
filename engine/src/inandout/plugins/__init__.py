"""Connector plugin system.

Public API::

    from inandout.plugins import ConnectorHooks, register_hooks, apply_hooks
    from inandout.plugins import WritebackHooks, register_writeback_hooks, apply_writeback_hooks
"""
from __future__ import annotations

from inandout.plugins.hooks import (
    ConnectorHooks,
    HookRegistry,
    _registry,
    apply_hooks,
    register_hooks,
)
from inandout.writeback.hooks import (
    WritebackHooks,
    WritebackHookRegistry,
    apply_writeback_hooks,
    register_writeback_hooks,
)

__all__ = [
    "ConnectorHooks",
    "HookRegistry",
    "_registry",
    "apply_hooks",
    "register_hooks",
    "WritebackHooks",
    "WritebackHookRegistry",
    "apply_writeback_hooks",
    "register_writeback_hooks",
]
