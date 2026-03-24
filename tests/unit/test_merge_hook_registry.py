"""Unit tests for MergeHookRegistry in writeback/merge_hooks.py."""
from __future__ import annotations

import pytest

from inandout.writeback.merge_hooks import MergeHookRegistry, merge_hook_registry


async def _dummy_hook(incoming: dict, existing: dict, meta: dict) -> dict:
    return {**existing, **incoming}


async def _other_hook(incoming: dict, existing: dict, meta: dict) -> dict:
    return existing


def test_get_unknown_returns_none():
    reg = MergeHookRegistry()
    assert reg.get("crm", "contacts") is None


def test_register_then_get():
    reg = MergeHookRegistry()
    reg.register("crm", "contacts", _dummy_hook)
    assert reg.get("crm", "contacts") is _dummy_hook


def test_register_overwrites():
    reg = MergeHookRegistry()
    reg.register("crm", "contacts", _dummy_hook)
    reg.register("crm", "contacts", _other_hook)
    assert reg.get("crm", "contacts") is _other_hook


def test_different_datatypes_are_independent():
    reg = MergeHookRegistry()
    reg.register("crm", "contacts", _dummy_hook)
    reg.register("crm", "accounts", _other_hook)
    assert reg.get("crm", "contacts") is _dummy_hook
    assert reg.get("crm", "accounts") is _other_hook


def test_different_connectors_are_independent():
    reg = MergeHookRegistry()
    reg.register("crm", "leads", _dummy_hook)
    reg.register("erp", "leads", _other_hook)
    assert reg.get("crm", "leads") is _dummy_hook
    assert reg.get("erp", "leads") is _other_hook


def test_key_format():
    """The internal key should include both connector and datatype."""
    reg = MergeHookRegistry()
    key = reg._key("my_connector", "my_datatype")
    assert "my_connector" in key
    assert "my_datatype" in key


def test_module_level_singleton_is_instance():
    assert isinstance(merge_hook_registry, MergeHookRegistry)


def test_empty_registry_get_returns_none_for_all():
    reg = MergeHookRegistry()
    for connector, datatype in [("a", "b"), ("c", "d"), ("", "")]:
        assert reg.get(connector, datatype) is None


def test_register_multiple_connectors():
    reg = MergeHookRegistry()
    hooks = {}
    for i in range(5):
        connector = f"conn_{i}"
        async def h(inc, ex, m): return inc  # noqa: E731
        reg.register(connector, "items", h)
        hooks[connector] = h

    for connector, h in hooks.items():
        assert reg.get(connector, "items") is h
