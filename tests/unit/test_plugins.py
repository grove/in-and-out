"""Unit tests for the inandout.plugins package.

Covers:
- ConnectorHooks dataclass
- HookRegistry: register / get / clear
- apply_hooks: transform-only, filter-drop, enrich, combined, no-ops
- register_hooks convenience wrapper
"""
from __future__ import annotations

import pytest


# ── Helpers ──────────────────────────────────────────────────────────────────


async def _double_value(record: dict) -> dict:
    """Transform: doubles the 'value' field."""
    return {**record, "value": record.get("value", 0) * 2}


async def _keep_all(record: dict) -> bool:
    return True


async def _drop_all(record: dict) -> bool:
    return False


async def _add_tag(record: dict, pool) -> dict:  # noqa: ANN001
    """Enrich: appends '_enriched' to record['tag']."""
    return {**record, "tag": record.get("tag", "") + "_enriched"}


# ── ConnectorHooks ────────────────────────────────────────────────────────────


def test_connector_hooks_all_none_by_default() -> None:
    from inandout.plugins.hooks import ConnectorHooks

    h = ConnectorHooks()
    assert h.transform is None
    assert h.filter is None
    assert h.enrich is None


def test_connector_hooks_stores_callables() -> None:
    from inandout.plugins.hooks import ConnectorHooks

    h = ConnectorHooks(transform=_double_value, filter=_keep_all, enrich=_add_tag)
    assert h.transform is _double_value
    assert h.filter is _keep_all
    assert h.enrich is _add_tag


# ── HookRegistry ─────────────────────────────────────────────────────────────


def test_registry_register_and_get() -> None:
    from inandout.plugins.hooks import ConnectorHooks, HookRegistry

    reg = HookRegistry()
    hooks = ConnectorHooks(transform=_double_value)
    reg.register("my_connector", hooks)
    assert reg.get("my_connector") is hooks


def test_registry_get_missing_returns_none() -> None:
    from inandout.plugins.hooks import HookRegistry

    reg = HookRegistry()
    assert reg.get("nonexistent") is None


def test_registry_register_replaces_existing() -> None:
    from inandout.plugins.hooks import ConnectorHooks, HookRegistry

    reg = HookRegistry()
    old = ConnectorHooks(transform=_double_value)
    new = ConnectorHooks(enrich=_add_tag)
    reg.register("c", old)
    reg.register("c", new)
    assert reg.get("c") is new


def test_registry_clear_removes_all() -> None:
    from inandout.plugins.hooks import ConnectorHooks, HookRegistry

    reg = HookRegistry()
    reg.register("a", ConnectorHooks())
    reg.register("b", ConnectorHooks())
    reg.clear()
    assert reg.get("a") is None
    assert reg.get("b") is None


# ── register_hooks convenience wrapper ───────────────────────────────────────


def test_register_hooks_writes_to_singleton() -> None:
    from inandout.plugins.hooks import ConnectorHooks, _registry, register_hooks

    _registry.clear()
    h = ConnectorHooks(transform=_double_value)
    register_hooks("singleton_test", h)
    assert _registry.get("singleton_test") is h
    _registry.clear()  # tidy up


# ── apply_hooks ──────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_apply_hooks_no_hooks_returns_record_unchanged() -> None:
    from inandout.plugins.hooks import _registry, apply_hooks

    _registry.clear()
    record = {"id": 1, "value": 7}
    result = await apply_hooks(record, "no_hooks_connector", pool=None)
    assert result == record


@pytest.mark.anyio
async def test_apply_hooks_transform_mutates_record() -> None:
    from inandout.plugins.hooks import ConnectorHooks, _registry, apply_hooks

    _registry.clear()
    _registry.register("t_connector", ConnectorHooks(transform=_double_value))
    record = {"value": 5}
    result = await apply_hooks(record, "t_connector", pool=None)
    assert result == {"value": 10}
    _registry.clear()


@pytest.mark.anyio
async def test_apply_hooks_filter_drop_returns_none() -> None:
    from inandout.plugins.hooks import ConnectorHooks, _registry, apply_hooks

    _registry.clear()
    _registry.register("f_connector", ConnectorHooks(filter=_drop_all))
    result = await apply_hooks({"id": 99}, "f_connector", pool=None)
    assert result is None
    _registry.clear()


@pytest.mark.anyio
async def test_apply_hooks_filter_keep_passes_through() -> None:
    from inandout.plugins.hooks import ConnectorHooks, _registry, apply_hooks

    _registry.clear()
    _registry.register("fk_connector", ConnectorHooks(filter=_keep_all))
    record = {"id": 3}
    result = await apply_hooks(record, "fk_connector", pool=None)
    assert result == record
    _registry.clear()


@pytest.mark.anyio
async def test_apply_hooks_enrich_augments_record() -> None:
    from inandout.plugins.hooks import ConnectorHooks, _registry, apply_hooks

    _registry.clear()
    _registry.register("e_connector", ConnectorHooks(enrich=_add_tag))
    result = await apply_hooks({"tag": "hello"}, "e_connector", pool=None)
    assert result == {"tag": "hello_enriched"}
    _registry.clear()


@pytest.mark.anyio
async def test_apply_hooks_combined_transform_filter_enrich() -> None:
    """transform doubles value, filter keeps it, enrich appends tag."""
    from inandout.plugins.hooks import ConnectorHooks, _registry, apply_hooks

    _registry.clear()
    _registry.register(
        "combo",
        ConnectorHooks(transform=_double_value, filter=_keep_all, enrich=_add_tag),
    )
    result = await apply_hooks({"value": 3, "tag": "x"}, "combo", pool=None)
    assert result == {"value": 6, "tag": "x_enriched"}
    _registry.clear()


@pytest.mark.anyio
async def test_apply_hooks_explicit_hooks_bypasses_registry() -> None:
    """Passing hooks=... directly should ignore the registry."""
    from inandout.plugins.hooks import ConnectorHooks, _registry, apply_hooks

    _registry.clear()
    _registry.register("c", ConnectorHooks(filter=_drop_all))  # would drop
    explicit = ConnectorHooks(transform=_double_value)          # but we pass explicitly
    result = await apply_hooks({"value": 4}, "c", pool=None, hooks=explicit)
    assert result == {"value": 8}  # not dropped, doubled instead
    _registry.clear()


@pytest.mark.anyio
async def test_apply_hooks_transform_then_filter_drop() -> None:
    """Transform runs first; filter sees the transformed record."""

    async def _transform(rec: dict) -> dict:
        return {**rec, "keep": False}

    async def _filter(rec: dict) -> bool:
        return rec.get("keep", True)

    from inandout.plugins.hooks import ConnectorHooks, apply_hooks

    result = await apply_hooks(
        {"id": 1},
        "x",
        pool=None,
        hooks=ConnectorHooks(transform=_transform, filter=_filter),
    )
    assert result is None


# ── example_hooks ─────────────────────────────────────────────────────────────


def test_example_hooks_get_hooks_returns_dict() -> None:
    from inandout.plugins.example_hooks import get_hooks

    result = get_hooks()
    assert isinstance(result, dict)
    assert "example_connector" in result


@pytest.mark.anyio
async def test_example_hooks_transform_is_noop() -> None:
    from inandout.plugins.example_hooks import get_hooks

    hooks = get_hooks()["example_connector"]
    record = {"id": 1, "name": "Alice"}
    result = await hooks.transform(record)  # type: ignore[misc]
    assert result == record
