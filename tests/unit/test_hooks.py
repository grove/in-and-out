"""Unit tests for connector plugin hooks."""
from __future__ import annotations

import pytest

from inandout.plugins.hooks import (
    ConnectorHooks,
    HookRegistry,
    apply_hooks,
    _registry,
)


@pytest.fixture(autouse=True)
def clear_registry():
    _registry.clear()
    yield
    _registry.clear()


# ---------------------------------------------------------------------------
# transform hook
# ---------------------------------------------------------------------------

async def test_transform_hook_mutates_field():
    async def _transform(record: dict) -> dict:
        record = dict(record)
        record["name"] = record["name"].upper()
        return record

    _registry.register("myconn", ConnectorHooks(transform=_transform))

    record = {"id": "1", "name": "alice"}
    result = await apply_hooks(record, "myconn")
    assert result is not None
    assert result["name"] == "ALICE"
    assert result["id"] == "1"


# ---------------------------------------------------------------------------
# filter hook
# ---------------------------------------------------------------------------

async def test_filter_hook_drops_records_returning_false():
    async def _filter(record: dict) -> bool:
        return record.get("active", False)

    _registry.register("myconn", ConnectorHooks(filter=_filter))

    active_record = {"id": "1", "active": True}
    inactive_record = {"id": "2", "active": False}

    assert await apply_hooks(active_record, "myconn") is not None
    assert await apply_hooks(inactive_record, "myconn") is None


async def test_filter_hook_keeps_records_returning_true():
    async def _filter(record: dict) -> bool:
        return True

    _registry.register("myconn", ConnectorHooks(filter=_filter))
    record = {"id": "42", "value": "hello"}
    result = await apply_hooks(record, "myconn")
    assert result == record


# ---------------------------------------------------------------------------
# No hooks — pass through unchanged
# ---------------------------------------------------------------------------

async def test_no_hooks_record_passes_through_unchanged():
    record = {"id": "99", "name": "bob", "score": 3.14}
    result = await apply_hooks(record, "connector_with_no_hooks")
    assert result == record


async def test_unregistered_connector_returns_record_unchanged():
    record = {"x": 1}
    result = await apply_hooks(record, "ghost_connector")
    assert result is record


# ---------------------------------------------------------------------------
# transform + filter in sequence
# ---------------------------------------------------------------------------

async def test_transform_then_filter_applied_in_order():
    calls: list[str] = []

    async def _transform(record: dict) -> dict:
        calls.append("transform")
        record = dict(record)
        record["transformed"] = True
        return record

    async def _filter(record: dict) -> bool:
        calls.append("filter")
        # Only keep records that were transformed
        return record.get("transformed", False)

    _registry.register("myconn", ConnectorHooks(transform=_transform, filter=_filter))

    result = await apply_hooks({"id": "1"}, "myconn")
    assert result is not None
    assert result["transformed"] is True
    assert calls == ["transform", "filter"]


# ---------------------------------------------------------------------------
# HookRegistry
# ---------------------------------------------------------------------------

def test_registry_get_returns_none_for_unknown():
    reg = HookRegistry()
    assert reg.get("unknown") is None


def test_registry_register_and_get():
    reg = HookRegistry()
    hooks = ConnectorHooks()
    reg.register("conn_a", hooks)
    assert reg.get("conn_a") is hooks


def test_registry_clear():
    reg = HookRegistry()
    reg.register("conn_b", ConnectorHooks())
    reg.clear()
    assert reg.get("conn_b") is None
