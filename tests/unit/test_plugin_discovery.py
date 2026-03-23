"""Unit tests for plugin hook auto-discovery via entry points."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from inandout.plugins.discovery import discover_and_register_hooks
from inandout.plugins.hooks import ConnectorHooks, HookRegistry


# ---------------------------------------------------------------------------
# discover_and_register_hooks tests
# ---------------------------------------------------------------------------

def _make_mock_entry_point(name: str, factory_return: dict) -> MagicMock:
    """Create a mock entry point that returns factory_return when loaded and called."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"fake_module:{name}"
    factory = MagicMock(return_value=factory_return)
    ep.load = MagicMock(return_value=factory)
    return ep


def test_discover_and_register_hooks_calls_factories():
    """discover_and_register_hooks should call each entry point factory."""
    registry = HookRegistry()

    async def _noop(record: dict) -> dict:
        return record

    hooks_map = {"test_connector": ConnectorHooks(transform=_noop)}
    ep = _make_mock_entry_point("test_plugin", hooks_map)

    with patch("inandout.plugins.discovery.entry_points", return_value=[ep]):
        count = discover_and_register_hooks(registry=registry)

    assert count == 1
    assert registry.get("test_connector") is not None


def test_discover_and_register_hooks_registers_in_registry():
    """Discovered hooks should be registered in the provided HookRegistry."""
    registry = HookRegistry()

    async def _filter(record: dict) -> bool:
        return True

    hooks_map = {"my_connector": ConnectorHooks(filter=_filter)}
    ep = _make_mock_entry_point("my_plugin", hooks_map)

    with patch("inandout.plugins.discovery.entry_points", return_value=[ep]):
        discover_and_register_hooks(registry=registry)

    registered = registry.get("my_connector")
    assert registered is not None
    assert registered.filter is _filter


def test_discover_and_register_hooks_returns_count():
    """discover_and_register_hooks should return count of registered hook sets."""
    registry = HookRegistry()

    async def _noop(record: dict) -> dict:
        return record

    hooks_map = {
        "connector_a": ConnectorHooks(transform=_noop),
        "connector_b": ConnectorHooks(transform=_noop),
    }
    ep = _make_mock_entry_point("multi_plugin", hooks_map)

    with patch("inandout.plugins.discovery.entry_points", return_value=[ep]):
        count = discover_and_register_hooks(registry=registry)

    assert count == 2


def test_invalid_entry_point_skipped_not_raised():
    """Entry point that raises on load should be logged and skipped, not raised."""
    registry = HookRegistry()
    bad_ep = MagicMock()
    bad_ep.name = "broken_plugin"
    bad_ep.value = "nonexistent.module:get_hooks"
    bad_ep.load = MagicMock(side_effect=ImportError("Module not found"))

    with patch("inandout.plugins.discovery.entry_points", return_value=[bad_ep]):
        count = discover_and_register_hooks(registry=registry)

    # Should return 0 (nothing registered) without raising
    assert count == 0


def test_factory_raising_exception_skipped():
    """Entry point factory that raises should be skipped, not raised."""
    registry = HookRegistry()
    ep = MagicMock()
    ep.name = "crashing_plugin"
    ep.value = "fake:module"
    factory = MagicMock(side_effect=RuntimeError("Factory crashed"))
    ep.load = MagicMock(return_value=factory)

    with patch("inandout.plugins.discovery.entry_points", return_value=[ep]):
        count = discover_and_register_hooks(registry=registry)

    assert count == 0


def test_factory_returning_non_dict_skipped():
    """Entry point factory returning non-dict should be skipped."""
    registry = HookRegistry()
    ep = MagicMock()
    ep.name = "bad_return_plugin"
    factory = MagicMock(return_value="not a dict")
    ep.load = MagicMock(return_value=factory)

    with patch("inandout.plugins.discovery.entry_points", return_value=[ep]):
        count = discover_and_register_hooks(registry=registry)

    assert count == 0


def test_multiple_entry_points_all_registered():
    """Multiple entry points should all be processed."""
    registry = HookRegistry()

    async def _noop(record: dict) -> dict:
        return record

    ep1 = _make_mock_entry_point("plugin_1", {"conn_1": ConnectorHooks(transform=_noop)})
    ep2 = _make_mock_entry_point("plugin_2", {"conn_2": ConnectorHooks(transform=_noop)})

    with patch("inandout.plugins.discovery.entry_points", return_value=[ep1, ep2]):
        count = discover_and_register_hooks(registry=registry)

    assert count == 2
    assert registry.get("conn_1") is not None
    assert registry.get("conn_2") is not None


def test_no_entry_points_returns_zero():
    """When no entry points are installed, should return 0."""
    registry = HookRegistry()

    with patch("inandout.plugins.discovery.entry_points", return_value=[]):
        count = discover_and_register_hooks(registry=registry)

    assert count == 0


# ---------------------------------------------------------------------------
# Self-referential example entry point tests
# ---------------------------------------------------------------------------

def test_example_hooks_module_is_importable():
    """The example_hooks module should be importable without errors."""
    from inandout.plugins.example_hooks import get_hooks
    assert callable(get_hooks)


def test_example_hooks_get_hooks_returns_dict():
    """get_hooks() should return a dict mapping connector names to ConnectorHooks."""
    from inandout.plugins.example_hooks import get_hooks
    result = get_hooks()
    assert isinstance(result, dict)
    assert len(result) > 0


def test_example_hooks_contains_example_connector():
    """get_hooks() should contain an 'example_connector' entry."""
    from inandout.plugins.example_hooks import get_hooks
    result = get_hooks()
    assert "example_connector" in result


def test_example_hooks_connector_has_transform():
    """The example_connector hooks should have a transform function."""
    from inandout.plugins.example_hooks import get_hooks
    hooks = get_hooks()["example_connector"]
    assert hooks.transform is not None
    assert callable(hooks.transform)


@pytest.mark.anyio
async def test_example_noop_transform_returns_record_unchanged():
    """The noop transform should return the record unchanged."""
    from inandout.plugins.example_hooks import _noop_transform
    record = {"id": "1", "name": "Test", "value": 42}
    result = await _noop_transform(record)
    assert result == record
