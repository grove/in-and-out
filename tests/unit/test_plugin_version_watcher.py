"""Unit tests for plugin version watcher (Step 84)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.plugins.version_watcher import get_plugin_versions, watch_plugin_versions


# ---------------------------------------------------------------------------
# get_plugin_versions tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_get_plugin_versions_returns_dict():
    """get_plugin_versions should return a dict."""
    # With real entry points (the 'example' one from pyproject.toml)
    result = await get_plugin_versions()
    assert isinstance(result, dict)


@pytest.mark.anyio
async def test_get_plugin_versions_mock_entry_points():
    """get_plugin_versions should use entry_points and pkg version to build dict."""
    mock_ep = MagicMock()
    mock_ep.value = "inandout.plugins.example_hooks:get_hooks"
    mock_ep.dist = MagicMock()
    mock_ep.dist.name = "inandout"
    mock_ep.name = "example"

    with (
        patch("importlib.metadata.entry_points", return_value=[mock_ep]),
        patch("importlib.metadata.packages_distributions", return_value={
            "inandout": ["inandout"],
        }),
        patch("importlib.metadata.version", return_value="1.2.3"),
    ):
        result = await get_plugin_versions()

    assert isinstance(result, dict)
    assert "inandout" in result
    assert result["inandout"] == "1.2.3"


# ---------------------------------------------------------------------------
# watch_plugin_versions tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_watch_version_change_calls_on_change():
    """Version change between polls should trigger on_change."""
    call_args: list[tuple[str, str, str]] = []

    async def _on_change(pkg: str, old: str, new: str) -> None:
        call_args.append((pkg, old, new))

    poll_count = [0]

    async def _fake_get_versions() -> dict[str, str]:
        poll_count[0] += 1
        if poll_count[0] == 1:
            return {"mypkg": "1.0.0"}
        return {"mypkg": "1.1.0"}  # version changed

    import anyio

    with patch("inandout.plugins.version_watcher.get_plugin_versions", _fake_get_versions):
        # Run watcher with very short interval; cancel after first poll
        async def _run() -> None:
            with anyio.move_on_after(0.1):
                await watch_plugin_versions(_on_change, poll_interval_secs=0.01)

        await _run()

    assert len(call_args) >= 1
    assert call_args[0][0] == "mypkg"
    assert call_args[0][1] == "1.0.0"  # old version
    assert call_args[0][2] == "1.1.0"  # new version


@pytest.mark.anyio
async def test_watch_no_version_change_does_not_call_on_change():
    """No version change → on_change should NOT be called."""
    call_args: list[tuple[str, str, str]] = []

    async def _on_change(pkg: str, old: str, new: str) -> None:
        call_args.append((pkg, old, new))

    async def _fake_get_versions() -> dict[str, str]:
        return {"mypkg": "1.0.0"}  # always the same

    import anyio

    with patch("inandout.plugins.version_watcher.get_plugin_versions", _fake_get_versions):
        async def _run() -> None:
            with anyio.move_on_after(0.05):
                await watch_plugin_versions(_on_change, poll_interval_secs=0.01)

        await _run()

    assert len(call_args) == 0


@pytest.mark.anyio
async def test_watch_new_package_calls_on_change_with_empty_old_version():
    """New package detected → on_change called with empty string as old_version."""
    call_args: list[tuple[str, str, str]] = []

    async def _on_change(pkg: str, old: str, new: str) -> None:
        call_args.append((pkg, old, new))

    poll_count = [0]

    async def _fake_get_versions() -> dict[str, str]:
        poll_count[0] += 1
        if poll_count[0] == 1:
            return {}  # no packages initially
        return {"newpkg": "2.0.0"}  # new package appears

    import anyio

    with patch("inandout.plugins.version_watcher.get_plugin_versions", _fake_get_versions):
        async def _run() -> None:
            with anyio.move_on_after(0.1):
                await watch_plugin_versions(_on_change, poll_interval_secs=0.01)

        await _run()

    assert len(call_args) >= 1
    # Find the call for "newpkg"
    new_pkg_calls = [c for c in call_args if c[0] == "newpkg"]
    assert new_pkg_calls, f"Expected a call for 'newpkg', got: {call_args}"
    assert new_pkg_calls[0][1] == ""  # old_version is empty string
    assert new_pkg_calls[0][2] == "2.0.0"
