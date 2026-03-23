"""Unit tests for connector config hot-reload via file watcher."""
from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# watch_connectors_dir: filters to only .yaml files
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_watch_connectors_dir_filters_to_yaml():
    """watch_connectors_dir should only yield changes for .yaml files."""
    from watchfiles import Change

    # Simulate mixed changes: yaml, json, txt
    mock_changes = [
        {(Change.modified, "/connectors/hub.yaml"), (Change.modified, "/connectors/notes.txt")},
        {(Change.added, "/connectors/new.yaml"), (Change.modified, "/connectors/data.json")},
    ]

    yielded: list[set[Path]] = []

    async def mock_awatch(path: Any):
        for change_set in mock_changes:
            yield change_set

    with patch("inandout.ingestion.watcher.awatch", mock_awatch):
        from inandout.ingestion.watcher import watch_connectors_dir
        async for paths in watch_connectors_dir(Path("/connectors")):
            yielded.append(paths)

    assert len(yielded) == 2
    assert all(p.suffix == ".yaml" for paths in yielded for p in paths)
    # First batch: hub.yaml only
    first_names = {p.name for p in yielded[0]}
    assert "hub.yaml" in first_names
    assert "notes.txt" not in first_names
    # Second batch: new.yaml only
    second_names = {p.name for p in yielded[1]}
    assert "new.yaml" in second_names
    assert "data.json" not in second_names


@pytest.mark.anyio
async def test_watch_connectors_dir_ignores_non_yaml_only_changes():
    """watch_connectors_dir yields nothing when only non-YAML files change."""
    from watchfiles import Change

    mock_changes = [
        {(Change.modified, "/connectors/notes.txt"), (Change.modified, "/connectors/data.json")},
    ]

    yielded: list[set[Path]] = []

    async def mock_awatch(path: Any):
        for change_set in mock_changes:
            yield change_set

    with patch("inandout.ingestion.watcher.awatch", mock_awatch):
        from inandout.ingestion.watcher import watch_connectors_dir
        async for paths in watch_connectors_dir(Path("/connectors")):
            yielded.append(paths)

    # No YAML files changed — nothing yielded
    assert len(yielded) == 0


# ---------------------------------------------------------------------------
# hot_reload_loop: calls on_change when a YAML changes
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_hot_reload_loop_calls_on_change():
    """hot_reload_loop calls on_change callback with changed paths."""
    from watchfiles import Change

    mock_changes = [
        {(Change.modified, "/connectors/hub.yaml")},
    ]

    on_change_calls: list[set[Path]] = []

    async def on_change(changed_paths: set[Path]) -> None:
        on_change_calls.append(changed_paths)

    async def mock_awatch(path: Any):
        for change_set in mock_changes:
            yield change_set

    with patch("inandout.ingestion.watcher.awatch", mock_awatch):
        from inandout.ingestion.watcher import hot_reload_loop
        await hot_reload_loop(Path("/connectors"), on_change)

    assert len(on_change_calls) == 1
    assert Path("/connectors/hub.yaml") in on_change_calls[0]


@pytest.mark.anyio
async def test_hot_reload_loop_non_yaml_ignored():
    """hot_reload_loop does not call on_change when only non-YAML files change."""
    from watchfiles import Change

    mock_changes = [
        {(Change.modified, "/connectors/readme.txt")},
    ]

    on_change_calls: list[set[Path]] = []

    async def on_change(changed_paths: set[Path]) -> None:
        on_change_calls.append(changed_paths)

    async def mock_awatch(path: Any):
        for change_set in mock_changes:
            yield change_set

    with patch("inandout.ingestion.watcher.awatch", mock_awatch):
        from inandout.ingestion.watcher import hot_reload_loop
        await hot_reload_loop(Path("/connectors"), on_change)

    # on_change never called since no YAML files changed
    assert len(on_change_calls) == 0


@pytest.mark.anyio
async def test_hot_reload_loop_multiple_yaml_files():
    """hot_reload_loop calls on_change with multiple YAML paths when multiple change."""
    from watchfiles import Change

    mock_changes = [
        {
            (Change.modified, "/connectors/hub.yaml"),
            (Change.added, "/connectors/salesforce.yaml"),
            (Change.modified, "/connectors/notes.txt"),
        },
    ]

    on_change_calls: list[set[Path]] = []

    async def on_change(changed_paths: set[Path]) -> None:
        on_change_calls.append(changed_paths)

    async def mock_awatch(path: Any):
        for change_set in mock_changes:
            yield change_set

    with patch("inandout.ingestion.watcher.awatch", mock_awatch):
        from inandout.ingestion.watcher import hot_reload_loop
        await hot_reload_loop(Path("/connectors"), on_change)

    assert len(on_change_calls) == 1
    changed_names = {p.name for p in on_change_calls[0]}
    assert "hub.yaml" in changed_names
    assert "salesforce.yaml" in changed_names
    assert "notes.txt" not in changed_names
    assert len(on_change_calls[0]) == 2


@pytest.mark.anyio
async def test_hot_reload_loop_error_in_on_change_does_not_stop_loop():
    """hot_reload_loop continues watching even if on_change raises."""
    from watchfiles import Change

    mock_changes = [
        {(Change.modified, "/connectors/hub.yaml")},
        {(Change.modified, "/connectors/salesforce.yaml")},
    ]

    on_change_calls: list[set[Path]] = []
    call_count = {"n": 0}

    async def on_change(changed_paths: set[Path]) -> None:
        call_count["n"] += 1
        on_change_calls.append(changed_paths)
        if call_count["n"] == 1:
            raise RuntimeError("Simulated error in on_change")

    async def mock_awatch(path: Any):
        for change_set in mock_changes:
            yield change_set

    with patch("inandout.ingestion.watcher.awatch", mock_awatch):
        from inandout.ingestion.watcher import hot_reload_loop
        await hot_reload_loop(Path("/connectors"), on_change)

    # Both calls happened despite the error in the first
    assert len(on_change_calls) == 2
