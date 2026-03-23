"""File watcher for connector config hot-reload using watchfiles."""
from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable

import structlog

logger = structlog.get_logger(__name__)

try:
    from watchfiles import awatch
except ImportError:
    awatch = None  # type: ignore[assignment]


async def watch_connectors_dir(connectors_dir: Path) -> AsyncIterator[set[Path]]:
    """Watch a directory for YAML file changes.

    Yields sets of changed Path objects, filtered to only .yaml files.
    Uses watchfiles.awatch under the hood.
    """
    if awatch is None:
        logger.warning("watchfiles_not_available_falling_back_to_sighup")
        return

    async for changes in awatch(connectors_dir):
        yaml_paths: set[Path] = set()
        for _change_type, path_str in changes:
            p = Path(path_str)
            if p.suffix == ".yaml":
                yaml_paths.add(p)
        if yaml_paths:
            yield yaml_paths


async def hot_reload_loop(
    connectors_dir: Path,
    on_change: Callable[[set[Path]], Awaitable[None]],
) -> None:
    """Watch connectors_dir for YAML changes and call on_change for each event.

    Wraps watch_connectors_dir and calls on_change with the set of changed paths.
    """
    async for changed_paths in watch_connectors_dir(connectors_dir):
        try:
            await on_change(changed_paths)
        except Exception as exc:
            logger.error("hot_reload_on_change_error", error=str(exc))
