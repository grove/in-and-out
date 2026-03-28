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


class _StopWrapper:
    """Adapts a callable into a stop-event accepted by watchfiles.awatch.

    watchfiles expects an object with a ``is_set() -> bool`` method.
    """
    __slots__ = ("_fn",)

    def __init__(self, fn: Callable[[], bool]) -> None:
        self._fn = fn

    def is_set(self) -> bool:
        return self._fn()


async def watch_connectors_dir(
    connectors_dir: Path,
    should_stop: Callable[[], bool] | None = None,
) -> AsyncIterator[set[Path]]:
    """Watch a directory for YAML file changes.

    Yields sets of changed Path objects, filtered to only .yaml files.
    Uses watchfiles.awatch under the hood.  If *should_stop* is provided it
    is forwarded to awatch as a stop_event so the watcher exits promptly when
    the callable returns True (e.g. when the daemon is draining).
    """
    if awatch is None:
        logger.warning("watchfiles_not_available_falling_back_to_sighup")
        return

    awatch_kwargs: dict = {}
    if should_stop is not None:
        awatch_kwargs["stop_event"] = _StopWrapper(should_stop)

    async for changes in awatch(connectors_dir, **awatch_kwargs):
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
    should_stop: Callable[[], bool] | None = None,
) -> None:
    """Watch connectors_dir for YAML changes and call on_change for each event.

    Wraps watch_connectors_dir and calls on_change with the set of changed paths.
    If *should_stop* is provided, the loop exits as soon as it returns True.
    """
    async for changed_paths in watch_connectors_dir(connectors_dir, should_stop=should_stop):
        if should_stop is not None and should_stop():
            break
        try:
            await on_change(changed_paths)
        except Exception as exc:
            logger.error("hot_reload_on_change_error", error=str(exc))
