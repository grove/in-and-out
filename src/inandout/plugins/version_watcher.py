"""Plugin hot-reload via version polling.

Polls installed package versions for all packages that register
``inandout.hooks`` entry points.  When a version change is detected (e.g.
after ``pip install --upgrade my-plugin``), the caller can trigger a
re-discovery and re-registration of hooks without restarting the daemon.
"""
from __future__ import annotations

from typing import Awaitable, Callable

import anyio
import structlog

logger = structlog.get_logger(__name__)

_ENTRY_POINT_GROUP = "inandout.hooks"


def get_plugin_versions() -> dict[str, str]:
    """Return a dict of {package_name: version} for all packages with inandout.hooks entry points.

    Uses ``importlib.metadata.packages_distributions()`` and
    ``importlib.metadata.version()`` to build the mapping.  Packages whose
    version cannot be determined are recorded as ``"unknown"``.
    """
    from importlib.metadata import entry_points, packages_distributions, version as pkg_version

    result: dict[str, str] = {}
    try:
        eps = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:
        logger.warning("get_plugin_versions_entry_points_error", error=str(exc))
        return result

    pkgs_dist = packages_distributions()

    for ep in eps:
        # ep.value is "module.path:attribute" → extract module top-level package
        module_name = ep.value.split(":")[0].split(".")[0]
        dist_packages = pkgs_dist.get(module_name, [])
        dist_name = dist_packages[0] if dist_packages else module_name
        try:
            result[dist_name] = pkg_version(dist_name)
        except Exception:
            result[dist_name] = "unknown"

    return result


async def watch_plugin_versions(
    on_change: Callable[[str, str, str], Awaitable[None]],
    poll_interval_secs: float = 60.0,
    should_stop: Callable[[], bool] | None = None,
) -> None:
    """Poll installed plugin package versions and call *on_change* when versions change.

    Parameters
    ----------
    on_change:
        Async callable called with (package_name, old_version, new_version).
        When a version bump is detected the caller should re-run
        ``discover_and_register_hooks()`` to pick up the new hooks.
    poll_interval_secs:
        How often to check for version changes (default: 60 s).
    should_stop:
        Optional zero-argument callable; the loop exits when it returns True.
    """
    logger.info("plugin_version_watcher_started", interval=poll_interval_secs)
    known_versions = get_plugin_versions()

    while True:
        if should_stop is not None and should_stop():
            logger.info("plugin_version_watcher_stopping")
            break

        await anyio.sleep(poll_interval_secs)

        try:
            current_versions = get_plugin_versions()
        except Exception as exc:
            logger.warning("plugin_version_poll_error", error=str(exc))
            continue

        for pkg_name, new_version in current_versions.items():
            old_version = known_versions.get(pkg_name)
            if old_version is not None and old_version != new_version:
                logger.info(
                    "plugin_version_changed",
                    package=pkg_name,
                    old=old_version,
                    new=new_version,
                )
                try:
                    await on_change(pkg_name, old_version, new_version)
                except Exception as exc:
                    logger.warning(
                        "plugin_version_on_change_error",
                        package=pkg_name,
                        error=str(exc),
                    )

        known_versions = current_versions
