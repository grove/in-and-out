"""Plugin hot-reload via version polling."""
from __future__ import annotations

from typing import Awaitable, Callable

import anyio
import structlog

logger = structlog.get_logger(__name__)

_ENTRY_POINT_GROUP = "inandout.hooks"


async def get_plugin_versions() -> dict[str, str]:
    """Return a dict of {package_name: version} for all packages with inandout.hooks entry points.

    Uses ``importlib.metadata.packages_distributions()`` and
    ``importlib.metadata.version()`` to build the mapping.
    """
    from importlib.metadata import entry_points, packages_distributions, version as pkg_version

    # Find packages that have inandout.hooks entry points
    result: dict[str, str] = {}

    try:
        eps = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:
        logger.warning("get_plugin_versions_entry_points_error", error=str(exc))
        return result

    # Build a mapping of module → packages from packages_distributions
    try:
        pkgs_dist = packages_distributions()
    except Exception:
        pkgs_dist = {}

    for ep in eps:
        # ep.value is like "some.module:function" — extract the top-level module
        module_name = ep.value.split(":")[0].split(".")[0]

        # Find the distribution package for this module
        dist_packages = pkgs_dist.get(module_name, [])
        if not dist_packages:
            # Try the entry point's distribution name directly
            try:
                dist_name = ep.dist.name if ep.dist else ep.name
            except Exception:
                dist_name = ep.name

            try:
                result[dist_name] = pkg_version(dist_name)
            except Exception:
                result[dist_name] = "unknown"
        else:
            for dist_name in dist_packages:
                try:
                    result[dist_name] = pkg_version(dist_name)
                except Exception:
                    result[dist_name] = "unknown"

    return result


async def watch_plugin_versions(
    on_change: Callable[[str, str, str], Awaitable[None]],
    poll_interval_secs: float = 60.0,
) -> None:
    """Poll installed plugin package versions and call *on_change* when versions change.

    Parameters
    ----------
    on_change:
        Async callable called with (package_name, old_version, new_version).
        When a package is newly detected, old_version is "".
    poll_interval_secs:
        How often to poll for version changes.
    """
    known_versions: dict[str, str] = await get_plugin_versions()

    logger.info(
        "plugin_version_watcher_started",
        packages=list(known_versions.keys()),
        poll_interval_secs=poll_interval_secs,
    )

    while True:
        await anyio.sleep(poll_interval_secs)

        try:
            current_versions = await get_plugin_versions()
        except Exception as exc:
            logger.warning("plugin_version_poll_error", error=str(exc))
            continue

        # Detect changes and new packages
        for pkg_name, new_version in current_versions.items():
            old_version = known_versions.get(pkg_name, "")
            if old_version != new_version:
                logger.info(
                    "plugin_version_changed",
                    package=pkg_name,
                    old_version=old_version,
                    new_version=new_version,
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
