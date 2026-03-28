"""Plugin hook auto-discovery via importlib.metadata entry points.

Third-party packages advertise hooks using the ``inandout.hooks`` entry-point
group in their ``pyproject.toml``::

    [project.entry-points."inandout.hooks"]
    my_plugin = "my_package.hooks:get_hooks"

The factory function (``get_hooks`` above) must return a
``dict[str, ConnectorHooks]`` mapping connector names to their hooks.
"""
from __future__ import annotations

from importlib.metadata import entry_points

import structlog

from inandout.plugins.hooks import ConnectorHooks, HookRegistry, _registry

logger = structlog.get_logger(__name__)

_ENTRY_POINT_GROUP = "inandout.hooks"


def discover_and_register_hooks(registry: HookRegistry | None = None) -> int:
    """Discover and register hooks from installed packages via entry points.

    Scans ``importlib.metadata.entry_points(group="inandout.hooks")`` for all
    registered hook factories.  Each entry point must point to a callable that
    returns ``dict[str, ConnectorHooks]``.

    Parameters
    ----------
    registry:
        The ``HookRegistry`` to populate.  Defaults to the module-level
        singleton ``_registry``.

    Returns
    -------
    int
        Number of hook registrations applied.
    """
    if registry is None:
        registry = _registry

    registered_count = 0

    try:
        eps = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:
        logger.warning("plugin_entry_points_load_error", error=str(exc))
        return 0

    for ep in eps:
        try:
            factory = ep.load()
        except Exception as exc:
            logger.warning(
                "plugin_hook_entry_point_load_failed",
                entry_point=ep.name,
                error=str(exc),
            )
            continue

        try:
            hooks_map: dict[str, ConnectorHooks] = factory()
        except Exception as exc:
            logger.warning(
                "plugin_hook_factory_error",
                entry_point=ep.name,
                error=str(exc),
            )
            continue

        if not isinstance(hooks_map, dict):
            logger.warning(
                "plugin_hook_factory_invalid_return",
                entry_point=ep.name,
                got=type(hooks_map).__name__,
            )
            continue

        for connector_name, hooks in hooks_map.items():
            registry.register(connector_name, hooks)
            registered_count += 1

    if registered_count:
        logger.info(
            "plugin_hooks_discovered",
            count=registered_count,
            group=_ENTRY_POINT_GROUP,
        )

    return registered_count
