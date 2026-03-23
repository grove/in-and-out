"""Plugin hook auto-discovery via importlib.metadata entry points."""
from __future__ import annotations

from importlib.metadata import entry_points

import structlog

from inandout.plugins.hooks import HookRegistry, _registry

logger = structlog.get_logger(__name__)

# Entry point group name for in-and-out hooks
_ENTRY_POINT_GROUP = "inandout.hooks"


def discover_and_register_hooks(registry: HookRegistry | None = None) -> int:
    """Discover and register hooks from installed packages via entry points.

    Scans ``importlib.metadata.entry_points(group="inandout.hooks")`` for all
    registered hook factories. Each entry point must point to a callable
    ``() -> dict[str, ConnectorHooks]`` that returns a mapping of connector
    names to hook objects.

    Args:
        registry: HookRegistry to register into. Defaults to the module-level
                  singleton registry.

    Returns:
        Number of connector hook sets registered.
    """
    if registry is None:
        registry = _registry

    registered_count = 0

    try:
        eps = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:
        logger.warning("plugin_entry_points_load_error", group=_ENTRY_POINT_GROUP, error=str(exc))
        return 0

    for ep in eps:
        try:
            factory = ep.load()
        except Exception as exc:
            logger.warning(
                "plugin_hook_entry_point_load_failed",
                entry_point=ep.name,
                value=ep.value,
                error=str(exc),
            )
            continue

        try:
            hooks_map = factory()
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
                type=type(hooks_map).__name__,
            )
            continue

        for connector_name, hooks in hooks_map.items():
            registry.register(connector_name, hooks)
            registered_count += 1

    logger.info(
        "plugin_hooks_discovered",
        count=registered_count,
        group=_ENTRY_POINT_GROUP,
    )
    return registered_count
