"""Writeback plugin hooks — transform and filter callbacks.

Hooks let third-party packages inject per-connector logic into the writeback
pipeline without modifying the core engine.  Two hook types are supported,
applied in order for each outbound write:

1. ``transform`` — mutate/reshape the payload before it is sent (return updated dict)
2. ``filter``    — return ``False`` to skip the write entirely (record is logged but
                   not dead-lettered; use this for intentional suppression)

The hook API mirrors the ingestion hooks in ``inandout.plugins.hooks``.
Third-party packages advertise hooks using the ``inandout.writeback_hooks``
entry-point group::

    [project.entry-points."inandout.writeback_hooks"]
    my_plugin = "my_package.writeback_hooks:get_writeback_hooks"

The factory function must return a ``dict[str, WritebackHooks]`` mapping
connector names to their hooks.

Example::

    # my_package/writeback_hooks.py
    from inandout.writeback.hooks import WritebackHooks

    async def _inject_source_system(payload: dict, action: str) -> dict:
        payload["source_system"] = "mdm"
        return payload

    async def _skip_empty(payload: dict, action: str) -> bool:
        return bool(payload)  # drop if empty

    def get_writeback_hooks() -> dict[str, WritebackHooks]:
        return {
            "hubspot": WritebackHooks(
                transform=_inject_source_system,
                filter=_skip_empty,
            ),
        }
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import structlog

logger = structlog.get_logger(__name__)

_ENTRY_POINT_GROUP = "inandout.writeback_hooks"


@dataclass
class WritebackHooks:
    """Optional async callbacks that run per-record during writeback.

    Hooks are applied in order: transform → filter.

    Both hooks receive the *post-declaration-transform* payload — i.e., after
    the connector YAML's ``field_mappings`` and ``external_reference_field``
    injection have already been applied.
    """

    transform: Callable[[dict[str, Any], str], Awaitable[dict[str, Any]]] | None = field(
        default=None
    )
    """Async ``(payload, action) → payload`` function.

    Receives the outgoing payload dict and the write action (``"insert"``,
    ``"update"``, or ``"delete"``).  Return the (possibly modified) dict.
    """

    filter: Callable[[dict[str, Any], str], Awaitable[bool]] | None = field(default=None)
    """Async ``(payload, action) → bool`` function.

    Return ``True`` to allow the write, ``False`` to skip it silently.
    Skipped writes are counted as ``skipped`` in the writeback result and do
    NOT go to the dead-letter queue.
    """


class WritebackHookRegistry:
    """Maps connector names to their writeback hooks."""

    def __init__(self) -> None:
        self._hooks: dict[str, WritebackHooks] = {}

    def register(self, connector_name: str, hooks: WritebackHooks) -> None:
        """Register *hooks* for *connector_name*, replacing any existing entry."""
        self._hooks[connector_name] = hooks

    def get(self, connector_name: str) -> WritebackHooks | None:
        """Return the hooks registered for *connector_name*, or ``None``."""
        return self._hooks.get(connector_name)

    def clear(self) -> None:
        """Remove all registered hooks (used in tests)."""
        self._hooks.clear()


# Module-level singleton
_registry = WritebackHookRegistry()


def register_writeback_hooks(connector_name: str, hooks: WritebackHooks) -> None:
    """Convenience wrapper around ``_registry.register``."""
    _registry.register(connector_name, hooks)


async def apply_writeback_hooks(
    payload: dict[str, Any],
    action: str,
    connector_name: str,
    hooks: WritebackHooks | None = None,
) -> dict[str, Any] | None:
    """Apply transform → filter hooks to *payload*.

    Returns the (possibly mutated) payload, or ``None`` if the filter dropped it.

    Parameters
    ----------
    payload:
        The outgoing HTTP payload dict (post-declarative-transforms).
    action:
        The write action: ``"insert"``, ``"update"``, or ``"delete"``.
    connector_name:
        Used to look up hooks when *hooks* is not provided directly.
    hooks:
        Explicit hooks object; if ``None``, the registry is consulted.
    """
    resolved = hooks if hooks is not None else _registry.get(connector_name)
    if resolved is None:
        return payload

    if resolved.transform is not None:
        payload = await resolved.transform(payload, action)

    if resolved.filter is not None:
        keep = await resolved.filter(payload, action)
        if not keep:
            return None

    return payload


def discover_and_register_writeback_hooks(
    registry: WritebackHookRegistry | None = None,
) -> int:
    """Discover and register writeback hooks from installed packages.

    Scans ``importlib.metadata.entry_points(group="inandout.writeback_hooks")``
    for all registered hook factories.  Each entry point must point to a
    callable that returns ``dict[str, WritebackHooks]``.

    Returns the number of hook registrations applied.
    """
    from importlib.metadata import entry_points

    if registry is None:
        registry = _registry

    registered_count = 0

    try:
        eps = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:
        logger.warning("writeback_hooks_discovery_failed", error=str(exc))
        return 0

    for ep in eps:
        try:
            factory = ep.load()
            hooks_map: dict[str, WritebackHooks] = factory()
            for connector_name, hooks in hooks_map.items():
                registry.register(connector_name, hooks)
                registered_count += 1
                logger.info(
                    "writeback_hook_registered",
                    entry_point=ep.name,
                    connector=connector_name,
                )
        except Exception as exc:
            logger.warning(
                "writeback_hooks_load_failed",
                entry_point=ep.name,
                error=str(exc),
            )

    return registered_count
