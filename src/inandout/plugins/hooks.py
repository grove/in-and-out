"""Connector plugin hooks — transform, filter, enrich callbacks.

Hooks let third-party packages inject per-connector logic into the ingestion
pipeline without modifying core source.  Three optional hook types are
supported, applied in order for each ingested record:

1. ``transform`` — mutate/reshape the record (return updated dict)
2. ``filter``    — return ``False`` to drop a record before upsert
3. ``enrich``    — augment the record from an external source (DB, cache, etc.)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from psycopg_pool import AsyncConnectionPool


@dataclass
class ConnectorHooks:
    """Optional async callbacks that run per-record during ingestion.

    Hooks are applied in order: transform → filter → enrich.
    """

    transform: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]] | None = field(default=None)
    """Async function that receives a record dict and returns a (possibly modified) dict."""

    filter: Callable[[dict[str, Any]], Awaitable[bool]] | None = field(default=None)
    """Async function that returns True to keep the record or False to drop it."""

    enrich: Callable[[dict[str, Any], AsyncConnectionPool], Awaitable[dict[str, Any]]] | None = field(default=None)
    """Async function that receives (record, pool) and returns an enriched record."""


class HookRegistry:
    """Module-level singleton that maps connector names to their hooks."""

    def __init__(self) -> None:
        self._hooks: dict[str, ConnectorHooks] = {}

    def register(self, connector_name: str, hooks: ConnectorHooks) -> None:
        """Register *hooks* for the named connector, replacing any existing entry."""
        self._hooks[connector_name] = hooks

    def get(self, connector_name: str) -> ConnectorHooks | None:
        """Return the hooks registered for *connector_name*, or ``None``."""
        return self._hooks.get(connector_name)

    def clear(self) -> None:
        """Remove all registered hooks (used in tests)."""
        self._hooks.clear()


# Module-level singleton — imported by discovery and apply_hooks
_registry = HookRegistry()


def register_hooks(connector_name: str, hooks: ConnectorHooks) -> None:
    """Convenience wrapper around ``_registry.register``."""
    _registry.register(connector_name, hooks)


async def apply_hooks(
    record: dict[str, Any],
    connector_name: str,
    pool: AsyncConnectionPool,
    hooks: ConnectorHooks | None = None,
) -> dict[str, Any] | None:
    """Apply transform → filter → enrich hooks to *record*.

    Returns the (possibly mutated) record, or ``None`` if the filter hook
    dropped it.

    Parameters
    ----------
    record:
        The raw record dict from the ingestion pipeline.
    connector_name:
        Used to look up hooks when *hooks* is not provided directly.
    pool:
        Database connection pool passed to the enrich hook.
    hooks:
        Explicit hooks object; if ``None``, the registry is consulted.
    """
    resolved = hooks if hooks is not None else _registry.get(connector_name)
    if resolved is None:
        return record

    # 1. Transform
    if resolved.transform is not None:
        record = await resolved.transform(record)

    # 2. Filter — drop the record when the hook returns False
    if resolved.filter is not None:
        keep = await resolved.filter(record)
        if not keep:
            return None

    # 3. Enrich
    if resolved.enrich is not None:
        record = await resolved.enrich(record, pool)

    return record
