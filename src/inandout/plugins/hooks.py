"""Connector plugin hooks — transform, filter, enrich callbacks."""
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
    """Mutate / normalise a record before upsert."""

    filter: Callable[[dict[str, Any]], Awaitable[bool]] | None = field(default=None)
    """Return False to drop the record (it will not be upserted)."""

    enrich: Callable[[dict[str, Any], AsyncConnectionPool], Awaitable[dict[str, Any]]] | None = field(
        default=None
    )
    """Add fields via DB look-up. Receives the record and the connection pool."""


class HookRegistry:
    """Module-level singleton that maps connector names to their hooks."""

    def __init__(self) -> None:
        self._hooks: dict[str, ConnectorHooks] = {}

    def register(self, connector_name: str, hooks: ConnectorHooks) -> None:
        """Register hooks for a connector."""
        self._hooks[connector_name] = hooks

    def get(self, connector_name: str) -> ConnectorHooks | None:
        """Return the hooks registered for *connector_name*, or None."""
        return self._hooks.get(connector_name)

    def clear(self) -> None:
        """Remove all registered hooks (useful in tests)."""
        self._hooks.clear()


# Module-level singleton
_registry = HookRegistry()


def register_hooks(connector_name: str, hooks: ConnectorHooks) -> None:
    """Convenience wrapper around ``_registry.register``."""
    _registry.register(connector_name, hooks)


async def apply_hooks(
    record: dict[str, Any],
    connector_name: str,
    pool: AsyncConnectionPool | None = None,
) -> dict[str, Any] | None:
    """Apply transform → filter → enrich hooks to *record*.

    Returns the (possibly mutated) record, or ``None`` if the filter hook
    dropped it.
    """
    hooks = _registry.get(connector_name)
    if hooks is None:
        return record

    # 1. transform
    if hooks.transform is not None:
        record = await hooks.transform(record)

    # 2. filter
    if hooks.filter is not None:
        if not await hooks.filter(record):
            return None

    # 3. enrich
    if hooks.enrich is not None and pool is not None:
        record = await hooks.enrich(record, pool)

    return record
