"""Merge hook registry for custom_merge conflict resolution strategy."""
from __future__ import annotations

from typing import Any, Awaitable, Callable

MergeHookFn = Callable[[dict[str, Any], dict[str, Any], dict[str, Any]], Awaitable[dict[str, Any]]]


class MergeHookRegistry:
    """Registry for custom merge hooks keyed by (connector_name, datatype)."""

    def __init__(self) -> None:
        self._hooks: dict[str, MergeHookFn] = {}

    def _key(self, connector: str, datatype: str) -> str:
        return f"writeback_merge_{connector}_{datatype}"

    def register(self, connector: str, datatype: str, fn: MergeHookFn) -> None:
        """Register a merge hook for the given connector/datatype pair."""
        self._hooks[self._key(connector, datatype)] = fn

    def get(self, connector: str, datatype: str) -> MergeHookFn | None:
        """Retrieve the merge hook for the given connector/datatype pair, or None."""
        return self._hooks.get(self._key(connector, datatype))


# Module-level singleton
merge_hook_registry = MergeHookRegistry()
