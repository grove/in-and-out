"""Example hooks package — serves as documentation and template for plugin authors.

To create a plugin package that registers hooks with in-and-out:

1. Create a Python package with a function like ``get_hooks()`` below.
2. Add to your package's pyproject.toml::

    [project.entry-points."inandout.hooks"]
    your_plugin_name = "your_package.hooks:get_hooks"

3. Install your package in the same environment as in-and-out.
4. The hooks will be auto-discovered at daemon startup.
"""
from __future__ import annotations

from inandout.plugins.hooks import ConnectorHooks


async def _noop_transform(record: dict) -> dict:
    """No-op transform hook — returns the record unchanged.

    This is a template. Replace with your own transform logic.
    """
    return record


def get_hooks() -> dict[str, ConnectorHooks]:
    """Return a mapping of connector name → ConnectorHooks.

    This function is the entry point factory called by the plugin discovery system.

    Returns:
        Dict mapping connector names to their respective hook objects.
    """
    return {
        "example_connector": ConnectorHooks(
            transform=_noop_transform,
        )
    }
