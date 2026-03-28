"""Connector dependency graph and topological sort."""
from __future__ import annotations

from typing import Any


def topological_sort(connector_configs: list[Any]) -> list[Any]:
    """Return connector configs sorted so dependencies come first.

    Parameters
    ----------
    connector_configs:
        List of ``ConnectorFileConfig`` objects.  Each object must expose
        ``connector.name`` (str) and ``connector.depends_on`` (list[str]).

    Returns
    -------
    list
        Sorted list of connector file configs.

    Raises
    ------
    ValueError
        If a circular dependency is detected.  The error message includes
        the offending cycle.
    """
    # Build maps
    name_to_cfg: dict[str, Any] = {
        cfg.connector.name: cfg for cfg in connector_configs
    }
    depends_on: dict[str, list[str]] = {
        cfg.connector.name: list(getattr(cfg.connector, "depends_on", []))
        for cfg in connector_configs
    }

    # Kahn's algorithm for topological sort
    # Build in-degree map and adjacency list (dependency → dependents)
    in_degree: dict[str, int] = {name: 0 for name in name_to_cfg}
    dependents: dict[str, list[str]] = {name: [] for name in name_to_cfg}

    for name, deps in depends_on.items():
        for dep in deps:
            if dep in in_degree:
                in_degree[name] += 1
                dependents[dep].append(name)
            # If dep is not in the loaded connectors, we ignore it
            # (the linter will catch LINT006 for unknown refs)

    # Start with nodes that have no in-bound dependencies, sorted by name
    queue: list[str] = sorted(
        name for name, deg in in_degree.items() if deg == 0
    )
    result: list[str] = []

    while queue:
        # Pick alphabetically-first node for stable ordering
        node = queue.pop(0)
        result.append(node)
        # Reduce in-degree of dependents
        for dep_name in sorted(dependents[node]):
            in_degree[dep_name] -= 1
            if in_degree[dep_name] == 0:
                queue.append(dep_name)
        queue.sort()

    if len(result) != len(name_to_cfg):
        # Cycle detected — identify which nodes are still stuck
        cycle_nodes = sorted(
            name for name, deg in in_degree.items() if deg > 0
        )
        raise ValueError(
            f"Circular connector dependency detected among: {', '.join(cycle_nodes)}"
        )

    return [name_to_cfg[name] for name in result]
