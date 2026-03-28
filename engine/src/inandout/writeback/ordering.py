"""Write batch dependency ordering and cycle detection.

Provides topological sort for rows within a write batch when write_dependencies
are configured in WritebackConfig. Rows in the same _group_id are sorted so
parent records come before child records.
"""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def detect_dependency_cycle(
    rows: list[dict[str, Any]],
    dependencies: list[Any],  # list[WriteDependency]
) -> bool:
    """Return True if there is a dependency cycle among the given rows.

    A cycle exists when a circular chain of join_field references is detected
    within the row set (e.g., row A references row B which references row A).
    """
    if not dependencies or not rows:
        return False

    # Build a graph: external_id → set of external_ids it depends on
    # For each row, check if its join_field value points to another row's external_id
    row_ids = {str(row.get("external_id", "")) for row in rows}

    # Adjacency list: node → set of nodes it depends on (must come after)
    edges: dict[str, set[str]] = {str(row.get("external_id", "")): set() for row in rows}

    for row in rows:
        ext_id = str(row.get("external_id", ""))
        for dep in dependencies:
            join_val = str(row.get(dep.join_field, ""))
            if join_val and join_val in row_ids and join_val != ext_id:
                edges[ext_id].add(join_val)

    # Detect cycle using DFS
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def _dfs(node: str) -> bool:
        visited.add(node)
        rec_stack.add(node)
        for neighbour in edges.get(node, set()):
            if neighbour not in visited:
                if _dfs(neighbour):
                    return True
            elif neighbour in rec_stack:
                return True
        rec_stack.discard(node)
        return False

    for node in list(edges.keys()):
        if node not in visited:
            if _dfs(node):
                return True

    return False


def topological_sort_rows(
    rows: list[dict[str, Any]],
    dependencies: list[Any],  # list[WriteDependency]
) -> list[dict[str, Any]]:
    """Sort rows so parent records come before child records.

    Groups rows by _group_id if present. Within each group, applies
    topological sort based on dependencies. If a cycle is detected within a
    group, all rows in that group are moved to dead-letter (marked with
    error_class='dependency_cycle').

    Returns:
        list of rows in the correct write order. Rows with cycles are returned
        with a special _cycle_error flag set to True so callers can dead-letter them.
    """
    if not dependencies or not rows:
        return rows

    # Group rows by _group_id (OSI-Mapping sets this for same-transaction records)
    groups: dict[str, list[dict[str, Any]]] = {}
    ungrouped: list[dict[str, Any]] = []

    for row in rows:
        group_id = row.get("_group_id")
        if group_id is not None:
            group_key = str(group_id)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
        else:
            ungrouped.append(row)

    result: list[dict[str, Any]] = []

    # Process each group
    for group_id, group_rows in groups.items():
        if detect_dependency_cycle(group_rows, dependencies):
            logger.warning(
                "writeback_dependency_cycle_detected",
                group_id=group_id,
                row_count=len(group_rows),
            )
            # Mark all rows in the group as cycle errors
            for row in group_rows:
                marked = dict(row)
                marked["_cycle_error"] = True
                result.append(marked)
            continue

        # Build dependency graph for topological sort
        row_by_id: dict[str, dict[str, Any]] = {
            str(row.get("external_id", i)): row
            for i, row in enumerate(group_rows)
        }
        row_ids = set(row_by_id.keys())

        # adjacency: id → set of ids that must come before it (parents)
        parents: dict[str, set[str]] = {k: set() for k in row_ids}
        for row in group_rows:
            ext_id = str(row.get("external_id", ""))
            for dep in dependencies:
                join_val = str(row.get(dep.join_field, ""))
                if join_val and join_val in row_ids and join_val != ext_id:
                    # This row depends on join_val (join_val is parent)
                    parents[ext_id].add(join_val)

        # Kahn's algorithm
        in_degree = {k: len(v) for k, v in parents.items()}
        queue = [k for k, deg in in_degree.items() if deg == 0]
        sorted_ids: list[str] = []

        while queue:
            node = queue.pop(0)
            sorted_ids.append(node)
            # Find children: nodes that have node as a parent
            for child_id, parent_set in parents.items():
                if node in parent_set:
                    in_degree[child_id] -= 1
                    if in_degree[child_id] == 0:
                        queue.append(child_id)

        # If not all nodes sorted, there's a cycle (shouldn't happen — already checked)
        if len(sorted_ids) != len(row_ids):
            for row in group_rows:
                marked = dict(row)
                marked["_cycle_error"] = True
                result.append(marked)
        else:
            for k in sorted_ids:
                result.append(row_by_id[k])

    # Append ungrouped rows at the end (preserve original order)
    result.extend(ungrouped)
    return result
