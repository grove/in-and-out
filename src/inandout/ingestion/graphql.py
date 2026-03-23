"""GraphQL support for ingestion: request building and response extraction."""
from __future__ import annotations

from typing import Any


def extract_graphql_records(response_data: dict[str, Any], data_path: str) -> list[dict[str, Any]]:
    """Extract records from a GraphQL response using dot-notation path traversal.

    Example: data_path="data.contacts.nodes" will traverse
    response_data["data"]["contacts"]["nodes"] and return the list found there.
    """
    current: Any = response_data
    for part in data_path.split("."):
        if not isinstance(current, dict):
            return []
        current = current.get(part)
        if current is None:
            return []
    if isinstance(current, list):
        return current
    return [current] if current is not None else []


def build_graphql_request_body(
    query: str,
    variables: dict[str, Any],
    cursor: str | None = None,
    cursor_var: str = "after",
) -> dict[str, Any]:
    """Build the request body for a GraphQL POST request.

    If cursor is provided, it is injected into the variables dict under cursor_var.
    """
    merged_variables = dict(variables)
    if cursor is not None:
        merged_variables[cursor_var] = cursor
    return {"query": query, "variables": merged_variables}
