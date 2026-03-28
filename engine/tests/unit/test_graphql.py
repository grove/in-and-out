"""Unit tests for GraphQL connector support."""
from __future__ import annotations

import os
from typing import Any

import httpx
import orjson
import pytest
import respx

from inandout.ingestion.graphql import build_graphql_request_body, extract_graphql_records


# ---------------------------------------------------------------------------
# extract_graphql_records: dot-notation path traversal
# ---------------------------------------------------------------------------

def test_extract_graphql_records_simple_path():
    """extract_graphql_records with simple 2-level path."""
    data = {
        "data": {
            "contacts": [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        }
    }
    records = extract_graphql_records(data, "data.contacts")
    assert len(records) == 2
    assert records[0]["name"] == "Alice"


def test_extract_graphql_records_nested_path():
    """extract_graphql_records with nested 3-level dot-notation path."""
    data = {
        "data": {
            "contacts": {
                "nodes": [{"id": "1"}, {"id": "2"}, {"id": "3"}]
            }
        }
    }
    records = extract_graphql_records(data, "data.contacts.nodes")
    assert len(records) == 3
    assert records[0]["id"] == "1"


def test_extract_graphql_records_returns_empty_when_path_not_found():
    """extract_graphql_records returns [] when path doesn't exist."""
    data = {"data": {"other": []}}
    records = extract_graphql_records(data, "data.contacts.nodes")
    assert records == []


def test_extract_graphql_records_returns_empty_when_not_list():
    """extract_graphql_records returns single-item list when path resolves to non-list."""
    data = {"data": {"contact": {"id": "1", "name": "Alice"}}}
    records = extract_graphql_records(data, "data.contact")
    # Returns [contact] since contact is a dict, not a list
    assert len(records) == 1
    assert records[0]["name"] == "Alice"


def test_extract_graphql_records_path_missing_intermediate():
    """extract_graphql_records returns [] when intermediate path segment is missing."""
    data = {"data": {}}
    records = extract_graphql_records(data, "data.contacts.nodes")
    assert records == []


def test_extract_graphql_records_empty_list():
    """extract_graphql_records returns [] when path resolves to empty list."""
    data = {"data": {"contacts": {"nodes": []}}}
    records = extract_graphql_records(data, "data.contacts.nodes")
    assert records == []


# ---------------------------------------------------------------------------
# build_graphql_request_body: constructs correct body
# ---------------------------------------------------------------------------

def test_build_graphql_request_body_basic():
    """build_graphql_request_body returns query and variables."""
    query = "query GetContacts { contacts { id name } }"
    variables = {"limit": 10}

    body = build_graphql_request_body(query, variables)

    assert body["query"] == query
    assert body["variables"] == {"limit": 10}


def test_build_graphql_request_body_no_cursor():
    """build_graphql_request_body without cursor doesn't inject after var."""
    query = "query { contacts { nodes { id } } }"
    variables = {}

    body = build_graphql_request_body(query, variables)

    assert "after" not in body["variables"]


def test_build_graphql_request_body_injects_cursor():
    """build_graphql_request_body injects cursor into variables."""
    query = "query GetContacts($after: String) { contacts(after: $after) { nodes { id } } }"
    variables = {"limit": 10}
    cursor = "cursor-abc-123"

    body = build_graphql_request_body(query, variables, cursor=cursor)

    assert body["variables"]["after"] == cursor
    assert body["variables"]["limit"] == 10  # original vars preserved


def test_build_graphql_request_body_custom_cursor_var():
    """build_graphql_request_body uses custom cursor_var name."""
    query = "query { contacts(endCursor: $cursor) { nodes { id } } }"
    variables = {}
    cursor = "abc"

    body = build_graphql_request_body(query, variables, cursor=cursor, cursor_var="cursor")

    assert body["variables"]["cursor"] == cursor
    assert "after" not in body["variables"]


def test_build_graphql_request_body_does_not_mutate_input():
    """build_graphql_request_body doesn't mutate the input variables dict."""
    variables = {"limit": 10}
    original_variables = dict(variables)

    build_graphql_request_body("query {}", variables, cursor="some-cursor")

    assert variables == original_variables


# ---------------------------------------------------------------------------
# GraphQL mode detected when graphql_query is set
# ---------------------------------------------------------------------------

def test_list_config_graphql_query_field_exists():
    """ListConfig should have graphql_query, graphql_variables, graphql_data_path fields."""
    from inandout.config.ingestion import ListConfig
    from inandout.config.pagination import PaginationConfig

    list_cfg = ListConfig(
        method="POST",
        path="/graphql",
        pagination=PaginationConfig(strategy="offset"),
        graphql_query="query { contacts { id } }",
        graphql_variables={"limit": 10},
        graphql_data_path="data.contacts",
    )

    assert list_cfg.graphql_query == "query { contacts { id } }"
    assert list_cfg.graphql_variables == {"limit": 10}
    assert list_cfg.graphql_data_path == "data.contacts"


def test_list_config_graphql_fields_default_none():
    """GraphQL fields in ListConfig default to None/empty."""
    from inandout.config.ingestion import ListConfig
    from inandout.config.pagination import PaginationConfig

    list_cfg = ListConfig(
        method="GET",
        path="/contacts",
        pagination=PaginationConfig(strategy="offset"),
    )

    assert list_cfg.graphql_query is None
    assert list_cfg.graphql_variables == {}
    assert list_cfg.graphql_data_path is None


# ---------------------------------------------------------------------------
# GraphQL pagination via 'after' variable
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_graphql_pagination_via_after_variable():
    """GraphQL transport sends POST with cursor in after variable for pagination."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig
    from inandout.config.ingestion import ListConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy
    from inandout.transport.http import HttpTransportAdapter

    connector = ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test-key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": {
                "ingestion": {
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {
                        "method": "POST",
                        "path": "/graphql",
                        "graphql_query": "query GetContacts($after: String) { contacts(after: $after) { nodes { id } pageInfo { endCursor } } }",
                        "graphql_data_path": "data.contacts.nodes",
                        "pagination": {
                            "strategy": "cursor",
                            "cursor": {
                                "request_param": "after",
                                "response_path": "data.contacts.pageInfo.endCursor",
                            },
                        },
                    },
                }
            }
        },
    )

    list_cfg = connector.datatypes["contacts"].ingestion.list  # type: ignore

    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        body = orjson.loads(request.content)
        if call_count["n"] == 1:
            # First page — no cursor
            assert "after" not in body["variables"] or body["variables"]["after"] is None
            return httpx.Response(200, json={
                "data": {
                    "contacts": {
                        "nodes": [{"id": "1"}, {"id": "2"}],
                        "pageInfo": {"endCursor": "cursor-page-2"},
                    }
                }
            })
        else:
            # Second page — with cursor
            assert body["variables"]["after"] == "cursor-page-2"
            return httpx.Response(200, json={
                "data": {
                    "contacts": {
                        "nodes": [],
                        "pageInfo": {"endCursor": None},
                    }
                }
            })

    respx.post("https://api.example.com/graphql").mock(side_effect=handler)

    pages: list[list[dict]] = []
    async with HttpTransportAdapter(connector) as transport:
        async for page in transport.fetch_pages(list_cfg):
            pages.append(page)

    assert call_count["n"] == 2
    assert len(pages) == 2
    assert len(pages[0]) == 2  # first page has 2 records
    assert len(pages[1]) == 0  # second page is empty


@pytest.mark.anyio
@respx.mock
async def test_graphql_mode_uses_post_method():
    """GraphQL mode always uses POST regardless of method in config."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig
    from inandout.config.pagination import PaginationConfig
    from inandout.transport.http import HttpTransportAdapter

    connector = ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test-key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": {
                "ingestion": {
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {
                        "method": "GET",  # will be ignored in GraphQL mode
                        "path": "/graphql",
                        "graphql_query": "query { contacts { id name } }",
                        "graphql_data_path": "data.contacts",
                        "pagination": {"strategy": "offset"},
                    },
                }
            }
        },
    )

    list_cfg = connector.datatypes["contacts"].ingestion.list  # type: ignore

    post_route = respx.post("https://api.example.com/graphql").mock(
        return_value=httpx.Response(200, json={
            "data": {"contacts": [{"id": "1", "name": "Alice"}]}
        })
    )

    pages: list[list[dict]] = []
    async with HttpTransportAdapter(connector) as transport:
        async for page in transport.fetch_pages(list_cfg):
            pages.append(page)

    assert post_route.called
    assert len(pages) == 1
    assert pages[0][0]["name"] == "Alice"
