"""Integration tests for GraphQL-based ingestion (T1 #8 adjacent).

The ingestion engine detects GraphQL mode when a datatype's list config
has a `graphql_query` field set. It then POSTs a GraphQL query to the endpoint
and extracts records using dot-notation `graphql_data_path`.

Covers:
- Basic GraphQL query fetches records via POST; extracted with graphql_data_path
- GraphQL cursor-based pagination fetches multiple pages
- GraphQL static variables are merged into the request body
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.graphql-test.example.com"
_DATATYPE = "users"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_GRAPHQL_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_GRAPHQL_TEST_KEY", None)


_DEFAULT_GRAPHQL_PAGINATION = PaginationConfig(
    strategy=PaginationStrategy.cursor,
    cursor=CursorConfig(
        request_param="after",
        response_path="data.users.pageInfo.endCursor",
    ),
)


def _make_graphql_connector(
    connector_name: str = "graphql_test",
    graphql_data_path: str = "data.users.nodes",
    pagination: PaginationConfig | None = None,
    graphql_variables: dict | None = None,
) -> ConnectorConfig:
    list_kwargs: dict = dict(
        method="POST",
        path="/graphql",
        graphql_query="query GetUsers($after: String) { users { nodes { id name email } pageInfo { endCursor hasNextPage } } }",
        graphql_data_path=graphql_data_path,
        pagination=pagination if pagination is not None else _DEFAULT_GRAPHQL_PAGINATION,
    )
    if graphql_variables is not None:
        list_kwargs["graphql_variables"] = graphql_variables

    return ConnectorConfig(
        name=connector_name,
        system="GraphQLSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="graphql_test_key",
            api_key=ApiKeyConfig(location="header", name="Authorization"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{"list": ListConfig(**list_kwargs)},
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_graphql_basic_query_ingests_records(pool, run_migrations):
    """GraphQL mode: POST request with query body; records extracted via graphql_data_path."""
    connector = _make_graphql_connector(
        connector_name="graphql_basic_test",
        graphql_data_path="data.users.nodes",
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    source_table = f"inout_src_{connector.name}_{_DATATYPE}"

    graphql_response = {
        "data": {
            "users": {
                "nodes": [
                    {"id": "gql-1", "name": "Alice", "email": "alice@example.com"},
                    {"id": "gql-2", "name": "Bob", "email": "bob@example.com"},
                    {"id": "gql-3", "name": "Carol", "email": "carol@example.com"},
                ],
                "pageInfo": {"endCursor": None, "hasNextPage": False},
            }
        }
    }

    received_request_body = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        import json
        received_request_body.update(json.loads(request.content))
        return httpx.Response(200, json=graphql_response)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/graphql").mock(side_effect=_handler)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 3, f"Expected 3 records; got {result}"

    # Verify the GraphQL query was in the POST body
    assert "query" in received_request_body, "POST body must contain 'query' field"
    assert "GetUsers" in received_request_body["query"] or "users" in received_request_body["query"]

    # Verify records in source table
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id FROM {source_table} ORDER BY external_id"
        )).fetchall()
    assert [r[0] for r in rows] == ["gql-1", "gql-2", "gql-3"]


@pytest.mark.anyio
async def test_graphql_cursor_pagination_fetches_multiple_pages(pool, run_migrations):
    """GraphQL cursor pagination: fetches page 1 with endCursor, then page 2 until no more pages."""
    pagination = PaginationConfig(
        strategy=PaginationStrategy.cursor,
        cursor=CursorConfig(
            request_param="after",          # injected into variables
            response_path="data.users.pageInfo.endCursor",
        ),
    )
    connector = _make_graphql_connector(
        connector_name="graphql_cursor_test",
        graphql_data_path="data.users.nodes",
        pagination=pagination,
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    source_table = f"inout_src_{connector.name}_{_DATATYPE}"

    page_responses = [
        {
            "data": {
                "users": {
                    "nodes": [
                        {"id": "page1-1", "name": "Alice"},
                        {"id": "page1-2", "name": "Bob"},
                    ],
                    "pageInfo": {"endCursor": "cursor-abc", "hasNextPage": True},
                }
            }
        },
        {
            "data": {
                "users": {
                    "nodes": [
                        {"id": "page2-1", "name": "Carol"},
                    ],
                    "pageInfo": {"endCursor": None, "hasNextPage": False},
                }
            }
        },
    ]
    call_index = [0]

    def _handler(request: httpx.Request) -> httpx.Response:
        i = min(call_index[0], len(page_responses) - 1)
        call_index[0] += 1
        return httpx.Response(200, json=page_responses[i])

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/graphql").mock(side_effect=_handler)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 3, f"Expected 3 records from 2 pages; got {result}"
    assert call_index[0] == 2, f"Expected 2 POST calls (2 pages); made {call_index[0]}"

    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id FROM {source_table} ORDER BY external_id"
        )).fetchall()
    assert {r[0] for r in rows} == {"page1-1", "page1-2", "page2-1"}


@pytest.mark.anyio
async def test_graphql_static_variables_merged_in_request(pool, run_migrations):
    """GraphQL static variables from config are included in the POST body variables."""
    connector = _make_graphql_connector(
        connector_name="graphql_vars_test",
        graphql_data_path="data.products.items",
        graphql_variables={"first": 50, "orgId": "org-123"},
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    graphql_response = {
        "data": {
            "products": {
                "items": [
                    {"id": "prod-1", "name": "Widget"},
                ]
            }
        }
    }

    received_variables = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        import json
        body = json.loads(request.content)
        received_variables.update(body.get("variables", {}))
        return httpx.Response(200, json=graphql_response)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/graphql").mock(side_effect=_handler)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert received_variables.get("first") == 50, (
        f"Static variable 'first' should be in request; got {received_variables}"
    )
    assert received_variables.get("orgId") == "org-123", (
        f"Static variable 'orgId' should be in request; got {received_variables}"
    )
