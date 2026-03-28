"""Unit tests for HTTP transport adapter pagination."""
from __future__ import annotations

import os
from typing import Any

import httpx
import orjson
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, RateLimitConfig
from inandout.config.ingestion import ListConfig
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
from inandout.transport.http import HttpTransportAdapter


# ---------------------------------------------------------------------------
# Helpers to build test fixtures
# ---------------------------------------------------------------------------

def make_connector(base_url: str = "https://api.example.com") -> ConnectorConfig:
    """Build a minimal ConnectorConfig for testing (api_key auth)."""
    # Provide the credential via env var
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret-value"
    return ConnectorConfig(
        name="test",
        system="ExampleSystem",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test-key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "items": {
                "ingestion": {
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {
                        "method": "GET",
                        "path": "/items",
                        "record_selector": "results",
                        "pagination": {
                            "strategy": "cursor",
                            "cursor": {
                                "response_path": "paging.next.after",
                                "request_param": "after",
                            },
                        },
                    },
                }
            }
        },
    )


def make_cursor_list_config(
    path: str = "/items",
    record_selector: str = "results",
    cursor_response_path: str = "paging.next.after",
    cursor_request_param: str = "after",
) -> ListConfig:
    return ListConfig(
        method="GET",
        path=path,
        record_selector=record_selector,
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(
                response_path=cursor_response_path,
                request_param=cursor_request_param,
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Cursor pagination tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cursor_pagination_two_pages():
    """Two pages: first page has a cursor, second page has no cursor → stops."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    page1_data = {
        "results": [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
        "paging": {"next": {"after": "cursor-abc"}},
    }
    page2_data = {
        "results": [{"id": "3", "name": "Carol"}],
        "paging": {},
    }

    connector = make_connector()
    list_config = make_cursor_list_config()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        # First request (no cursor param)
        mock.get("/items").mock(
            side_effect=[
                httpx.Response(200, content=orjson.dumps(page1_data)),
                httpx.Response(200, content=orjson.dumps(page2_data)),
            ]
        )

        async with HttpTransportAdapter(connector) as adapter:
            pages: list[list[dict[str, Any]]] = []
            async for page in adapter.fetch_pages(list_config):
                pages.append(page)

    assert len(pages) == 2
    assert pages[0] == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    assert pages[1] == [{"id": "3", "name": "Carol"}]


@pytest.mark.anyio
async def test_cursor_pagination_single_page_no_cursor():
    """Single page with no cursor in response → stops after first page."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    page1_data = {
        "results": [{"id": "1", "name": "Alice"}],
        "paging": {},
    }

    connector = make_connector()
    list_config = make_cursor_list_config()

    with respx.mock(base_url="https://api.example.com") as mock:
        mock.get("/items").mock(
            return_value=httpx.Response(200, content=orjson.dumps(page1_data))
        )

        async with HttpTransportAdapter(connector) as adapter:
            pages: list[list[dict[str, Any]]] = []
            async for page in adapter.fetch_pages(list_config):
                pages.append(page)

    assert len(pages) == 1
    assert pages[0] == [{"id": "1", "name": "Alice"}]


@pytest.mark.anyio
async def test_cursor_pagination_empty_page_terminates():
    """Empty results list on first page → yields empty list then stops."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    page_data = {
        "results": [],
        "paging": {},
    }

    connector = make_connector()
    list_config = make_cursor_list_config()

    with respx.mock(base_url="https://api.example.com") as mock:
        mock.get("/items").mock(
            return_value=httpx.Response(200, content=orjson.dumps(page_data))
        )

        async with HttpTransportAdapter(connector) as adapter:
            pages: list[list[dict[str, Any]]] = []
            async for page in adapter.fetch_pages(list_config):
                pages.append(page)

    assert len(pages) == 1
    assert pages[0] == []


@pytest.mark.anyio
async def test_cursor_pagination_record_selector_none():
    """When record_selector is None and response is a list, use it directly."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    records = [{"id": "1"}, {"id": "2"}]

    connector = make_connector()
    list_config = ListConfig(
        method="GET",
        path="/items",
        record_selector=None,
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(
                response_path="next_cursor",
                request_param="cursor",
            ),
        ),
    )

    with respx.mock(base_url="https://api.example.com") as mock:
        mock.get("/items").mock(
            return_value=httpx.Response(200, content=orjson.dumps(records))
        )

        async with HttpTransportAdapter(connector) as adapter:
            pages: list[list[dict[str, Any]]] = []
            async for page in adapter.fetch_pages(list_config):
                pages.append(page)

    assert len(pages) == 1
    assert pages[0] == [{"id": "1"}, {"id": "2"}]


@pytest.mark.anyio
async def test_cursor_sends_cursor_param_on_second_request():
    """Verify the cursor value from page 1 is sent as query param on page 2."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    page1_data = {
        "results": [{"id": "1"}],
        "paging": {"next": {"after": "tok-XYZ"}},
    }
    page2_data = {
        "results": [{"id": "2"}],
        "paging": {},
    }

    connector = make_connector()
    list_config = make_cursor_list_config()

    captured_requests: list[httpx.Request] = []

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/items").mock(
            side_effect=[
                httpx.Response(200, content=orjson.dumps(page1_data)),
                httpx.Response(200, content=orjson.dumps(page2_data)),
            ]
        )

        async with HttpTransportAdapter(connector) as adapter:
            # Patch _request to capture requests
            original_request = adapter._request

            async def capturing_request(method, path, **kwargs):
                # Build a fake request to inspect params
                captured_requests.append(kwargs.get("params", {}))
                return await original_request(method, path, **kwargs)

            adapter._request = capturing_request  # type: ignore[method-assign]

            pages: list[list[dict[str, Any]]] = []
            async for page in adapter.fetch_pages(list_config):
                pages.append(page)

    assert len(pages) == 2
    # First request: no cursor param
    assert "after" not in captured_requests[0]
    # Second request: cursor param set to "tok-XYZ"
    assert captured_requests[1].get("after") == "tok-XYZ"


# ---------------------------------------------------------------------------
# Cursor page_size / page_size_param tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cursor_page_size_sent_on_every_request():
    """When page_size + page_size_param are configured, the limit is sent on every request."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    page1_data = {
        "results": [{"id": "1"}],
        "paging": {"next": {"after": "tok-XYZ"}},
    }
    page2_data = {
        "results": [{"id": "2"}],
        "paging": {},
    }

    connector = make_connector()
    list_config = ListConfig(
        method="GET",
        path="/items",
        record_selector="results",
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(
                response_path="paging.next.after",
                request_param="after",
                page_size=50,
                page_size_param="limit",
            ),
        ),
    )

    captured_params: list[dict] = []

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/items").mock(
            side_effect=[
                httpx.Response(200, content=orjson.dumps(page1_data)),
                httpx.Response(200, content=orjson.dumps(page2_data)),
            ]
        )

        async with HttpTransportAdapter(connector) as adapter:
            original_request = adapter._request

            async def capturing_request(method, path, **kwargs):
                captured_params.append(kwargs.get("params", {}))
                return await original_request(method, path, **kwargs)

            adapter._request = capturing_request  # type: ignore[method-assign]

            pages: list[list] = []
            async for page in adapter.fetch_pages(list_config):
                pages.append(page)

    assert len(pages) == 2
    # limit=50 must appear on BOTH requests
    assert captured_params[0].get("limit") == "50"
    assert captured_params[1].get("limit") == "50"
    # cursor param only on second request
    assert "after" not in captured_params[0]
    assert captured_params[1].get("after") == "tok-XYZ"


@pytest.mark.anyio
async def test_cursor_no_page_size_means_no_limit_param():
    """Without page_size configured, no limit param is injected into requests."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    page_data = {"results": [{"id": "1"}], "paging": {}}

    connector = make_connector()
    list_config = make_cursor_list_config()  # no page_size / page_size_param

    captured_params: list[dict] = []

    with respx.mock(base_url="https://api.example.com") as mock:
        mock.get("/items").mock(
            return_value=httpx.Response(200, content=orjson.dumps(page_data))
        )

        async with HttpTransportAdapter(connector) as adapter:
            original_request = adapter._request

            async def capturing_request(method, path, **kwargs):
                captured_params.append(kwargs.get("params", {}))
                return await original_request(method, path, **kwargs)

            adapter._request = capturing_request  # type: ignore[method-assign]

            async for _ in adapter.fetch_pages(list_config):
                pass

    assert len(captured_params) == 1
    assert "limit" not in captured_params[0]
