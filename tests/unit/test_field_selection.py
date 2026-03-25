"""Unit tests for field selection (properties request) feature."""
from __future__ import annotations

import os

import pytest
import respx
import httpx

from inandout.config.connector import ConnectorConfig, ConnectionConfig
from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.ingestion import ListConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.transport.http import HttpTransportAdapter


def make_connector(base_url: str = "https://api.example.com") -> ConnectorConfig:
    """Build a minimal ConnectorConfig for testing."""
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
                        "pagination": {"strategy": "offset", "offset": {"page_size": 10}},
                    },
                }
            }
        },
    )


def make_list_config(**overrides) -> ListConfig:
    """Build a ListConfig with specified overrides."""
    defaults = {
        "method": "GET",
        "path": "/items",
        "record_selector": "results",
        "pagination": {"strategy": "offset", "offset": {"page_size": 10}},
    }
    defaults.update(overrides)
    return ListConfig(**defaults)


@pytest.mark.anyio
async def test_properties_comma_format():
    """Test properties with comma format: ?properties=field1,field2,field3"""
    connector = make_connector()
    list_cfg = make_list_config(
        properties=["name", "email", "phone"],
        properties_param="fields",
        properties_format="comma",
    )
    
    captured_url: str = ""
    
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_url
        captured_url = str(request.url)
        return httpx.Response(200, json={"results": []})  # Empty to stop pagination
    
    with respx.mock:
        respx.get("https://api.example.com/items").mock(side_effect=handler)
        
        async with HttpTransportAdapter(connector) as adapter:
            async for _ in adapter.fetch_pages(list_cfg):
                break
    
    assert "fields=name%2Cemail%2Cphone" in captured_url or "fields=name,email,phone" in captured_url


@pytest.mark.anyio
async def test_properties_array_format():
    """Test properties with array format: ?properties=field1&properties=field2"""
    connector = make_connector()
    list_cfg = make_list_config(
        properties=["name", "email"],
        properties_param="fields",
        properties_format="array",
    )
    
    captured_params: list[tuple[str, str]] = []
    
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_params
        # Extract all query params as tuples
        for key in request.url.params.keys():
            for value in request.url.params.get_list(key):
                captured_params.append((key, value))
        return httpx.Response(200, json={"results": []})
    
    with respx.mock:
        respx.get("https://api.example.com/items").mock(side_effect=handler)
        
        async with HttpTransportAdapter(connector) as adapter:
            async for _ in adapter.fetch_pages(list_cfg):
                break
    
    # Should have multiple 'fields' params
    field_params = [(k, v) for k, v in captured_params if k == "fields"]
    assert ("fields", "name") in field_params
    assert ("fields", "email") in field_params


@pytest.mark.anyio
async def test_properties_json_array_format():
    """Test properties with json_array format: ?properties=["field1","field2"]"""
    connector = make_connector()
    list_cfg = make_list_config(
        properties=["name", "email", "phone"],
        properties_param="fields",
        properties_format="json_array",
    )
    
    captured_url: str = ""
    
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_url
        captured_url = str(request.url)
        return httpx.Response(200, json={"results": []})
    
    with respx.mock:
        respx.get("https://api.example.com/items").mock(side_effect=handler)
        
        async with HttpTransportAdapter(connector) as adapter:
            async for _ in adapter.fetch_pages(list_cfg):
                break
    
    # Should have JSON array in query param
    assert "fields=%5B%22name" in captured_url or 'fields=["name"' in captured_url


@pytest.mark.anyio
async def test_no_properties_sends_no_param():
    """Test that when properties is empty, no properties param is sent."""
    connector = make_connector()
    list_cfg = make_list_config(properties=[])  # Empty
    
    captured_url: str = ""
    
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_url
        captured_url = str(request.url)
        return httpx.Response(200, json={"results": []})
    
    with respx.mock:
        respx.get("https://api.example.com/items").mock(side_effect=handler)
        
        async with HttpTransportAdapter(connector) as adapter:
            async for _ in adapter.fetch_pages(list_cfg):
                break
    
    assert "properties" not in captured_url


@pytest.mark.anyio
async def test_properties_with_cursor_pagination():
    """Test that properties work with cursor-based pagination."""
    connector = make_connector()
    list_cfg = make_list_config(
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(response_path="next", request_param="cursor"),
        ),
        properties=["id", "name"],
        properties_format="comma",
    )
    
    call_count = 0
    
    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        url = str(request.url)
        # Verify properties param is present
        assert "properties=id%2Cname" in url or "properties=id,name" in url
        
        if call_count == 1:
            return httpx.Response(200, json={"results": [{"id": "1"}], "next": "abc123"})
        return httpx.Response(200, json={"results": [{"id": "2"}]})
    
    with respx.mock:
        respx.get("https://api.example.com/items").mock(side_effect=handler)
        
        async with HttpTransportAdapter(connector) as adapter:
            pages = []
            async for page in adapter.fetch_pages(list_cfg):
                pages.append(page)
                if len(pages) >= 2:
                    break
    
    assert call_count == 2
    assert len(pages) == 2
