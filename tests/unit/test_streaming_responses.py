"""Unit tests for streaming response support."""
from __future__ import annotations

import pytest


def test_streaming_config_defaults_to_false():
    """streaming flag should default to False."""
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/stream",
        record_selector="items",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
    )
    assert cfg.streaming is False


def test_streaming_can_be_enabled():
    """streaming flag should be configurable."""
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/stream",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
        streaming=True,
        streaming_format="ndjson",
    )
    assert cfg.streaming is True
    assert cfg.streaming_format == "ndjson"


def test_streaming_format_options():
    """streaming_format should support ndjson, sse, json_array."""
    from inandout.config.ingestion import ListConfig
    
    for fmt in ["ndjson", "sse", "json_array"]:
        cfg = ListConfig(
            method="GET",
            path="/stream",
            pagination={"strategy": "offset", "offset": {"page_size": 100}},
            streaming=True,
            streaming_format=fmt,
        )
        assert cfg.streaming_format == fmt


def test_streaming_method_exists():
    """HttpTransportAdapter should have _fetch_streaming_pages method."""
    from inandout.transport.http import HttpTransportAdapter
    from inandout.config.connector import ConnectorConfig, ConnectionConfig
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    import os
    
    os.environ["INOUT_CREDENTIAL_TEST"] = "secret"
    connector = ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url="http://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test",
            api_key=ApiKeyConfig(location="header", name="X-API-Key"),
        ),
        datatypes={
            "test_dt": {
                "ingestion": {
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {
                        "method": "GET",
                        "path": "/test",
                        "record_selector": "items",
                        "pagination": {"strategy": "offset", "offset": {"page_size": 10}},
                    },
                },
            },
        },
    )
    
    adapter = HttpTransportAdapter(connector)
    assert hasattr(adapter, '_fetch_streaming_pages')
    assert callable(adapter._fetch_streaming_pages)


@pytest.mark.anyio
async def test_streaming_ndjson_parsing():
    """Test NDJSON streaming format parsing."""
    from inandout.transport.http import HttpTransportAdapter
    from inandout.config.connector import ConnectorConfig, ConnectionConfig
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.ingestion import ListConfig
    import respx
    import httpx
    import os
    
    os.environ["INOUT_CREDENTIAL_TEST"] = "secret"
    connector = ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url="http://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test",
            api_key=ApiKeyConfig(location="header", name="X-API-Key"),
        ),
        datatypes={
            "test_dt": {
                "ingestion": {
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {
                        "method": "GET",
                        "path": "/test",
                        "record_selector": "items",
                        "pagination": {"strategy": "offset", "offset": {"page_size": 10}},
                    },
                },
            },
        },
    )
    
    list_cfg = ListConfig(
        method="GET",
        path="/stream",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
        streaming=True,
        streaming_format="ndjson",
    )
    
    # Mock NDJSON response
    ndjson_data = '{"id": "1", "name": "Alice"}\n{"id": "2", "name": "Bob"}\n'
    
    with respx.mock:
        respx.get("http://api.example.com/stream").mock(
            return_value=httpx.Response(
                200,
                content=ndjson_data.encode(),
                headers={"content-type": "application/x-ndjson"},
            )
        )
        
        async with HttpTransportAdapter(connector) as adapter:
            records = []
            async for page in adapter.fetch_pages(list_cfg):
                records.extend(page)
            
            assert len(records) == 2
            assert records[0]["id"] == "1"
            assert records[0]["name"] == "Alice"
            assert records[1]["id"] == "2"
            assert records[1]["name"] == "Bob"
