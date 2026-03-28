"""Unit tests for enhanced health/ready endpoints."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.anyio
async def test_health_endpoint_returns_ok():
    """Health endpoint should return status ok."""
    from inandout.ingestion.daemon import _health
    from starlette.requests import Request
    
    request = MagicMock(spec=Request)
    response = await _health(request)
    
    assert response.status_code == 200
    import json
    body = json.loads(response.body)
    assert body["status"] == "ok"


@pytest.mark.anyio
async def test_ready_endpoint_includes_connectors():
    """Ready endpoint should include connector status information."""
    from inandout.ingestion.daemon import _make_ready_handler
    from starlette.requests import Request
    
    # Mock pool that returns empty results
    pool = MagicMock()
    conn_context = AsyncMock()
    conn_mock = AsyncMock()
    
    # Mock health check query
    health_cursor = AsyncMock()
    health_cursor.fetchall = AsyncMock(return_value=[])
    
    # Mock pause check query
    pause_cursor = AsyncMock()
    pause_cursor.fetchall = AsyncMock(return_value=[])
    
    conn_mock.execute = AsyncMock(side_effect=[health_cursor, pause_cursor])
    conn_context.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_context.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_context)
    
    # Mock connector configs
    connector_cfg = MagicMock()
    connector_cfg.name = "test_connector"
    connector_configs = [MagicMock(connector=connector_cfg)]
    
    ready_handler = _make_ready_handler(pool, connector_configs)
    request = MagicMock(spec=Request)
    
    response = await ready_handler(request)
    
    assert response.status_code == 200
    import json
    body = json.loads(response.body)
    assert body["status"] == "ready"
    assert "connectors" in body
    assert isinstance(body["connectors"], dict)


@pytest.mark.anyio
async def test_ready_endpoint_shows_paused_connectors():
    """Ready endpoint should mark paused connectors correctly."""
    from inandout.ingestion.daemon import _make_ready_handler
    from starlette.requests import Request
    
    # Mock pool with paused connector
    pool = MagicMock()
    conn_context = AsyncMock()
    conn_mock = AsyncMock()
    
    # Mock health check query (empty)
    health_cursor = AsyncMock()
    health_cursor.fetchall = AsyncMock(return_value=[])
    
    # Mock pause check query (one paused)
    pause_cursor = AsyncMock()
    pause_cursor.fetchall = AsyncMock(return_value=[
        ("test_connector", "test_datatype")
    ])
    
    conn_mock.execute = AsyncMock(side_effect=[health_cursor, pause_cursor])
    conn_context.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_context.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_context)
    
    # Mock connector configs
    connector_cfg = MagicMock()
    connector_cfg.name = "test_connector"
    connector_configs = [MagicMock(connector=connector_cfg)]
    
    ready_handler = _make_ready_handler(pool, connector_configs)
    request = MagicMock(spec=Request)
    
    response = await ready_handler(request)
    
    import json
    body = json.loads(response.body)
    assert body["connectors"]["test_connector"]["status"] == "paused"


@pytest.mark.anyio
async def test_ready_endpoint_shows_circuit_broken_connectors():
    """Ready endpoint should mark unavailable (circuit-broken) connectors."""
    from inandout.ingestion.daemon import _make_ready_handler
    from starlette.requests import Request
    
    # Mock pool with unavailable connector
    pool = MagicMock()
    conn_context = AsyncMock()
    conn_mock = AsyncMock()
    
    # Mock health check query (one unavailable)
    health_cursor = AsyncMock()
    health_cursor.fetchall = AsyncMock(return_value=[
        ("test_connector", "test_datatype", "unavailable", "API returned 503")
    ])
    
    # Mock pause check query (empty)
    pause_cursor = AsyncMock()
    pause_cursor.fetchall = AsyncMock(return_value=[])
    
    conn_mock.execute = AsyncMock(side_effect=[health_cursor, pause_cursor])
    conn_context.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_context.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_context)
    
    # Mock connector configs
    connector_cfg = MagicMock()
    connector_cfg.name = "test_connector"
    connector_configs = [MagicMock(connector=connector_cfg)]
    
    ready_handler = _make_ready_handler(pool, connector_configs)
    request = MagicMock(spec=Request)
    
    response = await ready_handler(request)
    
    import json
    body = json.loads(response.body)
    assert body["connectors"]["test_connector"]["status"] == "circuit_broken"
    assert body["connectors"]["test_connector"]["datatypes"]["test_datatype"]["status"] == "unavailable"
