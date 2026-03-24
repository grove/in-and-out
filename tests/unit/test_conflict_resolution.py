"""Unit tests for conflict resolution strategies in WritebackEngine."""
from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig
from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine, WritebackResult


def make_connector(base_url: str = "https://api.example.com") -> ConnectorConfig:
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"
    return ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="writeback_patch",
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test-key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": {
                "writeback": {
                    "protection_level": 2,
                    "conflict_resolution": "last_writer_wins",
                    "supported_actions": ["insert", "update", "delete"],
                    "operations": {
                        "lookup": {"method": "GET", "path": "/contacts/${external_id}"},
                        "insert": {"method": "POST", "path": "/contacts"},
                        "update": {"method": "PATCH", "path": "/contacts/${external_id}"},
                        "delete": {"method": "DELETE", "path": "/contacts/${external_id}"},
                    },
                }
            }
        },
    )


def make_writeback_config(conflict_resolution: ConflictResolution) -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.optimistic,
        conflict_resolution=conflict_resolution,
        supported_actions=["insert", "update", "delete"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
            insert=OperationConfig(method="POST", path="/contacts"),
            update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
            delete=OperationConfig(method="DELETE", path="/contacts/${external_id}"),
        ),
    )


def make_pool_with_last_written(last_written: dict[str, Any]) -> MagicMock:
    """Create a mock pool that returns the given _last_written dict."""
    pool = MagicMock()
    conn_ctx = AsyncMock()
    conn = AsyncMock()
    conn_ctx.__aenter__ = AsyncMock(return_value=conn)
    conn_ctx.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_ctx)

    # Mock the execute().fetchone() chain
    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(last_written,))
    conn.execute = AsyncMock(return_value=cursor)
    return pool


# ---------------------------------------------------------------------------
# server_wins: skips when server changed a field
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_server_wins_skips_when_server_changed():
    """server_wins: if server changed a field since last write, discard local update."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ConflictResolution.server_wins)

    last_written = {"name": "Alice", "email": "alice@example.com"}
    pool = make_pool_with_last_written(last_written)

    # Remote has a different value for 'name' — server changed it
    respx.get("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(
            200,
            json={"id": "123", "name": "Server-Alice", "email": "alice@example.com"},
            headers={"ETag": '"abc"'},
        )
    )

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport, connector, wb_cfg,
            action="update",
            external_id="123",
            row={"external_id": "123", "name": "Local-Alice"},
            log=MagicMock(),
            result=result,
        )

    assert result.skipped == 1
    assert result.conflicts == 1
    assert result.processed == 0


# ---------------------------------------------------------------------------
# server_wins: proceeds when server unchanged
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_server_wins_proceeds_when_server_unchanged():
    """server_wins: if server has same values as last_written, proceed with update."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ConflictResolution.server_wins)

    last_written = {"name": "Alice", "email": "alice@example.com"}
    pool = make_pool_with_last_written(last_written)

    # Remote has same values as last_written — no server change
    respx.get("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(
            200,
            json={"id": "123", "name": "Alice", "email": "alice@example.com"},
            headers={"ETag": '"abc"'},
        )
    )
    patch_route = respx.patch("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(200, json={"id": "123"})
    )

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport, connector, wb_cfg,
            action="update",
            external_id="123",
            row={"external_id": "123", "name": "Bob"},
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    assert result.skipped == 0
    assert result.conflicts == 0
    assert patch_route.called


# ---------------------------------------------------------------------------
# merge_fields: keeps local change when server unchanged
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_merge_fields_keeps_local_when_server_unchanged():
    """merge_fields: use local value when server has same value as last_written."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ConflictResolution.merge_fields)

    last_written = {"name": "Alice", "email": "alice@example.com"}
    pool = make_pool_with_last_written(last_written)

    # Server has unchanged values (same as last_written)
    respx.get("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(
            200,
            json={"id": "123", "name": "Alice", "email": "alice@example.com"},
            headers={"ETag": '"abc"'},
        )
    )
    patch_route = respx.patch("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(200, json={"id": "123"})
    )

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport, connector, wb_cfg,
            action="update",
            external_id="123",
            row={"external_id": "123", "name": "Bob"},  # local change: name → Bob
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    # Verify that local value was used (Bob, not Alice)
    import orjson
    last_call = patch_route.calls.last.request
    body = orjson.loads(last_call.content)
    assert body["name"] == "Bob"


# ---------------------------------------------------------------------------
# merge_fields: keeps server value when server changed that field
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_merge_fields_keeps_server_value_when_server_changed():
    """merge_fields: when server changed a field, keep server value for that field."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ConflictResolution.merge_fields)

    last_written = {"name": "Alice", "email": "alice@example.com"}
    pool = make_pool_with_last_written(last_written)

    # Server changed 'name' to 'Server-Alice'
    respx.get("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(
            200,
            json={"id": "123", "name": "Server-Alice", "email": "alice@example.com"},
            headers={"ETag": '"abc"'},
        )
    )
    patch_route = respx.patch("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(200, json={"id": "123"})
    )

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport, connector, wb_cfg,
            action="update",
            external_id="123",
            row={"external_id": "123", "name": "Local-Alice", "email": "newemail@example.com"},
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    import orjson
    last_call = patch_route.calls.last.request
    body = orjson.loads(last_call.content)
    # Server changed 'name' → server wins on that field
    assert body["name"] == "Server-Alice"
    # Server didn't change 'email' → local wins
    assert body["email"] == "newemail@example.com"


