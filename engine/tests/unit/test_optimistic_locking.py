"""Unit tests for optimistic locking in WritebackEngine."""
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
                    "conflict_resolution": "dead_letter",
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


def make_writeback_config(protection_level: ProtectionLevel) -> WritebackConfig:
    return WritebackConfig(
        protection_level=protection_level,
        conflict_resolution=ConflictResolution.dead_letter,
        supported_actions=["insert", "update", "delete"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
            insert=OperationConfig(method="POST", path="/contacts"),
            update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
            delete=OperationConfig(method="DELETE", path="/contacts/${external_id}"),
        ),
    )


# ---------------------------------------------------------------------------
# Successful optimistic update: GET returns ETag, PATCH with If-Match succeeds
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_optimistic_update_success():
    """GET returns ETag → PATCH with If-Match header → success."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ProtectionLevel.optimistic)

    # Mock pool (we only test _dispatch_row, not DB)
    pool = MagicMock()

    respx.get("https://api.example.com/contacts/123").mock(
        return_value=httpx.Response(
            200,
            json={"id": "123", "name": "Alice"},
            headers={"ETag": '"abc123"'},
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
            transport,
            connector,
            wb_cfg,
            action="update",
            external_id="123",
            row={"external_id": "123", "name": "Bob"},
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    assert result.failed == 0
    assert result.conflicts == 0
    # Verify If-Match header was sent
    assert patch_route.called
    last_request = patch_route.calls.last.request
    assert last_request.headers.get("If-Match") == '"abc123"'


# ---------------------------------------------------------------------------
# Conflict (412): skipped counter increments, not failed
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_optimistic_update_412_conflict():
    """412 response → conflict counter increments, skipped += 1, failed unchanged."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ProtectionLevel.optimistic)

    pool = MagicMock()

    respx.get("https://api.example.com/contacts/456").mock(
        return_value=httpx.Response(
            200,
            json={"id": "456"},
            headers={"ETag": '"stale-etag"'},
        )
    )
    respx.patch("https://api.example.com/contacts/456").mock(
        return_value=httpx.Response(412)
    )

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport,
            connector,
            wb_cfg,
            action="update",
            external_id="456",
            row={"external_id": "456", "name": "Updated"},
            log=MagicMock(),
            result=result,
        )

    assert result.conflicts == 1
    assert result.skipped == 1
    assert result.failed == 0
    assert result.processed == 0


# ---------------------------------------------------------------------------
# ProtectionLevel.none: no lookup, no If-Match header
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_protection_level_none_no_lookup():
    """ProtectionLevel.none: PATCH sent directly without prior GET or If-Match header."""
    connector = make_connector()
    wb_cfg = make_writeback_config(ProtectionLevel.none)

    pool = MagicMock()

    # No GET route registered — it should NOT be called
    patch_route = respx.patch("https://api.example.com/contacts/789").mock(
        return_value=httpx.Response(200, json={"id": "789"})
    )

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport,
            connector,
            wb_cfg,
            action="update",
            external_id="789",
            row={"external_id": "789", "name": "Direct"},
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    assert result.failed == 0
    assert result.conflicts == 0

    last_request = patch_route.calls.last.request
    assert "If-Match" not in last_request.headers


# ---------------------------------------------------------------------------
# WritebackResult has conflicts field
# ---------------------------------------------------------------------------

def test_writeback_result_has_conflicts_field():
    result = WritebackResult(connector="c", datatype="d", delta_table="t")
    assert result.conflicts == 0
    result.conflicts += 1
    assert result.conflicts == 1
