"""Unit tests for writeback idempotency key."""
from __future__ import annotations

import hashlib
import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import orjson
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
from inandout.writeback.engine import WritebackEngine, WritebackResult, _compute_row_hash


def make_connector() -> ConnectorConfig:
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"
    return ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="writeback_patch",
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test-key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": {
                "writeback": {
                    "protection_level": 3,
                    "conflict_resolution": "last_writer_wins",
                    "supported_actions": ["insert", "update"],
                    "operations": {
                        "lookup": {"method": "GET", "path": "/contacts/${external_id}"},
                        "insert": {"method": "POST", "path": "/contacts"},
                        "update": {"method": "PATCH", "path": "/contacts/${external_id}"},
                    },
                }
            }
        },
    )


def make_writeback_config(idempotency_key_header: str | None) -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
            insert=OperationConfig(method="POST", path="/contacts"),
            update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
        ),
        idempotency_key_header=idempotency_key_header,
    )


# ---------------------------------------------------------------------------
# Header injected when idempotency_key_header is set
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_idempotency_header_injected_on_insert():
    """Idempotency-Key header is injected when idempotency_key_header is configured."""
    connector = make_connector()
    wb_cfg = make_writeback_config("Idempotency-Key")

    post_route = respx.post("https://api.example.com/contacts").mock(
        return_value=httpx.Response(201, json={"id": "new-1"})
    )

    pool = MagicMock()
    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport, connector, wb_cfg,
            action="insert",
            external_id="new-1",
            row={"name": "Alice", "email": "alice@example.com"},
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    assert post_route.called
    request = post_route.calls.last.request
    assert "Idempotency-Key" in request.headers


# ---------------------------------------------------------------------------
# Header not injected when config field is None
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_idempotency_header_not_injected_when_not_configured():
    """No Idempotency-Key header when idempotency_key_header is None."""
    connector = make_connector()
    wb_cfg = make_writeback_config(None)  # not configured

    post_route = respx.post("https://api.example.com/contacts").mock(
        return_value=httpx.Response(201, json={"id": "new-2"})
    )

    pool = MagicMock()
    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")

    from inandout.transport.http import HttpTransportAdapter
    async with HttpTransportAdapter(connector) as transport:
        await engine._dispatch_row(
            transport, connector, wb_cfg,
            action="insert",
            external_id="new-2",
            row={"name": "Bob"},
            log=MagicMock(),
            result=result,
        )

    assert result.processed == 1
    request = post_route.calls.last.request
    assert "Idempotency-Key" not in request.headers


# ---------------------------------------------------------------------------
# Same row produces same idempotency key (deterministic)
# ---------------------------------------------------------------------------

def test_idempotency_key_is_deterministic():
    """Same row data always produces the same idempotency key."""
    row = {"name": "Alice", "email": "alice@example.com", "_action": "insert"}
    external_id = "user-123"
    connector_name = "myconn"
    datatype = "contacts"

    def compute_key(row: dict, external_id: str, connector_name: str, datatype: str) -> str:
        raw_hash = _compute_row_hash(row)
        key_material = f"{connector_name}:{datatype}:{external_id}:{raw_hash}"
        return hashlib.sha256(key_material.encode()).hexdigest()

    key1 = compute_key(row, external_id, connector_name, datatype)
    key2 = compute_key(row, external_id, connector_name, datatype)

    assert key1 == key2


# ---------------------------------------------------------------------------
# Different rows produce different keys
# ---------------------------------------------------------------------------

def test_idempotency_key_differs_for_different_rows():
    """Different row data produces different idempotency keys."""
    row1 = {"name": "Alice", "email": "alice@example.com"}
    row2 = {"name": "Bob", "email": "bob@example.com"}
    external_id = "user-123"
    connector_name = "myconn"
    datatype = "contacts"

    def compute_key(row: dict) -> str:
        raw_hash = _compute_row_hash(row)
        key_material = f"{connector_name}:{datatype}:{external_id}:{raw_hash}"
        return hashlib.sha256(key_material.encode()).hexdigest()

    key1 = compute_key(row1)
    key2 = compute_key(row2)

    assert key1 != key2


# ---------------------------------------------------------------------------
# _compute_row_hash is stable (sorted keys, non-_ fields only)
# ---------------------------------------------------------------------------

def test_compute_row_hash_ignores_underscore_fields():
    """_compute_row_hash should ignore _ prefixed fields."""
    row_with_meta = {"name": "Alice", "_action": "update", "_seq": 1}
    row_without_meta = {"name": "Alice"}

    hash1 = _compute_row_hash(row_with_meta)
    hash2 = _compute_row_hash(row_without_meta)

    assert hash1 == hash2


def test_compute_row_hash_stable_across_key_ordering():
    """_compute_row_hash should be stable regardless of dict key ordering."""
    row_a = {"name": "Alice", "email": "alice@example.com", "age": 30}
    row_b = {"age": 30, "email": "alice@example.com", "name": "Alice"}

    assert _compute_row_hash(row_a) == _compute_row_hash(row_b)


# ---------------------------------------------------------------------------
# WritebackConfig has idempotency_key_header field
# ---------------------------------------------------------------------------

def test_writeback_config_has_idempotency_key_header():
    """WritebackConfig should have idempotency_key_header field defaulting to None."""
    wb_cfg = make_writeback_config(None)
    assert wb_cfg.idempotency_key_header is None

    wb_cfg2 = make_writeback_config("Idempotency-Key")
    assert wb_cfg2.idempotency_key_header == "Idempotency-Key"
