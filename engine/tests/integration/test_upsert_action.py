"""Integration tests for T2 #19: upsert write strategy.

Covers:
- Upsert via dedicated endpoint (ops.upsert configured)
- Upsert via PATCH that succeeds (record already exists)
- Upsert via PATCH→POST fallback when PATCH returns 404 (record doesn't exist yet)
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig,
    ProtectionLevel,
    ConflictResolution,
    OperationsConfig,
    OperationConfig,
    UpdateOperationConfig,
)
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "upsert_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.upsert-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_UPSERT_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_UPSERT_TEST_KEY", None)


def _make_connector_with_upsert_op() -> ConnectorConfig:
    """Connector with a dedicated upsert (PUT) endpoint."""
    return ConnectorConfig(
        name=_CONNECTOR,
        system="UpsertSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="upsert_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["upsert"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/contacts/${external_id}"),
                        upsert=OperationConfig(method="PUT", path="/v1/contacts/${external_id}"),
                    ),
                ),
            ),
        },
    )


def _make_connector_fallback() -> ConnectorConfig:
    """Connector without dedicated upsert — uses PATCH→POST 404-fallback."""
    return ConnectorConfig(
        name=_CONNECTOR,
        system="UpsertSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="upsert_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["upsert"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/contacts/${external_id}"),
                        insert=OperationConfig(method="POST", path="/v1/contacts"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/contacts/${external_id}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_upsert_routes_to_dedicated_endpoint(pool, run_migrations):
    """With ops.upsert configured, upsert action sends a single PUT request."""
    delta_table = f"inout_delta_{_CONNECTOR}_ups_dedicated"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'upsert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, status, _action) VALUES (%s, %s, %s, %s)",
            ["contact-ups-1", "Alice", "active", "upsert"],
        )
        await conn.commit()

    connector = _make_connector_with_upsert_op()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    put_calls: list[httpx.Request] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.put("/v1/contacts/contact-ups-1").mock(
            side_effect=lambda req: (put_calls.append(req), httpx.Response(200, json={}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(put_calls) == 1, "Exactly one PUT must be sent to the upsert endpoint"
    body = put_calls[0].read()
    import orjson
    parsed = orjson.loads(body)
    assert parsed.get("name") == "Alice"
    assert parsed.get("status") == "active"


@pytest.mark.anyio
async def test_upsert_patch_succeeds_on_existing_record(pool, run_migrations):
    """Without ops.upsert, upsert sends PATCH; on 200 no POST is made."""
    delta_table = f"inout_delta_{_CONNECTOR}_ups_patch_ok"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'upsert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["contact-ups-2", "Bob", "upsert"],
        )
        await conn.commit()

    connector = _make_connector_fallback()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_calls: list[httpx.Request] = []
    post_calls: list[httpx.Request] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-ups-2").mock(
            side_effect=lambda req: (patch_calls.append(req), httpx.Response(200, json={}))[1]
        )
        mock.post("/v1/contacts").mock(
            side_effect=lambda req: (post_calls.append(req), httpx.Response(201, json={"id": "new-1"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(patch_calls) == 1, "PATCH must be tried first"
    assert len(post_calls) == 0, "POST must NOT be called when PATCH succeeds"


@pytest.mark.anyio
async def test_upsert_patch_404_falls_back_to_post(pool, run_migrations):
    """Without ops.upsert, upsert sends PATCH; on 404 falls back to POST to create the record."""
    delta_table = f"inout_delta_{_CONNECTOR}_ups_fallback"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'upsert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["contact-ups-3", "Carol", "upsert"],
        )
        await conn.commit()

    connector = _make_connector_fallback()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_calls: list[httpx.Request] = []
    post_calls: list[httpx.Request] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-ups-3").mock(
            side_effect=lambda req: (patch_calls.append(req), httpx.Response(404, json={"error": "not found"}))[1]
        )
        mock.post("/v1/contacts").mock(
            side_effect=lambda req: (post_calls.append(req), httpx.Response(201, json={"id": "new-carol"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1, f"Expected processed=1 but got {result}"
    assert result.failed == 0
    assert len(patch_calls) == 1, "PATCH must be attempted first"
    assert len(post_calls) == 1, "POST must be called as fallback when PATCH returns 404"
