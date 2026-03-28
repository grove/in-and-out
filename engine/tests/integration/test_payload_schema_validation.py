"""Integration tests for pre-write payload schema validation (T2 #23)."""
from __future__ import annotations

import os

import httpx
import orjson
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig,
)
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "payload_schema_test"
_DATATYPE = "orders"
_BASE_URL = "https://api.payload-schema.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_PAYLOAD_SCHEMA_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_PAYLOAD_SCHEMA_TEST_KEY", None)


def _make_connector(payload_schema: dict) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="PayloadSchemaSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="payload_schema_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    payload_schema=payload_schema,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/orders/${{external_id}}"),
                        insert=OperationConfig(method="POST", path="/v1/orders"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_payload_schema_blocks_missing_required_field(pool):
    """A payload missing a schema-required field is failed without HTTP dispatch."""
    delta_table = f"inout_delta_{_CONNECTOR}_miss_req"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        # Missing 'amount' which is schema-required
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["order-1", "Widget", "insert"],
        )
        await conn.commit()

    schema = {"required": ["amount"], "properties": {"amount": {"type": "number"}}}
    connector = _make_connector(schema)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/orders").mock(
            side_effect=lambda req: (posted.append(req), httpx.Response(201, json={"id": "order-1"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0
    assert len(posted) == 0, "POST must not be sent when payload fails schema validation"
    assert any("payload_validation" in e[2] for e in result._failed_entries)


@pytest.mark.anyio
async def test_payload_schema_blocks_wrong_type(pool):
    """A payload with a field of the wrong type is failed without HTTP dispatch."""
    delta_table = f"inout_delta_{_CONNECTOR}_wrong_type"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                quantity    TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        # quantity is TEXT "five" but schema requires integer
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, quantity, _action) VALUES (%s, %s, %s)",
            ["order-2", "five", "insert"],
        )
        await conn.commit()

    schema = {"properties": {"quantity": {"type": "integer"}}}
    connector = _make_connector(schema)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/orders").mock(
            side_effect=lambda req: (posted.append(req), httpx.Response(201, json={"id": "order-2"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0
    assert len(posted) == 0


@pytest.mark.anyio
async def test_payload_schema_passes_valid_payload(pool):
    """A payload satisfying all schema constraints proceeds with HTTP dispatch."""
    delta_table = f"inout_delta_{_CONNECTOR}_valid"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["order-3", "Widget Pro", "insert"],
        )
        await conn.commit()

    schema = {"required": ["name"], "properties": {"name": {"type": "string"}}}
    connector = _make_connector(schema)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/orders").mock(
            side_effect=lambda req: (posted.append(req), httpx.Response(201, json={"id": "order-3"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(posted) == 1


@pytest.mark.anyio
async def test_payload_schema_additional_properties_false_blocks_extra(pool):
    """additionalProperties:false blocks payloads with extra undeclared fields."""
    delta_table = f"inout_delta_{_CONNECTOR}_extra_props"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                secret_col  TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        # secret_col is an extra field not in the schema
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, secret_col, _action) VALUES (%s, %s, %s, %s)",
            ["order-4", "Widget", "should-be-blocked", "insert"],
        )
        await conn.commit()

    # Only 'name' is allowed; extra columns → rejected
    schema = {
        "properties": {"name": {"type": "string"}, "external_id": {"type": "string"}},
        "additionalProperties": False,
    }
    connector = _make_connector(schema)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/orders").mock(
            side_effect=lambda req: (posted.append(req), httpx.Response(201, json={"id": "order-4"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0
    assert len(posted) == 0
