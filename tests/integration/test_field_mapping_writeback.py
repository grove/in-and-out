"""Integration tests for pre-write field mappings (T2 #17)."""
from __future__ import annotations

import os

import httpx
import orjson
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.field_mapping import FieldMapping
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig,
)
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "field_map_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.fieldmap-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_FIELD_MAP_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_FIELD_MAP_TEST_KEY", None)


def _make_connector(field_mappings: list[FieldMapping], strict: bool = False) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="FieldMapSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="field_map_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    field_mappings=field_mappings,
                    field_mappings_strict=strict,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/contacts/${external_id}"),
                        insert=OperationConfig(method="POST", path="/v1/contacts"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_field_mapping_renames_field_before_insert(pool):
    """FieldMapping renames first_name → firstName in the outbound POST payload."""
    delta_table = f"inout_delta_{_CONNECTOR}_rename"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                first_name  TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, first_name, _action) VALUES (%s, %s, %s)",
            ["contact-1", "Alice", "insert"],
        )
        await conn.commit()

    mapping = FieldMapping(source="first_name", target="firstName")
    connector = _make_connector([mapping])
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted_bodies: list[dict] = []

    def _post_handler(request: httpx.Request) -> httpx.Response:
        posted_bodies.append(orjson.loads(request.content))
        return httpx.Response(201, json={"id": "contact-1"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/contacts").mock(side_effect=_post_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(posted_bodies) == 1
    body = posted_bodies[0]
    assert "firstName" in body
    assert body["firstName"] == "Alice"
    assert "first_name" not in body


@pytest.mark.anyio
async def test_field_mapping_cast_int_field(pool):
    """FieldMapping casts string age to int before the outbound POST."""
    delta_table = f"inout_delta_{_CONNECTOR}_cast"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                age         TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, age, _action) VALUES (%s, %s, %s)",
            ["contact-2", "25", "insert"],
        )
        await conn.commit()

    mapping = FieldMapping(source="age", target="age", cast="int")
    connector = _make_connector([mapping])
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted_bodies: list[dict] = []

    def _post_handler(request: httpx.Request) -> httpx.Response:
        posted_bodies.append(orjson.loads(request.content))
        return httpx.Response(201, json={"id": "contact-2"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/contacts").mock(side_effect=_post_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(posted_bodies) == 1
    body = posted_bodies[0]
    assert body["age"] == 25
    assert isinstance(body["age"], int)


@pytest.mark.anyio
async def test_field_mapping_default_for_missing_field(pool):
    """FieldMapping supplies a default value when the source field is absent from the row."""
    delta_table = f"inout_delta_{_CONNECTOR}_default"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, _action) VALUES (%s, %s)",
            ["contact-3", "insert"],
        )
        await conn.commit()

    # 'status' column doesn't exist — field_mapper should apply the default
    mapping = FieldMapping(source="status", target="status", default="active")
    connector = _make_connector([mapping])
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted_bodies: list[dict] = []

    def _post_handler(request: httpx.Request) -> httpx.Response:
        posted_bodies.append(orjson.loads(request.content))
        return httpx.Response(201, json={"id": "contact-3"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/contacts").mock(side_effect=_post_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(posted_bodies) == 1
    body = posted_bodies[0]
    assert body["status"] == "active"
