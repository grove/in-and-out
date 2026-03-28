"""Integration tests for external reference field injection (T2 #16)."""
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
    OperationsConfig, OperationConfig, UpdateOperationConfig,
)
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "ext_ref_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.ext-ref-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_EXT_REF_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_EXT_REF_TEST_KEY", None)


def _make_connector(external_reference_field: str | None, action: str = "insert") -> ConnectorConfig:
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path=f"/v1/contacts/${{external_id}}"),
        insert=OperationConfig(method="POST", path="/v1/contacts"),
        update=UpdateOperationConfig(method="PATCH", path=f"/v1/contacts/${{external_id}}"),
    )
    return ConnectorConfig(
        name=_CONNECTOR,
        system="ExtRefSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="ext_ref_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=[action],
                    external_reference_field=external_reference_field,
                    operations=ops,
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_external_reference_field_injected_in_insert(pool):
    """external_reference_field injects _cluster_id into the POST body under the configured key."""
    delta_table = f"inout_delta_{_CONNECTOR}_ref_insert"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert',
                _cluster_id TEXT
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action, _cluster_id) VALUES (%s, %s, %s, %s)",
            ["contact-1", "Alice", "insert", "cluster-42"],
        )
        await conn.commit()

    connector = _make_connector(external_reference_field="mdm_cluster_id", action="insert")
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
    assert "mdm_cluster_id" in body, f"Expected mdm_cluster_id in POST body, got: {body}"
    assert body["mdm_cluster_id"] == "cluster-42"


@pytest.mark.anyio
async def test_external_reference_field_injected_in_update(pool):
    """external_reference_field is also injected for update (PATCH) actions."""
    delta_table = f"inout_delta_{_CONNECTOR}_ref_update"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update',
                _cluster_id TEXT
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, status, _action, _cluster_id) VALUES (%s, %s, %s, %s)",
            ["contact-2", "active", "update", "cluster-99"],
        )
        await conn.commit()

    connector = _make_connector(external_reference_field="mdm_id", action="update")
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patched_bodies: list[dict] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patched_bodies.append(orjson.loads(request.content))
        return httpx.Response(200, json={"id": "contact-2"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-2").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(patched_bodies) == 1
    body = patched_bodies[0]
    assert "mdm_id" in body, f"Expected mdm_id in PATCH body, got: {body}"
    assert body["mdm_id"] == "cluster-99"


@pytest.mark.anyio
async def test_no_external_reference_when_cluster_id_absent(pool):
    """No injection happens when _cluster_id is NULL even if external_reference_field is set."""
    delta_table = f"inout_delta_{_CONNECTOR}_no_ref"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert',
                _cluster_id TEXT
            )
        """)
        # _cluster_id is NULL
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["contact-3", "Dave", "insert"],
        )
        await conn.commit()

    connector = _make_connector(external_reference_field="mdm_cluster_id", action="insert")
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
    # mdm_cluster_id should NOT appear when _cluster_id is absent
    assert "mdm_cluster_id" not in body
