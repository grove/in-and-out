"""Integration tests for T2 #6: CRDT (Conflict-free Replicated Data Type) support.

Covers:
- lww_register: remote is newer → write skipped
- lww_register: local is newer → write proceeds
- g_counter: only the delta (local − remote) is sent, not the absolute value
"""
from __future__ import annotations

import os

import httpx
import orjson
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

_CONNECTOR = "crdt_test"
_DATATYPE = "items"
_BASE_URL = "https://api.crdt-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_CRDT_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_CRDT_TEST_KEY", None)


def _make_connector(crdt_type: str, crdt_ts_field: str | None = None) -> ConnectorConfig:
    crdt_kwargs: dict = {"crdt_type": crdt_type}
    if crdt_ts_field is not None:
        crdt_kwargs["crdt_ts_field"] = crdt_ts_field
    return ConnectorConfig(
        name=_CONNECTOR,
        system="CrdtSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="crdt_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    **crdt_kwargs,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/items/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/items/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_lww_register_skips_write_when_remote_is_newer(pool, run_migrations):
    """With lww_register, if the remote timestamp is strictly newer, the write is skipped."""
    delta_table = f"inout_delta_{_CONNECTOR}_lww_skip"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                ts          INTEGER,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        # Local timestamp 1000 — remote will return ts=2000 (newer), so write should be skipped
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, ts, _action) VALUES (%s, %s, %s, %s)",
            ["item-lww-1", "Old Name", 1000, "update"],
        )
        await conn.commit()

    connector = _make_connector(crdt_type="lww_register", crdt_ts_field="ts")
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_calls: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Remote state has ts=2000 → newer than local ts=1000
        mock.get("/v1/items/item-lww-1").mock(
            return_value=httpx.Response(200, json={"name": "Current Name", "ts": 2000})
        )
        mock.patch("/v1/items/item-lww-1").mock(
            side_effect=lambda req: (patch_calls.append(req), httpx.Response(200, json={}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.skipped >= 1, f"Expected skipped>=1 but got {result}"
    assert len(patch_calls) == 0, "PATCH must NOT be sent when remote timestamp is newer (LWW skip)"


@pytest.mark.anyio
async def test_lww_register_writes_when_local_is_newer(pool, run_migrations):
    """With lww_register, if the local timestamp is newer, the PATCH proceeds normally."""
    delta_table = f"inout_delta_{_CONNECTOR}_lww_write"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                ts          INTEGER,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        # Local timestamp 3000 — remote will return ts=500 (older), so write should proceed
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, ts, _action) VALUES (%s, %s, %s, %s)",
            ["item-lww-2", "Updated Name", 3000, "update"],
        )
        await conn.commit()

    connector = _make_connector(crdt_type="lww_register", crdt_ts_field="ts")
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_calls: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Remote state has ts=500 → older than local ts=3000
        mock.get("/v1/items/item-lww-2").mock(
            return_value=httpx.Response(200, json={"name": "Stale Name", "ts": 500})
        )
        mock.patch("/v1/items/item-lww-2").mock(
            side_effect=lambda req: (patch_calls.append(req), httpx.Response(200, json={}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1, f"Expected processed=1 but got {result}"
    assert len(patch_calls) == 1, "PATCH must be sent when local timestamp is newer"


@pytest.mark.anyio
async def test_g_counter_sends_delta_not_absolute_value(pool, run_migrations):
    """With g_counter, the engine sends only the increment (local − remote) for numeric fields."""
    delta_table = f"inout_delta_{_CONNECTOR}_gcounter"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                views       INTEGER,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        # Local absolute value: views=150
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, views, name, _action) VALUES (%s, %s, %s, %s)",
            ["item-gc-1", 150, "Post Title", "update"],
        )
        await conn.commit()

    connector = _make_connector(crdt_type="g_counter")
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_bodies: list[dict] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Remote already has views=100
        mock.get("/v1/items/item-gc-1").mock(
            return_value=httpx.Response(200, json={"views": 100, "name": "Post Title"})
        )
        mock.patch("/v1/items/item-gc-1").mock(
            side_effect=lambda req: (patch_bodies.append(orjson.loads(req.read())), httpx.Response(200, json={}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1, f"Expected processed=1 but got {result}"
    assert len(patch_bodies) == 1, "PATCH must be sent"
    body = patch_bodies[0]
    assert body.get("views") == 50, (
        f"g_counter must send the delta (150 − 100 = 50), not the absolute 150. Got views={body.get('views')}"
    )
    # Non-numeric field forwarded as-is
    assert body.get("name") == "Post Title"
