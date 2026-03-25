"""Integration tests for post-write verification / level 3 protection (T2 #38)."""
from __future__ import annotations

import os
import re

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

_CONNECTOR = "post_verify_test"
_DATATYPE = "records"
_BASE_URL = "https://api.post-verify.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_POST_VERIFY_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_POST_VERIFY_TEST_KEY", None)


def _make_connector(conflict_resolution: ConflictResolution) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="PostVerifySystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="post_verify_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.post_write_verify,
                    conflict_resolution=conflict_resolution,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/records/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/records/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_post_write_verify_passes_when_state_matches(pool):
    """After a successful PATCH, the verification GET confirms the state matches.

    Expected: row counted as processed (verification passes).
    """
    delta_table = f"inout_delta_{_CONNECTOR}_verify_ok"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, status, _action) VALUES (%s, %s, %s)",
            ["rec-verify-1", "shipped", "update"],
        )
        await conn.commit()

    connector = _make_connector(ConflictResolution.dead_letter)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/records/rec-verify-1").mock(
            return_value=httpx.Response(200, json={"id": "rec-verify-1", "status": "shipped"})
        )
        # Verification GET returns data matching what was sent
        mock.get("/v1/records/rec-verify-1").mock(
            return_value=httpx.Response(200, json={"id": "rec-verify-1", "status": "shipped"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0


@pytest.mark.anyio
async def test_post_write_verify_dead_letters_on_mismatch(pool):
    """After a successful PATCH, the verification GET shows the state did not stick.

    With conflict_resolution=dead_letter the row transitions to failed.
    Expected: processed=0, failed=1 (net effect of process/unprocess).
    """
    delta_table = f"inout_delta_{_CONNECTOR}_verify_mismatch"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, status, _action) VALUES (%s, %s, %s)",
            ["rec-verify-2", "shipped", "update"],
        )
        await conn.commit()

    connector = _make_connector(ConflictResolution.dead_letter)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/records/rec-verify-2").mock(
            return_value=httpx.Response(200, json={"id": "rec-verify-2"})
        )
        # Verification GET returns stale data — the write did not stick
        mock.get("/v1/records/rec-verify-2").mock(
            return_value=httpx.Response(200, json={"id": "rec-verify-2", "status": "pending"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    # After _post_write_verify does processed-=1, and the outer code does processed+=1,
    # the net result with dead_letter mismatch is: processed=0, failed=1
    assert result.failed == 1
    assert result.processed == 0


@pytest.mark.anyio
async def test_post_write_verify_insert_action_also_verified(pool):
    """Level-3 verification also fires for insert actions (POST + GET verify)."""
    delta_table = f"inout_delta_{_CONNECTOR}_verify_insert"
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
            ["rec-verify-3", "New Record", "insert"],
        )
        await conn.commit()

    connector_with_insert = ConnectorConfig(
        name=_CONNECTOR,
        system="PostVerifySystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="post_verify_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.post_write_verify,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/records/${{external_id}}"),
                        insert=OperationConfig(method="POST", path="/v1/records"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/records/${{external_id}}"),
                    ),
                ),
            ),
        },
    )
    writeback_cfg = connector_with_insert.datatypes[_DATATYPE].writeback

    get_count: list[int] = [0]

    def _get_handler(request: httpx.Request) -> httpx.Response:
        get_count[0] += 1
        # Verification GET returns matching data
        return httpx.Response(200, json={"id": "rec-verify-3", "name": "New Record"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/records").mock(
            return_value=httpx.Response(201, json={"id": "rec-verify-3"})
        )
        mock.get("/v1/records/rec-verify-3").mock(side_effect=_get_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector_with_insert, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert get_count[0] == 1, "Verification GET must be called once after INSERT"
