"""Integration tests for merge and split actions (T2 #34)."""
from __future__ import annotations

import json
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

_CONNECTOR = "merge_split_test"
_DATATYPE = "entities"
_BASE_URL = "https://api.merge-split-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_MERGE_SPLIT_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_MERGE_SPLIT_TEST_KEY", None)


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="MergeSplitSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="merge_split_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update", "insert", "delete", "merge", "split"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/entities/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/entities/${{external_id}}"),
                        insert=OperationConfig(method="POST", path="/v1/entities"),
                        delete=OperationConfig(method="DELETE", path=f"/v1/entities/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_merge_updates_survivor_and_deletes_losers(pool):
    """Merge action PATCHes the survivor record and DELETEs each loser."""
    delta_table = f"inout_delta_{_CONNECTOR}_merge"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id  TEXT,
                name         TEXT,
                _action      TEXT NOT NULL DEFAULT 'update',
                _cluster_id  TEXT,
                _losing_ids  JSONB
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action, _losing_ids) "
            "VALUES (%s, %s, %s, %s::jsonb)",
            ["survivor-1", "Merged Entity", "merge", json.dumps(["loser-1", "loser-2"])],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patched = []
    deleted = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patched.append(request.url.path)
        return httpx.Response(200, json={"id": "survivor-1", "name": "Merged Entity"})

    def _delete_handler(request: httpx.Request) -> httpx.Response:
        deleted.append(request.url.path)
        return httpx.Response(204)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/entities/survivor-1").mock(side_effect=_patch_handler)
        mock.delete("/v1/entities/loser-1").mock(side_effect=_delete_handler)
        mock.delete("/v1/entities/loser-2").mock(side_effect=_delete_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(patched) == 1
    assert len(deleted) == 2
    assert "/v1/entities/loser-1" in deleted
    assert "/v1/entities/loser-2" in deleted


@pytest.mark.anyio
async def test_split_creates_child_records(pool):
    """Split action POSTs each child payload from _split_rows."""
    delta_table = f"inout_delta_{_CONNECTOR}_split"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id  TEXT,
                _action      TEXT NOT NULL DEFAULT 'update',
                _cluster_id  TEXT,
                _split_rows  JSONB
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, _action, _split_rows) "
            "VALUES (%s, %s, %s::jsonb)",
            [
                "parent-1",
                "split",
                json.dumps([{"name": "Child A"}, {"name": "Child B"}]),
            ],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted = []
    child_counter: list[int] = [0]

    def _post_handler(request: httpx.Request) -> httpx.Response:
        posted.append(orjson.loads(request.content))
        child_counter[0] += 1
        return httpx.Response(201, json={"id": f"child-{child_counter[0]}"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/entities").mock(side_effect=_post_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(posted) == 2
    assert any(p.get("name") == "Child A" for p in posted)
    assert any(p.get("name") == "Child B" for p in posted)


@pytest.mark.anyio
async def test_merge_skipped_when_no_update_operation(pool):
    """Merge action is skipped and counted when WritebackConfig has no update operation."""
    delta_table = f"inout_delta_{_CONNECTOR}_merge_skip"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id  TEXT,
                name         TEXT,
                _action      TEXT NOT NULL DEFAULT 'update',
                _losing_ids  JSONB
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action, _losing_ids) "
            "VALUES (%s, %s, %s, %s::jsonb)",
            ["survivor-2", "Entity", "merge", json.dumps(["loser-3"])],
        )
        await conn.commit()

    connector = _make_connector()
    # WritebackConfig without update operation — merge should be skipped
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "delete"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/entities/${external_id}"),
            insert=OperationConfig(method="POST", path="/v1/entities"),
            delete=OperationConfig(method="DELETE", path="/v1/entities/${external_id}"),
        ),
    )

    with respx.mock(base_url=_BASE_URL, assert_all_called=False):
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.skipped >= 1
    assert result.failed == 0
    assert result.processed == 0
