"""Integration tests for separate processing paths per operation type (T2 #15).

Covers:
- Insert rows route exclusively to the insert (POST) endpoint
- Update rows route exclusively to the update (PATCH) endpoint
- Delete rows route exclusively to the delete (DELETE) endpoint — not to update
- Delete rows are safely skipped when ops.delete is not configured
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

_BASE_URL = "https://api.separate-paths-test.example.com"
_CONNECTOR = "separate_paths_test"
_DATATYPE = "entities"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_SEPARATE_PATHS_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_SEPARATE_PATHS_TEST_KEY", None)


def _make_full_ops_connector() -> ConnectorConfig:
    """Connector with all three operation types configured."""
    return ConnectorConfig(
        name=_CONNECTOR,
        system="SeparatePathsSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="separate_paths_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert", "update", "delete"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/entities/${{external_id}}"),
                        insert=OperationConfig(method="POST", path="/v1/entities"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/entities/${{external_id}}"),
                        delete=OperationConfig(method="DELETE", path=f"/v1/entities/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


def _make_update_only_connector() -> ConnectorConfig:
    """Connector with only update configured — no delete endpoint."""
    return ConnectorConfig(
        name=_CONNECTOR,
        system="SeparatePathsSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="separate_paths_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/entities/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/entities/${{external_id}}"),
                        # no delete endpoint configured
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_insert_routes_only_to_post_not_patch(pool, run_migrations):
    """T2 #15: _action=insert triggers POST only — PATCH and DELETE are never called."""
    delta_table = f"inout_delta_{_CONNECTOR}_insert_path"
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
            ["entity-1", "Alpha", "insert"],
        )
        await conn.commit()

    connector = _make_full_ops_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    post_called = [False]
    patch_called = [False]
    delete_called = [False]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        def _post_handler(request: httpx.Request) -> httpx.Response:
            post_called[0] = True
            return httpx.Response(201, json={"id": "entity-1"})

        def _patch_handler(request: httpx.Request) -> httpx.Response:
            patch_called[0] = True
            return httpx.Response(200, json={"id": "entity-1"})

        def _delete_handler(request: httpx.Request) -> httpx.Response:
            delete_called[0] = True
            return httpx.Response(204)

        mock.post("/v1/entities").mock(side_effect=_post_handler)
        mock.patch(url__regex=r"/v1/entities/.*").mock(side_effect=_patch_handler)
        mock.delete(url__regex=r"/v1/entities/.*").mock(side_effect=_delete_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert post_called[0] is True, "POST should be called for insert action"
    assert patch_called[0] is False, "PATCH must not be called for insert action"
    assert delete_called[0] is False, "DELETE must not be called for insert action"


@pytest.mark.anyio
async def test_delete_routes_only_to_delete_not_patch(pool, run_migrations):
    """T2 #15: _action=delete triggers DELETE only — PATCH is never called (no cross-contamination)."""
    delta_table = f"inout_delta_{_CONNECTOR}_delete_path"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                _action     TEXT NOT NULL DEFAULT 'delete'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, _action) VALUES (%s, %s)",
            ["entity-2", "delete"],
        )
        await conn.commit()

    connector = _make_full_ops_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_called = [False]
    delete_called = [False]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        def _patch_handler(request: httpx.Request) -> httpx.Response:
            patch_called[0] = True
            return httpx.Response(200, json={"id": "entity-2"})

        def _delete_handler(request: httpx.Request) -> httpx.Response:
            delete_called[0] = True
            return httpx.Response(204)

        mock.patch(url__regex=r"/v1/entities/.*").mock(side_effect=_patch_handler)
        mock.delete(url__regex=r"/v1/entities/.*").mock(side_effect=_delete_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert delete_called[0] is True, "DELETE should be called for delete action"
    assert patch_called[0] is False, "PATCH must not be called for delete action (no cross-contamination)"


@pytest.mark.anyio
async def test_delete_skipped_when_no_delete_endpoint_configured(pool, run_migrations):
    """T2 #15: _action=delete rows are safely skipped when ops.delete is not configured — not routed to update."""
    delta_table = f"inout_delta_{_CONNECTOR}_delete_skip"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL
            )
        """)
        # Mix: two updates and one delete (delete should be skipped)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["entity-3", "Beta", "update"],
        )
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["entity-4", "Gamma", "delete"],
        )
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["entity-5", "Delta", "update"],
        )
        await conn.commit()

    connector = _make_update_only_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patch_called_ids: list[str] = []
    delete_called = [False]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        def _patch_handler(request: httpx.Request) -> httpx.Response:
            # Extract entity id from path
            entity_id = request.url.path.split("/")[-1]
            patch_called_ids.append(entity_id)
            return httpx.Response(200, json={"id": entity_id})

        def _delete_handler(request: httpx.Request) -> httpx.Response:
            delete_called[0] = True
            return httpx.Response(204)

        mock.patch(url__regex=r"/v1/entities/.*").mock(side_effect=_patch_handler)
        mock.delete(url__regex=r"/v1/entities/.*").mock(side_effect=_delete_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    # 2 updates processed, 1 delete skipped
    assert result.processed == 2, f"Expected 2 processed (updates only); got {result}"
    assert result.skipped == 1, f"Expected 1 skipped (delete without endpoint); got {result}"
    assert set(patch_called_ids) == {"entity-3", "entity-5"}, (
        f"Only update rows should trigger PATCH; got {patch_called_ids}"
    )
    assert delete_called[0] is False, "DELETE endpoint should not be called when ops.delete is None"
