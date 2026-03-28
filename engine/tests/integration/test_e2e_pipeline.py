"""End-to-end pipeline smoke test.

Exercises the full ingestion → writeback flow against a real PostgreSQL:
1. Run an ingestion sync (mock HTTP API) → records land in source table
2. Simulate what OSI-Mapping would do: project records into a delta table
3. Run a writeback cycle → HTTP operations dispatched → feedback in result table
4. Verify health endpoint responds (Starlette app, not full uvicorn daemon)
"""
from __future__ import annotations

import os

import pytest
import respx
import httpx
from fastapi.testclient import TestClient

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig, UpdateOperationConfig,
)
from inandout.ingestion.engine import IngestionEngine
from inandout.ingestion.daemon import _build_app
from inandout.writeback.engine import WritebackEngine
from inandout.postgres.schema import source_table_name


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR_NAME = "e2e_test"
_DATATYPE = "orders"
_BASE_URL = "https://api.e2e.example.com"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR_NAME,
        system="E2ESystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="e2e_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/orders",
                            record_selector="orders",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                ),
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/orders/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/orders/${external_id}"),
                    ),
                ),
            )
        },
    )


@pytest.mark.anyio
async def test_ingestion_to_writeback_full_pipeline(pool, run_migrations):
    """Complete ingestion → writeback data flow."""
    os.environ["INOUT_CREDENTIAL_E2E_KEY"] = "dummy"

    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    writeback_cfg = connector.datatypes[_DATATYPE].writeback
    assert ingestion_cfg is not None
    assert writeback_cfg is not None

    # ----------------------------------------------------------------
    # Step 1: Ingestion sync — fetch orders from mock API
    # ----------------------------------------------------------------
    orders = [
        {"id": "ord-1", "status": "pending", "amount": 100},
        {"id": "ord-2", "status": "pending", "amount": 200},
        {"id": "ord-3", "status": "shipped", "amount": 300},
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/orders").mock(return_value=httpx.Response(
            200, json={"orders": orders, "next_cursor": None}
        ))
        engine = IngestionEngine(pool)
        sync_result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert sync_result.status == "completed"
    assert sync_result.records_inserted == 3

    # Verify records are in the source table
    src_table = source_table_name(_CONNECTOR_NAME, _DATATYPE)
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id FROM {src_table} ORDER BY external_id"
        )).fetchall()
    assert [r[0] for r in rows] == ["ord-1", "ord-2", "ord-3"]

    # ----------------------------------------------------------------
    # Step 2: Simulate OSI-Mapping delta projection (status changes)
    # ----------------------------------------------------------------
    delta_table = f"_delta_{_CONNECTOR_NAME}_{_DATATYPE}"
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {delta_table} (
                external_id TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        # Two orders changed to 'fulfilled'
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, status, _action) VALUES (%s, %s, 'update')",
            ["ord-1", "fulfilled"],
        )
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, status, _action) VALUES (%s, %s, 'update')",
            ["ord-2", "fulfilled"],
        )
        await conn.commit()

    # ----------------------------------------------------------------
    # Step 3: Writeback cycle — dispatch PATCH operations
    # ----------------------------------------------------------------
    patched: list[str] = []

    def _handle_patch(request: httpx.Request) -> httpx.Response:
        patched.append(request.url.path.split("/")[-1])
        return httpx.Response(200, json={"status": "ok"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        import re
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        wb_engine = WritebackEngine(pool)
        wb_result = await wb_engine.run_writeback_cycle(
            connector, _DATATYPE, writeback_cfg, delta_table
        )

    assert wb_result.processed == 2
    assert wb_result.failed == 0
    assert set(patched) == {"ord-1", "ord-2"}

    # ----------------------------------------------------------------
    # Step 4: Verify feedback written to inout_ops_writeback_result
    # ----------------------------------------------------------------
    async with pool.connection() as conn:
        fb_rows = await (await conn.execute(
            """SELECT external_id FROM inout_ops_writeback_result
               WHERE connector = %s AND datatype = %s
               ORDER BY external_id""",
            [_CONNECTOR_NAME, _DATATYPE],
        )).fetchall()
    assert {r[0] for r in fb_rows} == {"ord-1", "ord-2"}


@pytest.mark.anyio
async def test_health_endpoint_responds(pool):
    """The FastAPI health app returns 200 OK on /health."""
    connector = _make_connector()
    engine = IngestionEngine(pool)

    app = _build_app(engine, [])

    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        resp2 = client.get("/ready")
        assert resp2.status_code == 200
        assert resp2.json()["status"] == "ready"


@pytest.mark.anyio
async def test_second_sync_is_incremental_after_first(pool, run_migrations):
    """After a full sync establishes a watermark, the second sync uses it."""
    os.environ["INOUT_CREDENTIAL_E2E_KEY"] = "dummy"

    from inandout.config.ingestion import IncrementalConfig, RequestFilterConfig, RequestFilterMode

    incremental_connector = ConnectorConfig(
        name="e2e_inc",
        system="E2ESystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="e2e_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "events": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/events",
                            record_selector="events",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                            incremental=IncrementalConfig(
                                enabled=True,
                                cursor_field="timestamp",
                                cursor_type="timestamp",
                                request_filter=RequestFilterConfig(
                                    mode=RequestFilterMode.query_param,
                                    **{"param": "since", "value": "${watermark}"},
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )

    ingestion_cfg = incremental_connector.datatypes["events"].ingestion
    assert ingestion_cfg is not None

    observed_params: list[dict] = []

    def _handle(request: httpx.Request) -> httpx.Response:
        observed_params.append(dict(request.url.params))
        return httpx.Response(200, json={
            "events": [{"id": "e1", "timestamp": "2026-03-01T00:00:00Z"}],
            "next_cursor": None,
        })

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(side_effect=_handle)

        from inandout.config.tool import DatabaseConfig
        from inandout.postgres.pool import create_pool as _create_pool
        pool1 = await _create_pool(DatabaseConfig(dsn=pool.conninfo))
        try:
            engine1 = IngestionEngine(pool1)
            r1 = await engine1.run_sync(incremental_connector, "events", ingestion_cfg)
            assert r1.status == "completed"
        finally:
            await pool1.close()

        pool2 = await _create_pool(DatabaseConfig(dsn=pool.conninfo))
        try:
            engine2 = IngestionEngine(pool2)
            r2 = await engine2.run_sync(incremental_connector, "events", ingestion_cfg)
            assert r2.status == "completed"
        finally:
            await pool2.close()

    # First request: no 'since' param
    assert "since" not in observed_params[0]
    # Second request: has the watermark from the first sync
    assert "since" in observed_params[1]
    assert observed_params[1]["since"] == "2026-03-01T00:00:00Z"
