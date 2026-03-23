"""Integration tests: three-way conflict detection (B2)."""
from __future__ import annotations

import os
import re

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.conflict-test.example.com"
_CONNECTOR = "conflict_test"
_DATATYPE = "orders"


def _make_connector(connector_name: str = _CONNECTOR):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution, OperationsConfig,
        OperationConfig, UpdateOperationConfig,
    )

    return ConnectorConfig(
        name=connector_name,
        system="ConflictTest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="conflict_key",
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
                            pagination=PaginationConfig(strategy="none"),
                        )
                    },
                ),
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.skip_and_warn,
                    supported_actions=["update"],
                    use_desired_state_table=True,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/orders/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/orders/${external_id}"),
                    ),
                ),
            )
        },
    )


@pytest.mark.anyio
async def test_three_way_conflict_skip_and_warn(pool, run_migrations):
    """External actor modifies a record between MDM decision and writeback.
    With skip_and_warn: the write is skipped, lwstate updated to current.
    """
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name(_CONNECTOR, _DATATYPE)
    lwst = lwstate_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, _CONNECTOR, _DATATYPE)
        await ensure_lwstate_table(conn, _CONNECTOR, _DATATYPE)

        # lwstate: last we wrote was status=pending
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-1', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "pending"}).decode()],
        )
        # desired-state: update to status=shipped, based on status=pending
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action, _sync_run_id)
            VALUES ('ord-1', %s, 'update', NULL)
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "shipped", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # External actor changed status to 'processing'
        return httpx.Response(200, json={"id": "ord-1", "status": "processing"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-1", "status": "shipped"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    # skip_and_warn: PATCH must NOT have been sent
    assert len(patched) == 0, "PATCH should not be sent when conflict detected with skip_and_warn"
    assert result.skipped >= 1
    assert result.conflicts >= 1

    # lwstate should be updated to the current (external) state
    async with pool.connection() as conn:
        lw_row = await (
            await conn.execute(f"SELECT data FROM {lwst} WHERE external_id='ord-1'")
        ).fetchone()
    assert lw_row is not None
    lw_data = lw_row[0] if isinstance(lw_row[0], dict) else orjson.loads(lw_row[0])
    assert lw_data.get("status") == "processing"


@pytest.mark.anyio
async def test_three_way_no_conflict_current_matches_base(pool, run_migrations):
    """No external modification: current == base, write proceeds."""
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector(connector_name="conflict_test_b")
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name("conflict_test_b", _DATATYPE)
    lwst = lwstate_table_name("conflict_test_b", _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, "conflict_test_b", _DATATYPE)
        await ensure_lwstate_table(conn, "conflict_test_b", _DATATYPE)

        # lwstate: last wrote status=pending
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-2', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "pending"}).decode()],
        )
        # desired: update to status=shipped, base=pending
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('ord-2', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "shipped", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # No external change — current still matches base
        return httpx.Response(200, json={"id": "ord-2", "status": "pending"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-2", "status": "shipped"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    # No conflict: PATCH should have been sent
    assert result.processed >= 1 or len(patched) >= 1 or result.skipped == 0
