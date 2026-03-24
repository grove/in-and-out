"""
End-to-end test simulating the full OSI-Mapping pipeline:
  External API → [Ingestion] → inout_src_* → [Simulated OSI view] → inout_dst_* → [Writeback] → Target API

This test validates the complete data flow and contract between all three layers.
Does NOT require a real OSI-Mapping installation — we manually populate the desired-state
table as OSI-Mapping would, then verify the writeback executes correctly.
"""
from __future__ import annotations

import os
import re
import uuid

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "osi_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.osi-test.example.com"


def _make_connector(connector_name: str = _CONNECTOR):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution,
        OperationsConfig, OperationConfig, UpdateOperationConfig,
    )

    return ConnectorConfig(
        name=connector_name,
        system="OSITest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="osi_key",
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
                            path="/v1/contacts",
                            record_selector="contacts",
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
                    conflict_resolution=ConflictResolution.skip_and_warn,
                    supported_actions=["update"],
                    use_desired_state_table=True,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/contacts/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/contacts/${external_id}"),
                    ),
                ),
            )
        },
    )


@pytest.mark.anyio
async def test_full_osi_pipeline(pool, run_migrations):
    """
    Complete pipeline: ingest → OSI populates desired-state → writeback executes.
    Verifies the schema contract at each boundary.
    """
    os.environ["INOUT_CREDENTIAL_OSI_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.writeback.engine import WritebackEngine
    from inandout.postgres.schema import source_table_name, ensure_source_table
    from inandout.postgres.desired_state import (
        ensure_desired_state_table, ensure_lwstate_table,
        desired_state_table_name, lwstate_table_name,
    )

    connector = _make_connector()

    # ── Step 1: Ingestion ──────────────────────────────────────────────────
    contacts = [
        {"id": "c-1", "name": "Alice", "email": "alice@example.com", "status": "active"},
        {"id": "c-2", "name": "Bob",   "email": "bob@example.com",   "status": "active"},
    ]
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(return_value=httpx.Response(
            200, json={"contacts": contacts, "next_cursor": None}
        ))
        engine = IngestionEngine(pool)
        sync_result = await engine.run_sync(
            connector, _DATATYPE, connector.datatypes[_DATATYPE].ingestion
        )
    assert sync_result.status == "completed"
    assert sync_result.records_inserted == 2

    # Verify source table schema contract (T1 #2)
    src = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT external_id, data, raw, _schema_version FROM {src} WHERE external_id='c-1'"
        )).fetchone()
    assert row is not None
    assert row[0] == "c-1"
    assert row[2] is not None  # raw preserved

    # ── Step 2: Simulate OSI-Mapping desired-state population ─────────────
    # OSI has computed that c-1's status should change to "vip"
    dst = desired_state_table_name(_CONNECTOR, _DATATYPE)
    lwst = lwstate_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, _CONNECTOR, _DATATYPE)
        await ensure_lwstate_table(conn, _CONNECTOR, _DATATYPE)

        # Simulate lwstate (what writeback last wrote)
        # Use the actual lwstate table schema: external_id, data, _written_at
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('c-1', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _written_at=NOW()
            """,
            [orjson.dumps({"name": "Alice", "status": "active"}).decode()],
        )

        # OSI inserts desired-state row
        # The desired-state table schema: external_id, data, _action, _schema_version, etc.
        # We encode base into the data field alongside the desired values
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('c-1', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({
                "name": "Alice",
                "status": "vip",
                "_base": {"name": "Alice", "status": "active"},
            }).decode()],
        )
        await conn.commit()

    # ── Step 3: Writeback executes the desired-state ──────────────────────
    patched = {}

    def _handle_get(request):
        # Pre-flight read returns current state = base (no external modification)
        return httpx.Response(200, json={"id": "c-1", "name": "Alice", "status": "active"})

    def _handle_patch(request):
        patched["c-1"] = orjson.loads(request.content)
        return httpx.Response(200, json={"id": "c-1", "name": "Alice", "status": "vip"})

    wb_cfg = connector.datatypes[_DATATYPE].writeback
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/contacts/\w[\w-]*")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/contacts/\w[\w-]*")).mock(side_effect=_handle_patch)

        wb_engine = WritebackEngine(pool)
        wb_result = await wb_engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    assert wb_result.processed == 1, f"Expected 1 processed, got {wb_result.processed}; failed={wb_result.failed}"
    assert wb_result.failed == 0
    assert wb_result.conflicts == 0
    assert "c-1" in patched, f"PATCH not sent for c-1; patched={patched}"
    assert patched["c-1"].get("status") == "vip"

    # ── Step 4: Verify lwstate updated ────────────────────────────────────
    async with pool.connection() as conn:
        lw_row = await (await conn.execute(
            f"SELECT data FROM {lwst} WHERE external_id='c-1'"
        )).fetchone()
    assert lw_row is not None
    lw_data = lw_row[0] if isinstance(lw_row[0], dict) else orjson.loads(lw_row[0])
    assert lw_data.get("status") == "vip"


@pytest.mark.anyio
async def test_osi_pipeline_conflict_detected(pool, run_migrations):
    """
    External actor modifies the record between OSI decision and writeback.
    Three-way comparison detects conflict → skip_and_warn → lwstate updated to current.
    """
    os.environ["INOUT_CREDENTIAL_OSI_KEY"] = "dummy"
    from inandout.writeback.engine import WritebackEngine
    from inandout.postgres.desired_state import (
        ensure_desired_state_table, ensure_lwstate_table,
        desired_state_table_name, lwstate_table_name,
    )

    conflict_connector = "osi_test_conflict"
    connector = _make_connector(connector_name=conflict_connector)

    dst = desired_state_table_name(conflict_connector, _DATATYPE)
    lwst = lwstate_table_name(conflict_connector, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, conflict_connector, _DATATYPE)
        await ensure_lwstate_table(conn, conflict_connector, _DATATYPE)

        # lwstate: last we wrote was status=active
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('c-x', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "active"}).decode()],
        )

        # OSI desired: status=vip, base: status=active
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('c-x', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({
                "status": "vip",
                "_base": {"status": "active"},
            }).decode()],
        )
        await conn.commit()

    wb_cfg = connector.datatypes[_DATATYPE].writeback
    patched = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Pre-flight returns status=processing — external actor changed it!
        mock.get(re.compile(r"/v1/contacts/\w[\w-]*")).mock(
            return_value=httpx.Response(200, json={"status": "processing"})
        )
        mock.patch(re.compile(r"/v1/contacts/\w[\w-]*")).mock(
            side_effect=lambda r: patched.append(r) or httpx.Response(200, json={})
        )

        wb_engine = WritebackEngine(pool)
        wb_result = await wb_engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, dst
        )

    # skip_and_warn: no PATCH sent
    assert len(patched) == 0, f"PATCH should not be sent when conflict detected; patched={patched}"
    assert wb_result.skipped >= 1
    assert wb_result.conflicts >= 1

    # lwstate updated to current (status=processing)
    async with pool.connection() as conn:
        lw_row = await (await conn.execute(
            f"SELECT data FROM {lwst} WHERE external_id='c-x'"
        )).fetchone()
    assert lw_row is not None
    lw_data = lw_row[0] if isinstance(lw_row[0], dict) else orjson.loads(lw_row[0])
    assert lw_data.get("status") == "processing"
