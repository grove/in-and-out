"""Integration tests for control table command execution."""
from __future__ import annotations

import os
import uuid

import pytest
import respx
import httpx
import orjson

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.engine.control import ControlDispatcher
from inandout.ingestion.engine import IngestionEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")


def _make_connector(name: str = "ctrl_test") -> ConnectorConfig:
    return ConnectorConfig(
        name=name,
        system="TestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/items",
                            record_selector="results",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )


async def _insert_control_command(
    pool,
    command: str,
    connector: str | None = None,
    datatype: str | None = None,
    payload: dict | None = None,
) -> uuid.UUID:
    cmd_id = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO inout_ops_control (id, connector, datatype, command, payload)
               VALUES (%s, %s, %s, %s, %s)""",
            [cmd_id, connector, datatype, command, orjson.dumps(payload or {}).decode()],
        )
        await conn.commit()
    return cmd_id


async def _get_command_status(pool, cmd_id: uuid.UUID) -> dict:
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT status, result FROM inout_ops_control WHERE id = %s",
            [cmd_id],
        )).fetchone()
    if row is None:
        return {}
    result = row[1]
    if isinstance(result, (str, bytes)):
        result = orjson.loads(result)
    return {"status": row[0], "result": result}


@pytest.mark.anyio
async def test_force_full_sync_clears_watermark(pool, run_migrations):
    """force_full_sync removes the watermark so the next sync is a full sync."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_connector("ctrl_ffs")
    ingestion_cfg = connector.datatypes["items"].ingestion
    assert ingestion_cfg is not None

    # First: run a sync to establish a watermark
    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/items").mock(return_value=httpx.Response(
            200, json={"results": [{"id": "1", "updated_at": "2026-01-01"}], "next_cursor": None}
        ))
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "items", ingestion_cfg)
    assert result.status == "completed"

    # Verify watermark exists (if incremental config set it — this connector has no cursor_field
    # so watermark won't be set, but we can insert one manually)
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO inout_ops_watermark (connector, datatype, watermark_type, watermark_value)
               VALUES (%s, %s, 'timestamp', '2026-01-01')
               ON CONFLICT (connector, datatype) DO UPDATE SET watermark_value = EXCLUDED.watermark_value""",
            ["ctrl_ffs", "items"],
        )
        await conn.commit()

    # Issue force_full_sync command
    cmd_id = await _insert_control_command(
        pool, "force_full_sync", connector="ctrl_ffs", datatype="items"
    )

    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)
    count = await dispatcher.dispatch_pending()
    assert count >= 1  # may pick up commands from other tests in the suite

    status = await _get_command_status(pool, cmd_id)
    assert status["status"] == "completed"
    assert "ctrl_ffs/items" in status["result"].get("cleared", "")

    # Verify watermark is gone
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT watermark_value FROM inout_ops_watermark WHERE connector='ctrl_ffs' AND datatype='items'"
        )).fetchone()
    assert row is None


@pytest.mark.anyio
async def test_pause_and_resume_connector(pool, run_migrations):
    """pause_connector and resume_connector modify the in-process paused set."""
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    # Pause
    cmd_pause = await _insert_control_command(
        pool, "pause_connector", connector="hubspot", datatype="contacts"
    )
    await dispatcher.dispatch_pending()
    status = await _get_command_status(pool, cmd_pause)
    assert status["status"] == "completed"
    assert ("hubspot", "contacts") in paused

    # Resume
    cmd_resume = await _insert_control_command(
        pool, "resume_connector", connector="hubspot", datatype="contacts"
    )
    await dispatcher.dispatch_pending()
    status = await _get_command_status(pool, cmd_resume)
    assert status["status"] == "completed"
    assert ("hubspot", "contacts") not in paused


@pytest.mark.anyio
async def test_unknown_command_marked_failed(pool, run_migrations):
    """Unknown commands are acknowledged and marked failed, not raised."""
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    cmd_id = await _insert_control_command(pool, "totally_unknown_command")
    await dispatcher.dispatch_pending()

    status = await _get_command_status(pool, cmd_id)
    assert status["status"] == "failed"
    assert "Unknown command" in (status["result"] or {}).get("error", "")


@pytest.mark.anyio
async def test_requeue_dead_letter_moves_rows_to_source_table(pool, run_migrations):
    """requeue_dead_letter re-upserts DL rows into the source table."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector_name = "ctrl_requeue"
    datatype = "items"

    # Ensure source table and DL table exist
    from inandout.postgres.schema import (
        source_table_name, ensure_source_table,
        dead_letter_table_name, ensure_dead_letter_table,
    )
    src_table = source_table_name(connector_name, datatype)
    dl_table = dead_letter_table_name("ingestion", connector_name, datatype)
    async with pool.connection() as conn:
        await ensure_source_table(conn, connector_name, datatype)
        await ensure_dead_letter_table(conn, "ingestion", connector_name, datatype)
        # Insert a DL row
        await conn.execute(
            f"""INSERT INTO {dl_table} (external_id, raw, error_message, error_class)
                VALUES ('dl1', %s, 'missing pk', 'data_error')""",
            [orjson.dumps({"id": "dl1", "name": "Rescued"}).decode()],
        )
        await conn.commit()

    paused: set = set()
    engine = IngestionEngine(pool)
    dispatcher = ControlDispatcher(pool, paused)

    cmd_id = await _insert_control_command(
        pool, "requeue_dead_letter",
        connector=connector_name, datatype=datatype,
        payload={"limit": 10},
    )
    await dispatcher.dispatch_pending(engine=engine)

    status = await _get_command_status(pool, cmd_id)
    assert status["status"] == "completed"
    assert status["result"]["requeued"] >= 1

    # Verify the row landed in the source table
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT external_id FROM {src_table} WHERE external_id = 'dl1'"
        )).fetchone()
    assert row is not None

    # Verify the DL row was stamped with requeued_at
    async with pool.connection() as conn:
        dl_row = await (await conn.execute(
            f"SELECT requeue_count, requeued_at FROM {dl_table} WHERE external_id = 'dl1'"
        )).fetchone()
    assert dl_row is not None
    assert dl_row[0] == 1
    assert dl_row[1] is not None


@pytest.mark.anyio
async def test_validate_command_completes_and_returns_result(pool, run_migrations):
    """T2 #37 / T1 #43: issuing a 'validate' control command marks the row
    completed and returns a result dict with connectivity/auth/errors keys.

    Uses a real HTTP stub (respx) to simulate the target system, so the
    validation probe can actually succeed without a live external dependency.
    """
    import respx

    _VALIDATE_BASE = "https://api.validate-ctrl-test.example.com"
    _VALIDATE_CONNECTOR = "validate_ctrl_test"
    os.environ["INOUT_CREDENTIAL_VALIDATE_CTRL_KEY"] = "dummy"

    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    # Issue validate command via control table (shallow path: base_url in payload)
    # The ControlDispatcher's _cmd_validate_writeback falls back to a direct HTTP
    # probe when no connector_cfg is available in the engine registry.
    cmd_id = await _insert_control_command(
        pool,
        "validate",
        connector=_VALIDATE_CONNECTOR,
        datatype="contacts",
        payload={"base_url": _VALIDATE_BASE},
    )

    with respx.mock(base_url=_VALIDATE_BASE, assert_all_called=False) as mock:
        mock.get("/").mock(return_value=httpx.Response(200, json={"ok": True}))
        mock.head("/").mock(return_value=httpx.Response(200, headers={"ETag": '"abc123"'}))
        await dispatcher.dispatch_pending()

    status = await _get_command_status(pool, cmd_id)
    assert status["status"] == "completed", (
        f"validate command must complete; got {status['status']!r}, result={status['result']}"
    )
    result = status["result"]
    assert isinstance(result, dict), f"validate result must be a dict; got {result!r}"
    assert "connectivity" in result, f"result must have 'connectivity' key; got {result}"
    assert result["connectivity"] == "ok", (
        f"connectivity must be 'ok' for reachable base_url; got {result['connectivity']!r}"
    )
    assert "errors" in result, "result must have 'errors' key"
    # No errors expected for a healthy stub
    assert result["errors"] == [], (
        f"Expected no validation errors for healthy stub; got {result['errors']}"
    )


@pytest.mark.anyio
async def test_validate_command_reports_auth_failure(pool, run_migrations):
    """T2 #37: validate command correctly identifies auth failure (401 from target)."""
    import respx

    _VALIDATE_BASE = "https://api.validate-auth-fail.example.com"
    _VALIDATE_CONNECTOR = "validate_auth_fail_test"

    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    cmd_id = await _insert_control_command(
        pool,
        "validate",
        connector=_VALIDATE_CONNECTOR,
        payload={"base_url": _VALIDATE_BASE},
    )

    with respx.mock(base_url=_VALIDATE_BASE, assert_all_called=False) as mock:
        mock.get("/").mock(return_value=httpx.Response(401, json={"error": "unauthorized"}))
        await dispatcher.dispatch_pending()

    status = await _get_command_status(pool, cmd_id)
    assert status["status"] == "completed", (
        f"validate command must complete even on auth failure; got {status['status']!r}"
    )
    result = status["result"]
    assert result.get("connectivity") == "ok", "Connectivity should be ok (server responded)"
    assert result.get("auth") == "failed", (
        f"auth must be 'failed' when server returns 401; got {result.get('auth')!r}"
    )
    assert len(result.get("errors", [])) >= 1, "Should have at least one error in result"
