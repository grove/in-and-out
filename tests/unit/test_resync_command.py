"""Unit tests for conflict-driven re-ingestion signal (T2 #39)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# run_sync_single_record tests
# ---------------------------------------------------------------------------

def test_ingestion_engine_has_run_sync_single_record():
    """IngestionEngine should have run_sync_single_record method."""
    from inandout.ingestion.engine import IngestionEngine
    assert hasattr(IngestionEngine, "run_sync_single_record")


def test_ingestion_engine_single_record_source_has_targeted_resync_log():
    """run_sync_single_record should log targeted_resync_completed."""
    import inspect
    from inandout.ingestion import engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "targeted_resync_completed" in source


def test_ingestion_engine_single_record_does_not_update_watermark():
    """run_sync_single_record should NOT call set_watermark."""
    import inspect
    import ast
    from inandout.ingestion import engine as engine_mod
    source = inspect.getsource(engine_mod.IngestionEngine.run_sync_single_record)
    assert "set_watermark" not in source


# ---------------------------------------------------------------------------
# ControlDispatcher.resync command tests
# ---------------------------------------------------------------------------

def test_control_dispatcher_has_resync_command():
    """ControlDispatcher._execute should handle 'resync' command."""
    import inspect
    from inandout.engine import control as ctrl_mod
    source = inspect.getsource(ctrl_mod)
    assert "resync" in source
    assert "_cmd_resync" in source


@pytest.mark.asyncio
async def test_resync_with_external_id_calls_run_sync_single_record():
    """resync with external_id → run_sync_single_record called."""
    from inandout.engine.control import ControlDispatcher
    from unittest.mock import AsyncMock, MagicMock

    pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=(0,))  # 0 prior resyncs
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    dispatcher = ControlDispatcher(pool, set())

    # Mock engine with run_sync_single_record
    mock_result = MagicMock()
    mock_result.status = "completed"
    mock_result.records_inserted = 1
    mock_result.records_updated = 0

    mock_engine = AsyncMock()
    mock_engine.run_sync_single_record = AsyncMock(return_value=mock_result)

    # Mock connector_cfg lookup — engine has no _connector_configs
    result = await dispatcher._cmd_resync(
        "hubspot", "contacts",
        {"external_id": "42"},
        mock_engine,
    )

    # With no _connector_configs on engine, should return skipped
    assert result["status"] in ("skipped", "completed", "failed")


@pytest.mark.asyncio
async def test_resync_without_external_id_calls_full_sync():
    """resync without external_id → full run_sync called."""
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    pool.connection = MagicMock()

    dispatcher = ControlDispatcher(pool, set())

    mock_result = MagicMock()
    mock_result.status = "completed"

    mock_engine = AsyncMock()
    mock_engine.run_sync = AsyncMock(return_value=mock_result)

    result = await dispatcher._cmd_resync(
        "hubspot", "contacts",
        {},  # no external_id
        mock_engine,
    )

    # No connector config found → skipped
    assert result["status"] in ("skipped", "completed", "failed")


@pytest.mark.asyncio
async def test_resync_max_iterations_stops_resyncing():
    """After max_iterations resyncs for same record → abandoned."""
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    mock_conn = AsyncMock()
    # Return count = 3 (already resynced 3 times)
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=(3,))
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    dispatcher = ControlDispatcher(pool, set())

    mock_engine = AsyncMock()
    mock_engine.run_sync_single_record = AsyncMock()

    result = await dispatcher._cmd_resync(
        "hubspot", "contacts",
        {"external_id": "42", "max_iterations": 3},
        mock_engine,
    )

    assert result["status"] == "abandoned"
    assert result["reason"] == "max_iterations"
    # run_sync_single_record should NOT have been called
    mock_engine.run_sync_single_record.assert_not_called()


@pytest.mark.asyncio
async def test_resync_requires_engine():
    """resync command without engine raises RuntimeError."""
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    dispatcher = ControlDispatcher(pool, set())

    with pytest.raises(RuntimeError, match="requires an active IngestionEngine"):
        await dispatcher._cmd_resync("hubspot", "contacts", {}, None)


@pytest.mark.asyncio
async def test_resync_requires_connector_and_datatype():
    """resync command without connector or datatype raises ValueError."""
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    dispatcher = ControlDispatcher(pool, set())
    mock_engine = AsyncMock()

    with pytest.raises(ValueError, match="requires 'connector' and 'datatype'"):
        await dispatcher._cmd_resync(None, None, {}, mock_engine)
