"""Unit tests for Step 52 — Streaming writeback via LISTEN/NOTIFY."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# test_trigger_ddl_generated
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_trigger_ddl_generated():
    """create_delta_notify_trigger calls execute with CREATE TRIGGER SQL."""
    from inandout.writeback.notify import create_delta_notify_trigger

    mock_conn = AsyncMock()
    await create_delta_notify_trigger(mock_conn, "_delta_hubspot_contacts")

    assert mock_conn.execute.call_count == 2
    calls = mock_conn.execute.call_args_list
    # First call: CREATE OR REPLACE FUNCTION
    first_sql = calls[0][0][0]
    assert "CREATE OR REPLACE FUNCTION" in first_sql
    assert "inandout_notify" in first_sql
    assert "pg_notify" in first_sql
    assert "inandout_delta" in first_sql

    # Second call: CREATE TRIGGER (via DO $$...)
    second_sql = calls[1][0][0]
    assert "CREATE TRIGGER" in second_sql
    assert "AFTER INSERT" in second_sql


@pytest.mark.anyio
async def test_trigger_ddl_contains_connector_datatype_payload():
    """The trigger function sends connector:datatype as payload."""
    from inandout.writeback.notify import create_delta_notify_trigger

    mock_conn = AsyncMock()
    await create_delta_notify_trigger(mock_conn, "_delta_salesforce_accounts")

    func_sql = mock_conn.execute.call_args_list[0][0][0]
    assert "salesforce" in func_sql
    assert "accounts" in func_sql


# ---------------------------------------------------------------------------
# test_streaming_mode_triggers_on_notify
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_streaming_mode_triggers_on_notify():
    """Streaming writeback loop calls run_writeback_cycle on notification."""
    from inandout.writeback.daemon import _writeback_loop_streaming
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    # Mock listen_for_deltas to yield one notification then stop
    async def _mock_listen(pool, channel="inandout_delta"):
        yield "hubspot:contacts"

    mock_engine = MagicMock(spec=WritebackEngine)
    mock_result = WritebackResult(
        connector="hubspot",
        datatype="contacts",
        delta_table="_delta_hubspot_contacts",
    )
    mock_engine.run_writeback_cycle = AsyncMock(return_value=mock_result)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"

    mock_writeback_cfg = MagicMock()
    mock_writeback_cfg.streaming = True

    mock_pool = MagicMock()

    with patch("inandout.writeback.notify.listen_for_deltas", _mock_listen):
        await _writeback_loop_streaming(
            mock_engine,
            mock_pool,
            mock_connector,
            "contacts",
            mock_writeback_cfg,
            "_delta_hubspot_contacts",
        )

    mock_engine.run_writeback_cycle.assert_called_once()


@pytest.mark.anyio
async def test_streaming_mode_ignores_other_connectors():
    """Streaming loop ignores notifications for different connector/datatype."""
    from inandout.writeback.daemon import _writeback_loop_streaming
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    async def _mock_listen(pool, channel="inandout_delta"):
        # Notification for a different connector
        yield "salesforce:accounts"

    mock_engine = MagicMock(spec=WritebackEngine)
    mock_result = WritebackResult(
        connector="hubspot",
        datatype="contacts",
        delta_table="_delta_hubspot_contacts",
    )
    mock_engine.run_writeback_cycle = AsyncMock(return_value=mock_result)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_pool = MagicMock()
    mock_writeback_cfg = MagicMock()

    with patch("inandout.writeback.notify.listen_for_deltas", _mock_listen):
        await _writeback_loop_streaming(
            mock_engine,
            mock_pool,
            mock_connector,
            "contacts",
            mock_writeback_cfg,
            "_delta_hubspot_contacts",
        )

    # Should NOT have called run_writeback_cycle since notification was for salesforce
    mock_engine.run_writeback_cycle.assert_not_called()
