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


@pytest.mark.anyio
async def test_streaming_loop_exits_on_draining_flag():
    """Loop exits without calling engine when _draining is True before cycle."""
    import inandout.writeback.daemon as daemon_mod
    from inandout.writeback.daemon import _writeback_loop_streaming
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    notifs_yielded = []

    async def _mock_listen(pool, channel="inandout_delta"):
        for payload in ["hubspot:contacts", "hubspot:contacts"]:
            notifs_yielded.append(payload)
            yield payload

    mock_engine = MagicMock(spec=WritebackEngine)
    mock_engine.run_writeback_cycle = AsyncMock()

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_pool = MagicMock()
    mock_writeback_cfg = MagicMock()

    # Enable draining before entering the loop
    original_draining = daemon_mod._draining
    daemon_mod._draining = True
    try:
        with patch("inandout.writeback.notify.listen_for_deltas", _mock_listen):
            await _writeback_loop_streaming(
                mock_engine, mock_pool, mock_connector,
                "contacts", mock_writeback_cfg, "_delta_hubspot_contacts",
            )
    finally:
        daemon_mod._draining = original_draining

    # Draining flag set means loop body should break before calling engine
    mock_engine.run_writeback_cycle.assert_not_called()


@pytest.mark.anyio
async def test_streaming_loop_engine_exception_is_caught():
    """Engine exception during a notify cycle is caught; loop continues."""
    from inandout.writeback.daemon import _writeback_loop_streaming
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    calls: list[str] = []

    async def _mock_listen(pool, channel="inandout_delta"):
        yield "hubspot:contacts"   # first → engine raises
        yield "hubspot:contacts"   # second → engine succeeds

    mock_engine = MagicMock(spec=WritebackEngine)
    good_result = WritebackResult(
        connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts"
    )

    cycle_count = [0]

    async def fake_cycle(*args, **kwargs):
        cycle_count[0] += 1
        if cycle_count[0] == 1:
            raise RuntimeError("transient write error")
        calls.append("ok")
        return good_result

    mock_engine.run_writeback_cycle = fake_cycle

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_pool = MagicMock()
    mock_writeback_cfg = MagicMock()

    with patch("inandout.writeback.notify.listen_for_deltas", _mock_listen):
        await _writeback_loop_streaming(
            mock_engine, mock_pool, mock_connector,
            "contacts", mock_writeback_cfg, "_delta_hubspot_contacts",
        )

    # Loop should have processed both notifications; second one succeeded
    assert cycle_count[0] == 2
    assert "ok" in calls


@pytest.mark.anyio
async def test_streaming_loop_handles_empty_payload():
    """An empty payload triggers a cycle (no connector:datatype filter is applied)."""
    from inandout.writeback.daemon import _writeback_loop_streaming
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    async def _mock_listen(pool, channel="inandout_delta"):
        yield ""  # empty payload

    mock_result = WritebackResult(
        connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts"
    )
    mock_engine = MagicMock(spec=WritebackEngine)
    mock_engine.run_writeback_cycle = AsyncMock(return_value=mock_result)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_pool = MagicMock()
    mock_writeback_cfg = MagicMock()

    with patch("inandout.writeback.notify.listen_for_deltas", _mock_listen):
        await _writeback_loop_streaming(
            mock_engine, mock_pool, mock_connector,
            "contacts", mock_writeback_cfg, "_delta_hubspot_contacts",
        )

    # Empty payload → no connector:datatype filtering → engine called
    mock_engine.run_writeback_cycle.assert_called_once()


@pytest.mark.anyio
async def test_listen_for_deltas_sleeps_on_connection_failure():
    """After a connection failure, anyio.sleep is called with the base delay.

    Driving the generator one step only (single __anext__) so it pauses at
    the first yield and never enters the infinite reconnect loop again.
    """
    from inandout.writeback.notify import listen_for_deltas

    sleep_delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleep_delays.append(d)

    class SucceedingNotif:
        """Yields one payload then stops."""
        def __init__(self) -> None:
            self._done = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._done:
                raise StopAsyncIteration
            self._done = True
            notif = MagicMock()
            notif.payload = "sf:contacts"
            return notif

    class FailingConn:
        async def set_autocommit(self, _) -> None:
            pass

        async def execute(self, sql: str) -> None:
            # Fail on the LISTEN command
            raise RuntimeError("connection refused")

        def notifies(self):
            return SucceedingNotif()

    class SucceedingConn:
        async def set_autocommit(self, _) -> None:
            pass

        async def execute(self, sql: str) -> None:
            pass

        def notifies(self):
            return SucceedingNotif()

    attempt = [0]

    class FakePoolCtx:
        async def __aenter__(self):
            attempt[0] += 1
            if attempt[0] == 1:
                return FailingConn()
            return SucceedingConn()

        async def __aexit__(self, *_):
            pass

    class FakePool:
        def connection(self):
            return FakePoolCtx()

    # Only drive one payload out of the generator — it will pause at yield.
    # Path: fail conn 1 → sleep(0.5) → succeed conn 2 → yield "sf:contacts" → pause
    with patch("anyio.sleep", new=fake_sleep):
        gen = listen_for_deltas(
            FakePool(), reconnect_delay_secs=0.5, reconnect_max_secs=60.0
        )
        payload = await gen.__anext__()

    assert payload == "sf:contacts"
    assert sleep_delays == [0.5]  # reconnect_delay_secs was used
    assert attempt[0] == 2  # one failed attempt + one successful attempt
