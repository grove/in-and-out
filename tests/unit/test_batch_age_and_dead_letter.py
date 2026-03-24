"""Unit tests for T2 #33 batch_max_age_secs enforcement and T2 #24 dead-letter replay."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call


# ===========================================================================
# T2 #33 — batch_max_age_secs: effective sleep is min(interval, age_secs)
# ===========================================================================

def _make_writeback_cfg(batch_max_age_secs=None, **kwargs):
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig,
        ProtectionLevel, UpdateOperationConfig, WritebackConfig,
    )
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/x/${external_id}"),
        insert=OperationConfig(method="POST", path="/x"),
        update=UpdateOperationConfig(method="PATCH", path="/x/${external_id}"),
    )
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update"],
        operations=ops,
        batch_max_age_secs=batch_max_age_secs,
        **kwargs,
    )


def test_batch_max_age_secs_default_is_none():
    cfg = _make_writeback_cfg()
    assert cfg.batch_max_age_secs is None


def test_batch_max_age_secs_set():
    cfg = _make_writeback_cfg(batch_max_age_secs=2.5)
    assert cfg.batch_max_age_secs == 2.5


@pytest.mark.asyncio
async def test_polling_loop_sleep_clamped_by_batch_max_age_secs():
    """When batch_max_age_secs < interval, sleep is clamped to batch_max_age_secs."""
    from inandout.writeback import daemon as daemon_mod
    from inandout.writeback.engine import WritebackResult

    mock_engine = MagicMock()
    mock_result = WritebackResult(connector="c", datatype="d", delta_table="t")
    mock_engine.run_writeback_cycle = AsyncMock(return_value=mock_result)

    mock_connector = MagicMock()
    mock_connector.name = "c"
    mock_connector.circuit_breaker = None

    writeback_cfg = _make_writeback_cfg(batch_max_age_secs=1.0)

    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        # After recording the first sleep, set draining to break the loop
        daemon_mod._draining = True

    original_draining = daemon_mod._draining
    daemon_mod._draining = False
    try:
        with patch("inandout.writeback.daemon.anyio") as mock_anyio, \
             patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb_factory:
            mock_anyio.sleep = _fake_sleep
            mock_cb = MagicMock()
            mock_cb.allow_request.return_value = True
            mock_cb.state.value = "closed"
            mock_cb_factory.return_value = mock_cb

            from inandout.writeback.daemon import _writeback_polling_loop
            await _writeback_polling_loop(
                mock_engine,
                mock_connector,
                "d",
                writeback_cfg,
                "_delta_c_d",
                interval_secs=60.0,
            )
    finally:
        daemon_mod._draining = original_draining

    assert sleep_calls, "sleep should have been called"
    # Effective sleep must be min(60.0, 1.0) = 1.0
    assert sleep_calls[0] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_polling_loop_sleep_unchanged_when_age_exceeds_interval():
    """When batch_max_age_secs > interval, sleep stays at interval."""
    from inandout.writeback import daemon as daemon_mod
    from inandout.writeback.engine import WritebackResult

    mock_engine = MagicMock()
    mock_result = WritebackResult(connector="c", datatype="d", delta_table="t")
    mock_engine.run_writeback_cycle = AsyncMock(return_value=mock_result)

    mock_connector = MagicMock()
    mock_connector.name = "c"
    mock_connector.circuit_breaker = None

    writeback_cfg = _make_writeback_cfg(batch_max_age_secs=120.0)  # > interval

    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        daemon_mod._draining = True

    original_draining = daemon_mod._draining
    daemon_mod._draining = False
    try:
        with patch("inandout.writeback.daemon.anyio") as mock_anyio, \
             patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb_factory:
            mock_anyio.sleep = _fake_sleep
            mock_cb = MagicMock()
            mock_cb.state.value = "closed"
            mock_cb_factory.return_value = mock_cb

            from inandout.writeback.daemon import _writeback_polling_loop
            await _writeback_polling_loop(
                mock_engine,
                mock_connector,
                "d",
                writeback_cfg,
                "_delta_c_d",
                interval_secs=5.0,
            )
    finally:
        daemon_mod._draining = original_draining

    assert sleep_calls[0] == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_polling_loop_sleep_unchanged_when_no_age_limit():
    """Without batch_max_age_secs, sleep equals interval_secs."""
    from inandout.writeback import daemon as daemon_mod
    from inandout.writeback.engine import WritebackResult

    mock_engine = MagicMock()
    mock_result = WritebackResult(connector="c", datatype="d", delta_table="t")
    mock_engine.run_writeback_cycle = AsyncMock(return_value=mock_result)

    mock_connector = MagicMock()
    mock_connector.name = "c"
    mock_connector.circuit_breaker = None

    writeback_cfg = _make_writeback_cfg(batch_max_age_secs=None)

    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        daemon_mod._draining = True

    original_draining = daemon_mod._draining
    daemon_mod._draining = False
    try:
        with patch("inandout.writeback.daemon.anyio") as mock_anyio, \
             patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb_factory:
            mock_anyio.sleep = _fake_sleep
            mock_cb = MagicMock()
            mock_cb.state.value = "closed"
            mock_cb_factory.return_value = mock_cb

            from inandout.writeback.daemon import _writeback_polling_loop
            await _writeback_polling_loop(
                mock_engine,
                mock_connector,
                "d",
                writeback_cfg,
                "_delta_c_d",
                interval_secs=30.0,
            )
    finally:
        daemon_mod._draining = original_draining

    assert sleep_calls[0] == pytest.approx(30.0)


# ===========================================================================
# T2 #24 — WritebackConfig.max_retry_count
# ===========================================================================

def test_max_retry_count_default():
    cfg = _make_writeback_cfg()
    assert cfg.max_retry_count == 3


def test_max_retry_count_set():
    cfg = _make_writeback_cfg(max_retry_count=5)
    assert cfg.max_retry_count == 5


def test_max_retry_count_zero_disables_auto_dead_letter():
    cfg = _make_writeback_cfg(max_retry_count=0)
    assert cfg.max_retry_count == 0


# ===========================================================================
# T2 #24 — failure_count_for_row
# ===========================================================================

@pytest.mark.asyncio
async def test_failure_count_for_row_returns_zero_when_no_failures():
    from inandout.deadletter.writeback import failure_count_for_row

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone = AsyncMock(return_value=(0,))
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_pool.connection = MagicMock()
    mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    count = await failure_count_for_row(mock_pool, "c", "d", "_delta_c_d", "ext-001")
    assert count == 0


@pytest.mark.asyncio
async def test_failure_count_for_row_returns_count():
    from inandout.deadletter.writeback import failure_count_for_row

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone = AsyncMock(return_value=(4,))
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_pool.connection = MagicMock()
    mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    count = await failure_count_for_row(mock_pool, "c", "d", "_delta_c_d", "ext-001")
    assert count == 4


@pytest.mark.asyncio
async def test_failure_count_returns_zero_on_db_error():
    from inandout.deadletter.writeback import failure_count_for_row

    mock_pool = MagicMock()
    mock_pool.connection.side_effect = RuntimeError("connection refused")

    count = await failure_count_for_row(mock_pool, "c", "d", "_delta_c_d", "ext-001")
    assert count == 0


# ===========================================================================
# T2 #24 — _auto_dead_letter_exceeded_rows (engine integration)
# ===========================================================================

@pytest.mark.asyncio
async def test_auto_dead_letter_skips_when_max_retry_zero():
    """max_retry_count=0 means never auto-dead-letter."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = MagicMock()

    writeback_cfg = _make_writeback_cfg(max_retry_count=0)
    result = WritebackResult(connector="c", datatype="d", delta_table="_delta_c_d")
    result._failed_entries.append(("ext-001", "insert", "error"))

    with patch("inandout.deadletter.writeback.failure_count_for_row") as mock_count:
        await engine._auto_dead_letter_exceeded_rows(result, writeback_cfg)
        mock_count.assert_not_called()


@pytest.mark.asyncio
async def test_auto_dead_letter_moves_row_when_count_exceeded():
    """When failure count >= max_retry_count, row is moved to dead-letter."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = MagicMock()

    writeback_cfg = _make_writeback_cfg(max_retry_count=3)
    result = WritebackResult(connector="c", datatype="d", delta_table="_delta_c_d")
    result._failed_entries.append(("ext-002", "update", "timeout"))

    with patch("inandout.deadletter.writeback.failure_count_for_row", return_value=3) as mock_count, \
         patch("inandout.deadletter.writeback.move_to_dead_letter") as mock_move:
        mock_move.return_value = None
        await engine._auto_dead_letter_exceeded_rows(result, writeback_cfg)
        mock_count.assert_called_once()
        mock_move.assert_called_once()
        # Verify called with correct connector/datatype/external_id
        call_args = mock_move.call_args
        # move_to_dead_letter(pool, connector, datatype, external_id=..., action=..., ...)
        assert call_args[0][1] == "c"  # connector (positional)
        assert call_args[0][2] == "d"  # datatype (positional)


@pytest.mark.asyncio
async def test_auto_dead_letter_does_not_move_row_below_threshold():
    """When failure count < max_retry_count, row is NOT moved to dead-letter."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = MagicMock()

    writeback_cfg = _make_writeback_cfg(max_retry_count=5)
    result = WritebackResult(connector="c", datatype="d", delta_table="_delta_c_d")
    result._failed_entries.append(("ext-003", "insert", "server error"))

    with patch("inandout.deadletter.writeback.failure_count_for_row", return_value=2) as mock_count, \
         patch("inandout.deadletter.writeback.move_to_dead_letter") as mock_move:
        await engine._auto_dead_letter_exceeded_rows(result, writeback_cfg)
        mock_count.assert_called_once()
        mock_move.assert_not_called()
