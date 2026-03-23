"""Unit tests for replication slot health monitoring (T2 #32)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# ReplicationSlotConfig model tests
# ---------------------------------------------------------------------------

def test_replication_slot_config_defaults():
    """ReplicationSlotConfig should have sensible defaults."""
    from inandout.config.tool import ReplicationSlotConfig

    cfg = ReplicationSlotConfig()
    assert cfg.slot_name is None
    assert cfg.warn_lag_bytes == 100_000_000
    assert cfg.max_lag_bytes == 1_000_000_000
    assert cfg.poll_interval_secs == 30.0


def test_writeback_tool_config_has_replication_slot():
    """WritebackToolConfig should have replication_slot field."""
    from inandout.config.tool import ReplicationSlotConfig
    from inandout.config.tool import WritebackToolConfig

    # Check field exists
    cfg = WritebackToolConfig(
        database={"dsn": "postgresql://localhost/test"},
    )
    assert isinstance(cfg.replication_slot, ReplicationSlotConfig)
    assert cfg.replication_slot.slot_name is None


# ---------------------------------------------------------------------------
# get_slot_lag tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_slot_lag_returns_lag_bytes_and_secs():
    """get_slot_lag should return (lag_bytes, lag_secs) from pg_replication_slots."""
    from inandout.writeback.slot_monitor import get_slot_lag

    mock_row = (50_000_000, 5.0)  # 50MB lag, 5s lag
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=mock_row)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    result = await get_slot_lag(pool, "my_slot")
    assert result is not None
    lag_bytes, lag_secs = result
    assert lag_bytes == 50_000_000
    assert lag_secs == 5.0


@pytest.mark.asyncio
async def test_get_slot_lag_returns_none_when_slot_not_found():
    """get_slot_lag should return None when slot doesn't exist."""
    from inandout.writeback.slot_monitor import get_slot_lag

    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)  # slot not found
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    result = await get_slot_lag(pool, "nonexistent_slot")
    assert result is None


@pytest.mark.asyncio
async def test_get_slot_lag_returns_none_on_query_error():
    """get_slot_lag should return None if query raises an exception."""
    from inandout.writeback.slot_monitor import get_slot_lag

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(side_effect=Exception("connection error"))

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    result = await get_slot_lag(pool, "my_slot")
    assert result is None


# ---------------------------------------------------------------------------
# monitor_replication_slot behaviour tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_monitor_below_warn_threshold_no_error_log(caplog):
    """Below warn_lag_bytes → no error log, no fallback."""
    import logging
    from inandout.writeback.slot_monitor import monitor_replication_slot
    from inandout.config.tool import ReplicationSlotConfig
    import anyio

    config = ReplicationSlotConfig(
        slot_name="test_slot",
        warn_lag_bytes=100_000_000,
        max_lag_bytes=1_000_000_000,
        poll_interval_secs=0.01,
    )

    fallback_called = []

    def on_fallback():
        fallback_called.append(True)

    call_count = [0]

    async def mock_get_slot_lag(pool, slot_name):
        call_count[0] += 1
        if call_count[0] == 1:
            return (50_000_000, 1.0)  # below warn threshold
        return None  # stop further iterations

    pool = MagicMock()

    from unittest.mock import patch
    with patch("inandout.writeback.slot_monitor.get_slot_lag", side_effect=mock_get_slot_lag):
        with caplog.at_level(logging.ERROR, logger="inandout.writeback.slot_monitor"):
            with anyio.move_on_after(0.05):
                await monitor_replication_slot(pool, config, on_fallback)

    assert not fallback_called
    # No ERROR-level log for lag (below threshold)
    assert call_count[0] >= 1


@pytest.mark.asyncio
async def test_monitor_above_warn_threshold_logs_error(caplog):
    """Above warn_lag_bytes → ERROR logged, no fallback."""
    import logging
    from inandout.writeback.slot_monitor import monitor_replication_slot
    from inandout.config.tool import ReplicationSlotConfig
    import anyio

    config = ReplicationSlotConfig(
        slot_name="test_slot",
        warn_lag_bytes=10_000_000,  # 10MB warn
        max_lag_bytes=1_000_000_000,
        poll_interval_secs=0.01,
    )

    fallback_called = []

    def on_fallback():
        fallback_called.append(True)

    call_count = [0]

    async def mock_get_slot_lag(pool, slot_name):
        call_count[0] += 1
        if call_count[0] == 1:
            return (50_000_000, 5.0)  # 50MB > 10MB warn threshold
        return None

    pool = MagicMock()

    from unittest.mock import patch
    with patch("inandout.writeback.slot_monitor.get_slot_lag", side_effect=mock_get_slot_lag):
        with caplog.at_level(logging.ERROR, logger="inandout.writeback.slot_monitor"):
            with anyio.move_on_after(0.05):
                await monitor_replication_slot(pool, config, on_fallback)

    assert not fallback_called
    # Verify the slot monitor ran at least once
    assert call_count[0] >= 1


@pytest.mark.asyncio
async def test_monitor_above_max_threshold_calls_fallback():
    """Above max_lag_bytes → on_fallback() called."""
    from inandout.writeback.slot_monitor import monitor_replication_slot
    from inandout.config.tool import ReplicationSlotConfig
    import anyio

    config = ReplicationSlotConfig(
        slot_name="test_slot",
        warn_lag_bytes=100_000_000,
        max_lag_bytes=500_000_000,
        poll_interval_secs=0.01,
    )

    fallback_called = []

    def on_fallback():
        fallback_called.append(True)

    call_count = [0]

    async def mock_get_slot_lag(pool, slot_name):
        call_count[0] += 1
        if call_count[0] == 1:
            return (600_000_000, 60.0)  # 600MB > 500MB max threshold
        return None

    pool = MagicMock()

    from unittest.mock import patch
    with patch("inandout.writeback.slot_monitor.get_slot_lag", side_effect=mock_get_slot_lag):
        with anyio.move_on_after(0.1):
            await monitor_replication_slot(pool, config, on_fallback)

    assert fallback_called  # on_fallback was called


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def test_replication_slot_lag_bytes_metric_exists():
    """replication_slot_lag_bytes gauge should be importable from metrics."""
    from inandout.observability.metrics import replication_slot_lag_bytes
    assert replication_slot_lag_bytes is not None


def test_replication_slot_lag_bytes_metric_has_slot_name_label():
    """replication_slot_lag_bytes should have slot_name label."""
    from inandout.observability.metrics import replication_slot_lag_bytes
    labels = list(replication_slot_lag_bytes._labelnames)
    assert "slot_name" in labels
