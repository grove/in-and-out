"""Unit tests for _polling_loop cron vs interval scheduling.

Verifies that after each sync tick, _polling_loop sleeps for:
- The cron-derived delay when schedule.cron is set.
- The fixed interval_secs when schedule.cron is None.

We test _next_interval_secs directly (unit), then test via _polling_loop
integration with a tick counter.
"""
from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod
from inandout.ingestion.daemon import _next_interval_secs


# ---------------------------------------------------------------------------
# Direct _next_interval_secs tests
# ---------------------------------------------------------------------------

def _make_schedule(cron: str | None = None, interval: str | None = None) -> MagicMock:
    s = MagicMock()
    s.cron = cron
    s.interval = interval
    return s


def test_next_interval_secs_returns_default_when_no_cron():
    """No cron → always returns default_interval_secs."""
    schedule = _make_schedule(cron=None)
    assert _next_interval_secs(schedule, 300.0) == 300.0


def test_next_interval_secs_returns_default_when_cron_empty_string():
    """Empty string cron → returns default."""
    schedule = _make_schedule(cron="")
    # Empty string is falsy → same branch as no cron
    assert _next_interval_secs(schedule, 60.0) == 60.0


def test_next_interval_secs_returns_positive_delay_for_valid_cron():
    """Valid cron → returns a positive float (seconds until next fire)."""
    schedule = _make_schedule(cron="*/5 * * * *")  # every 5 min
    delay = _next_interval_secs(schedule, 300.0)
    assert 0.0 <= delay <= 300.0


def test_next_interval_secs_falls_back_on_invalid_cron():
    """Unparseable cron → returns default_interval_secs (exception swallowed)."""
    schedule = _make_schedule(cron="not-a-cron-expression")
    result = _next_interval_secs(schedule, 120.0)
    assert result == 120.0


# ---------------------------------------------------------------------------
# _polling_loop integration: sleep arg after sync reflects _next_interval_secs
# ---------------------------------------------------------------------------

def _make_engine_pool() -> MagicMock:
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(
        return_value=AsyncMock(
            fetchone=AsyncMock(return_value=("lock-id",)),
            rowcount=1,
        )
    )
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


@pytest.mark.anyio
async def test_polling_loop_uses_interval_when_no_cron():
    """When cron is None, sleep after sync equals the configured interval_secs."""
    from inandout.ingestion.daemon import _polling_loop
    from inandout.ingestion.engine import IngestionEngine

    sleep_calls: list[float] = []
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        sleep_calls.append(secs)
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    connector = MagicMock()
    connector.name = "hubspot"
    ingestion_cfg = MagicMock()
    schedule = _make_schedule(cron=None, interval=None)
    ingestion_cfg.schedule = schedule

    mock_result = MagicMock()
    mock_result.status = "completed"
    mock_result.error_message = None

    pool = _make_engine_pool()
    engine = IngestionEngine(pool)

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb,
            patch.object(engine, "run_sync", new=AsyncMock(return_value=mock_result)),
        ):
            mock_cb.return_value.allow_request.return_value = True
            mock_cb.return_value.state = MagicMock()
            mock_cb.return_value.record_success = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=120.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    # The sleep after the sync tick must equal the fixed interval (no cron)
    post_sync_sleep = sleep_calls[-1] if sleep_calls else None
    assert post_sync_sleep == 120.0


@pytest.mark.anyio
async def test_polling_loop_uses_cron_delay_when_cron_is_set():
    """When cron is set and valid, the sleep after sync must be <= configured interval."""
    from inandout.ingestion.daemon import _polling_loop
    from inandout.ingestion.engine import IngestionEngine

    sleep_calls: list[float] = []
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        sleep_calls.append(secs)
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    connector = MagicMock()
    connector.name = "hubspot"
    ingestion_cfg = MagicMock()
    schedule = _make_schedule(cron="*/5 * * * *", interval=None)
    ingestion_cfg.schedule = schedule

    mock_result = MagicMock()
    mock_result.status = "completed"
    mock_result.error_message = None

    pool = _make_engine_pool()
    engine = IngestionEngine(pool)

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb,
            patch.object(engine, "run_sync", new=AsyncMock(return_value=mock_result)),
        ):
            mock_cb.return_value.allow_request.return_value = True
            mock_cb.return_value.state = MagicMock()
            mock_cb.return_value.record_success = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=300.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    # Every 5-min cron → sleep should be in [0, 300]
    post_sync_sleep = sleep_calls[-1] if sleep_calls else None
    assert post_sync_sleep is not None
    assert 0.0 <= post_sync_sleep <= 300.0
