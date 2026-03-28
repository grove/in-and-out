"""Unit tests for _polling_loop pause-check behaviour.

When is_paused() returns True, the loop must:
- NOT call engine.run_sync.
- Call anyio.sleep(interval_secs) and then re-check (continue).
- Eventually drain when _draining is set.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schedule() -> MagicMock:
    s = MagicMock()
    s.cron = None
    s.interval = None
    return s


def _make_engine_pool() -> MagicMock:
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(
        return_value=AsyncMock(fetchone=AsyncMock(return_value=("row",)), rowcount=1)
    )
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_polling_loop_paused_skips_run_sync():
    """When paused, run_sync must never be called."""
    from inandout.ingestion.daemon import _polling_loop
    from inandout.ingestion.engine import IngestionEngine

    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 3:
            _daemon_mod._draining = True

    connector = MagicMock()
    connector.name = "hubspot"
    ingestion_cfg = MagicMock()
    ingestion_cfg.schedule = _make_schedule()

    engine = IngestionEngine(_make_engine_pool())
    run_sync_mock = AsyncMock()

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb,
            patch("inandout.ingestion.daemon.is_paused", return_value=True),
            patch.object(engine, "run_sync", run_sync_mock),
        ):
            mock_cb.return_value.allow_request.return_value = True
            mock_cb.return_value.state = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=60.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    run_sync_mock.assert_not_called()


@pytest.mark.anyio
async def test_polling_loop_paused_sleeps_interval():
    """When paused, anyio.sleep must be called with the configured interval."""
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
    ingestion_cfg.schedule = _make_schedule()

    engine = IngestionEngine(_make_engine_pool())

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_cb,
            patch("inandout.ingestion.daemon.is_paused", return_value=True),
            patch.object(engine, "run_sync", new=AsyncMock()),
        ):
            mock_cb.return_value.allow_request.return_value = True
            mock_cb.return_value.state = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=90.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    assert all(s == 90.0 for s in sleep_calls), (
        f"All sleep calls must use interval 90.0, got: {sleep_calls}"
    )
