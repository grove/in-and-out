"""Unit tests for _polling_loop circuit-open skip behaviour.

When cb.allow_request() returns False (circuit is open):
- engine.run_sync must NOT be called.
- anyio.sleep(interval_secs) must still be called.
- The loop proceeds to the next tick (does not exit).
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
async def test_circuit_open_skips_run_sync():
    """When circuit breaker is open, run_sync must not be called."""
    from inandout.ingestion.daemon import _polling_loop
    from inandout.ingestion.engine import IngestionEngine

    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
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
            patch("inandout.ingestion.daemon.is_paused", return_value=False),
            patch.object(engine, "run_sync", run_sync_mock),
        ):
            cb_instance = mock_cb.return_value
            cb_instance.allow_request.return_value = False  # circuit OPEN
            cb_instance.state = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=60.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    run_sync_mock.assert_not_called()


@pytest.mark.anyio
async def test_circuit_open_still_sleeps_interval():
    """When circuit breaker is open, anyio.sleep must be called with interval_secs."""
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
            patch("inandout.ingestion.daemon.is_paused", return_value=False),
            patch.object(engine, "run_sync", new=AsyncMock()),
        ):
            cb_instance = mock_cb.return_value
            cb_instance.allow_request.return_value = False  # circuit OPEN
            cb_instance.state = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=45.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    assert sleep_calls, "sleep must be called even when circuit is open"
    assert all(s == 45.0 for s in sleep_calls), (
        f"All sleep calls must use interval 45.0, got: {sleep_calls}"
    )


@pytest.mark.anyio
async def test_circuit_open_loop_continues_not_exits():
    """Circuit open must not break the loop — it must iterate until draining."""
    from inandout.ingestion.daemon import _polling_loop
    from inandout.ingestion.engine import IngestionEngine

    sleep_count = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 3:
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
            patch("inandout.ingestion.daemon.is_paused", return_value=False),
            patch.object(engine, "run_sync", new=AsyncMock()),
        ):
            cb_instance = mock_cb.return_value
            cb_instance.allow_request.return_value = False
            cb_instance.state = MagicMock()

            await _polling_loop(
                engine, connector, "contacts", ingestion_cfg,
                default_interval_secs=30.0,
                paused_connectors=set(),
            )
    finally:
        _daemon_mod._draining = original_draining

    # Must have slept 3 times (3 ticks before drain), not just 1
    assert sleep_count == 3
