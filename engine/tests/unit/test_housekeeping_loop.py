"""Unit tests for _housekeeping_loop interval and call behaviour.

Covers:
- run_housekeeping is called once per tick.
- Loop sleeps interval_secs between ticks.
- Loop exits cleanly when _draining is set (pre-sleep and post-sleep checks).
- Exceptions from run_housekeeping are swallowed (loop continues).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod
from inandout.ingestion.daemon import _housekeeping_loop


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config() -> MagicMock:
    cfg = MagicMock()
    cfg.housekeeping = MagicMock()
    cfg.housekeeping.retention = MagicMock()
    return cfg


def _make_pool() -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=AsyncMock())
    return pool


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_housekeeping_loop_calls_run_housekeeping_per_tick():
    """run_housekeeping must be called once per tick (sleep → check → housekeeping)."""
    ticks = 0
    original_draining = _daemon_mod._draining
    hk_calls: list[int] = []

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    async def _fake_housekeeping(pool, cfg, pairs):
        hk_calls.append(1)
        return {}

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.ingestion.daemon.run_housekeeping",
                side_effect=_fake_housekeeping,
            ),
        ):
            await _housekeeping_loop(
                _make_pool(), _make_config(), [], interval_secs=300.0
            )
    finally:
        _daemon_mod._draining = original_draining

    assert len(hk_calls) >= 1


@pytest.mark.anyio
async def test_housekeeping_loop_sleeps_interval():
    """Loop must sleep interval_secs between ticks."""
    ticks = 0
    original_draining = _daemon_mod._draining
    sleep_calls: list[float] = []

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        sleep_calls.append(secs)
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.ingestion.daemon.run_housekeeping",
                new=AsyncMock(return_value={}),
            ),
        ):
            await _housekeeping_loop(
                _make_pool(), _make_config(), [], interval_secs=600.0
            )
    finally:
        _daemon_mod._draining = original_draining

    assert sleep_calls
    assert all(s == 600.0 for s in sleep_calls), (
        f"All sleep calls must use 600.0s, got: {sleep_calls}"
    )


@pytest.mark.anyio
async def test_housekeeping_loop_exits_when_draining_preset():
    """If _draining is True before first sleep, loop exits immediately."""
    original_draining = _daemon_mod._draining
    hk_calls: list[int] = []

    async def _fake_housekeeping(pool, cfg, pairs):
        hk_calls.append(1)
        return {}

    _daemon_mod._draining = True
    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", new=AsyncMock()),
            patch(
                "inandout.ingestion.daemon.run_housekeeping",
                side_effect=_fake_housekeeping,
            ),
        ):
            await _housekeeping_loop(
                _make_pool(), _make_config(), [], interval_secs=300.0
            )
    finally:
        _daemon_mod._draining = original_draining

    assert hk_calls == [], "run_housekeeping must not be called when draining preset"


@pytest.mark.anyio
async def test_housekeeping_loop_swallows_exception():
    """Exceptions from run_housekeeping must not propagate — loop continues."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    async def _failing_hk(pool, cfg, pairs):
        raise RuntimeError("pool unavailable")

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.ingestion.daemon.run_housekeeping",
                side_effect=_failing_hk,
            ),
        ):
            # Must not raise
            await _housekeeping_loop(
                _make_pool(), _make_config(), [], interval_secs=300.0
            )
    finally:
        _daemon_mod._draining = original_draining
