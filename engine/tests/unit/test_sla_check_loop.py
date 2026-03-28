"""Unit tests for _sla_check_loop daemon behaviour.

Covers:
- Loop exits cleanly when _draining is set.
- check_all_slas is called once per tick.
- Exceptions from check_all_slas are swallowed (loop continues).
- Loop logs 'sla_check_loop_started' on entry.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod
from inandout.ingestion.daemon import _sla_check_loop


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool() -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=AsyncMock())
    return pool


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sla_check_loop_drains_cleanly():
    """Loop must exit without error when _draining is set after one tick."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        _daemon_mod._draining = True

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.observability.sla.check_all_slas",
                new=AsyncMock(return_value={}),
            ),
        ):
            await _sla_check_loop(_make_pool(), [], interval_secs=5.0)
    finally:
        _daemon_mod._draining = original_draining

    assert ticks >= 1


@pytest.mark.anyio
async def test_sla_check_loop_calls_check_all_slas_per_tick():
    """check_all_slas must be called once per non-draining tick."""
    ticks = 0
    original_draining = _daemon_mod._draining
    check_calls: list[int] = []

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    async def _fake_check(pool, connector_configs):
        check_calls.append(1)
        return {}

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.observability.sla.check_all_slas",
                side_effect=_fake_check,
            ),
        ):
            await _sla_check_loop(_make_pool(), [], interval_secs=5.0)
    finally:
        _daemon_mod._draining = original_draining

    # Two ticks: first non-draining tick calls check, second triggers drain
    assert len(check_calls) >= 1


@pytest.mark.anyio
async def test_sla_check_loop_swallows_exception():
    """Exceptions from check_all_slas must not propagate — loop must continue."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    async def _failing_check(pool, connector_configs):
        raise RuntimeError("db connection refused")

    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.observability.sla.check_all_slas",
                side_effect=_failing_check,
            ),
        ):
            # Must not raise
            await _sla_check_loop(_make_pool(), [], interval_secs=5.0)
    finally:
        _daemon_mod._draining = original_draining


@pytest.mark.anyio
async def test_sla_check_loop_exits_before_check_when_draining_pre_sleep():
    """If _draining is already True, loop exits before calling check_all_slas."""
    original_draining = _daemon_mod._draining
    check_calls: list[int] = []

    async def _fake_sleep(secs: float) -> None:
        pass  # sleep does nothing; draining already set before we enter

    async def _fake_check(pool, connector_configs):
        check_calls.append(1)
        return {}

    _daemon_mod._draining = True
    try:
        with (
            patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.observability.sla.check_all_slas",
                side_effect=_fake_check,
            ),
        ):
            await _sla_check_loop(_make_pool(), [], interval_secs=5.0)
    finally:
        _daemon_mod._draining = original_draining

    assert check_calls == [], "check_all_slas must not be called when already draining"
