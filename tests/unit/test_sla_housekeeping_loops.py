"""Unit tests for _sla_check_loop and _housekeeping_loop draining in daemon.py.

Covers:
- _sla_check_loop calls check_all_slas after sleeping, then drains cleanly.
- _sla_check_loop drains before executing check_all_slas when draining on entry.
- _housekeeping_loop calls run_housekeeping after sleeping.
- _housekeeping_loop exits on the pre-sleep drain check (first exit path).
- _housekeeping_loop exits on the post-sleep drain check (second exit path).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod


# ---------------------------------------------------------------------------
# _sla_check_loop
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sla_check_loop_calls_check_all_slas():
    """_sla_check_loop should call check_all_slas once per iteration before draining."""
    called_with: list = []
    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 2:
            _daemon_mod._draining = True

    async def _fake_check(pool, connectors):
        called_with.append(True)
        return {}

    pool = MagicMock()
    connectors = [MagicMock()]

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.observability.sla.check_all_slas", side_effect=_fake_check),
        ):
            await _daemon_mod._sla_check_loop(pool, connectors, interval_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    assert called_with, "check_all_slas should have been called at least once"


@pytest.mark.anyio
async def test_sla_check_loop_drains_on_entry_without_calling_check():
    """If _draining is True on entry, _sla_check_loop should exit before calling check_all_slas."""
    called = []
    orig = _daemon_mod._draining
    _daemon_mod._draining = True

    async def _fake_check(pool, connectors):
        called.append(True)
        return {}

    try:
        with (
            patch("anyio.sleep", new_callable=AsyncMock),
            patch("inandout.observability.sla.check_all_slas", side_effect=_fake_check),
        ):
            await _daemon_mod._sla_check_loop(MagicMock(), [], interval_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    assert not called, "check_all_slas must not be called when draining on entry"


# ---------------------------------------------------------------------------
# _housekeeping_loop
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_housekeeping_loop_calls_run_housekeeping():
    """_housekeeping_loop should call run_housekeeping after sleeping."""
    called = []
    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 2:
            _daemon_mod._draining = True

    async def _fake_housekeeping(pool, cfg, pairs):
        called.append(True)

    pool = MagicMock()
    config = MagicMock()
    config.housekeeping = MagicMock()

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.ingestion.daemon.run_housekeeping", side_effect=_fake_housekeeping),
        ):
            await _daemon_mod._housekeeping_loop(pool, config, [("c", "dt")], interval_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    assert called, "run_housekeeping should have been called"


@pytest.mark.anyio
async def test_housekeeping_loop_exits_on_pre_sleep_drain():
    """_housekeeping_loop exits immediately on the pre-sleep drain check."""
    called = []
    orig = _daemon_mod._draining
    _daemon_mod._draining = True  # drain on entry

    async def _fake_housekeeping(*a):
        called.append(True)

    try:
        with (
            patch("anyio.sleep", new_callable=AsyncMock),
            patch("inandout.ingestion.daemon.run_housekeeping", side_effect=_fake_housekeeping),
        ):
            await _daemon_mod._housekeeping_loop(MagicMock(), MagicMock(), [], interval_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    assert not called, "run_housekeeping must not be called when draining on entry"


@pytest.mark.anyio
async def test_housekeeping_loop_exits_on_post_sleep_drain():
    """_housekeeping_loop exits after sleep when _draining is set during sleep."""
    called = []
    orig = _daemon_mod._draining
    _daemon_mod._draining = False

    async def _fake_sleep(secs: float) -> None:
        # Set drain flag *during* the sleep — simulates SIGTERM arriving mid-sleep.
        _daemon_mod._draining = True

    async def _fake_housekeeping(*a):
        called.append(True)

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.ingestion.daemon.run_housekeeping", side_effect=_fake_housekeeping),
        ):
            await _daemon_mod._housekeeping_loop(MagicMock(), MagicMock(), [], interval_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    assert not called, "run_housekeeping must not be called when drain flag is set during sleep"
