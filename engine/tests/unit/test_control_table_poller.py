"""Unit tests for _control_table_poller in daemon.py.

Covers:
- dispatch_pending is called on every iteration before draining.
- The loop exits cleanly on the post-sleep drain check.
- An exception from dispatch_pending is swallowed (not re-raised).
- The loop exits immediately on the pre-loop drain check (draining=True on entry).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dispatcher(dispatch_side_effect=None) -> MagicMock:
    d = MagicMock()
    if dispatch_side_effect is not None:
        d.dispatch_pending = AsyncMock(side_effect=dispatch_side_effect)
    else:
        d.dispatch_pending = AsyncMock(return_value=0)
    return d


def _make_engine() -> MagicMock:
    e = MagicMock()
    return e


async def _run_poller(
    dispatcher: MagicMock,
    engine: MagicMock,
    n_ticks: int,
) -> int:
    """Run _control_table_poller for *n_ticks* sleeps, then drain. Returns dispatch call count."""
    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= n_ticks:
            _daemon_mod._draining = True

    try:
        with patch("anyio.sleep", side_effect=_fake_sleep):
            await _daemon_mod._control_table_poller(dispatcher, engine, poll_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    return dispatcher.dispatch_pending.call_count


# ---------------------------------------------------------------------------
# dispatch_pending is called each iteration
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_poller_calls_dispatch_pending_every_tick():
    """dispatch_pending should be called once per iteration."""
    dispatcher = _make_dispatcher()
    count = await _run_poller(dispatcher, _make_engine(), n_ticks=3)
    assert count == 3


# ---------------------------------------------------------------------------
# Exception is swallowed
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_poller_swallows_dispatch_exception():
    """An exception from dispatch_pending must not propagate out of _control_table_poller."""
    dispatcher = _make_dispatcher(dispatch_side_effect=RuntimeError("db error"))
    # Should complete without raising
    count = await _run_poller(dispatcher, _make_engine(), n_ticks=2)
    assert count == 2, "dispatch_pending should be called despite raising"


# ---------------------------------------------------------------------------
# Drains on entry (pre-loop check)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_poller_exits_immediately_when_draining_on_entry():
    """If _draining is True on entry, the loop must not call dispatch_pending at all."""
    dispatcher = _make_dispatcher()
    orig = _daemon_mod._draining
    _daemon_mod._draining = True
    try:
        with patch("anyio.sleep", new_callable=AsyncMock):
            await _daemon_mod._control_table_poller(dispatcher, _make_engine(), poll_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    dispatcher.dispatch_pending.assert_not_called()


# ---------------------------------------------------------------------------
# Post-sleep drain check exits without a second dispatch
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_poller_exits_after_post_sleep_drain_without_extra_dispatch():
    """
    When drain is set during sleep (post-sleep check), the loop exits without
    calling dispatch_pending a second time in the same iteration.
    """
    dispatcher = _make_dispatcher()
    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    dispatched = []

    async def _tracking_dispatch(engine):
        dispatched.append(True)
        return 0

    dispatcher.dispatch_pending = AsyncMock(side_effect=_tracking_dispatch)

    async def _fake_sleep_set_drain(secs: float) -> None:
        _daemon_mod._draining = True  # drain during the first sleep

    try:
        with patch("anyio.sleep", side_effect=_fake_sleep_set_drain):
            await _daemon_mod._control_table_poller(dispatcher, _make_engine(), poll_secs=0.0)
    finally:
        _daemon_mod._draining = orig

    # dispatch_pending was called once before the sleep; loop exited via post-sleep check
    assert len(dispatched) == 1
