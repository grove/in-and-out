"""Unit tests for _polling_loop circuit-breaker interaction in daemon.py.

Covers:
- When cb.allow_request() returns False, engine.run_sync is never called but
  anyio.sleep is still awaited for the full interval.
- _mark_connector_unavailable is called once the circuit opens after N failures.
- A successful run calls cb.record_success() and _mark_connector_healthy.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schedule(interval: str = "60s") -> MagicMock:
    sched = MagicMock()
    sched.interval = interval
    sched.cron = None
    return sched


def _make_ingestion_cfg(interval: str = "60s") -> MagicMock:
    cfg = MagicMock()
    cfg.schedule = _make_schedule(interval)
    return cfg


def _make_connector_cfg(name: str = "hubspot") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    return cfg


def _make_result(status: str = "completed", error_message: str | None = None) -> MagicMock:
    r = MagicMock()
    r.status = status
    r.error_message = error_message
    return r


def _make_engine(pool: MagicMock | None = None) -> MagicMock:
    engine = MagicMock()
    engine._pool = pool or MagicMock()
    engine.run_sync = AsyncMock(return_value=_make_result("completed"))
    return engine


async def _run_loop_n_ticks(
    engine: MagicMock,
    connector_cfg: MagicMock,
    datatype: str,
    ingestion_cfg: MagicMock,
    cb: MagicMock,
    n_ticks: int,
) -> list[float]:
    """
    Drive _polling_loop through *n_ticks* iterations then set draining.
    Returns all values passed to anyio.sleep (excluding the initial interval-parse sleep).
    """
    tick = [0]
    slept: list[float] = []
    orig_draining = _daemon_mod._draining
    _daemon_mod._draining = False

    async def _fake_sleep(secs: float) -> None:
        slept.append(secs)
        tick[0] += 1
        if tick[0] >= n_ticks:
            _daemon_mod._draining = True

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch(
                "inandout.transport.circuit_breaker.get_circuit_breaker",
                return_value=cb,
            ),
            patch("inandout.ingestion.daemon._mark_connector_unavailable", new_callable=AsyncMock),
            patch("inandout.ingestion.daemon._mark_connector_healthy", new_callable=AsyncMock),
        ):
            await _daemon_mod._polling_loop(
                engine,
                connector_cfg,
                datatype,
                ingestion_cfg,
                60.0,
                set(),
            )
    finally:
        _daemon_mod._draining = orig_draining

    return slept


# ---------------------------------------------------------------------------
# Circuit open → run_sync skipped, sleep still called
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_circuit_open_skips_run_sync_but_sleeps():
    """When cb.allow_request() is False, run_sync must NOT be called but sleep IS."""
    engine = _make_engine()

    cb = MagicMock()
    cb.allow_request.return_value = False
    cb.state = "open"

    slept = await _run_loop_n_ticks(engine, _make_connector_cfg(), "contacts",
                                    _make_ingestion_cfg(), cb, n_ticks=1)

    engine.run_sync.assert_not_called()
    assert slept, "Expected anyio.sleep to be called even when circuit is open"


# ---------------------------------------------------------------------------
# Failure path → circuit opens → _mark_connector_unavailable called
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_failures_trip_circuit_and_call_mark_unavailable():
    """After N failures that trip the circuit open, _mark_connector_unavailable is called."""
    from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState

    connector_cfg = _make_connector_cfg("salesforce")
    engine = _make_engine()
    engine.run_sync = AsyncMock(return_value=_make_result("failed", "timeout"))

    # Use a real circuit breaker with threshold=1 so a single failure opens it.
    cb = CircuitBreaker("salesforce", "contacts", failure_threshold=1)

    orig_draining = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]
    marked_unavailable = []

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 2:
            _daemon_mod._draining = True

    async def _fake_mark_unavailable(pool, connector, datatype, reason=""):
        marked_unavailable.append((connector, datatype, reason))

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker", return_value=cb),
            patch("inandout.ingestion.daemon._mark_connector_unavailable",
                  side_effect=_fake_mark_unavailable),
            patch("inandout.ingestion.daemon._mark_connector_healthy", new_callable=AsyncMock),
        ):
            await _daemon_mod._polling_loop(
                engine, connector_cfg, "contacts", _make_ingestion_cfg(), 60.0, set()
            )
    finally:
        _daemon_mod._draining = orig_draining

    assert marked_unavailable, "_mark_connector_unavailable should have been called"
    assert marked_unavailable[0][0] == "salesforce"
    assert marked_unavailable[0][1] == "contacts"


# ---------------------------------------------------------------------------
# Success path → record_success + _mark_connector_healthy
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_successful_sync_records_success_and_marks_healthy():
    """A completed run should call cb.record_success() and _mark_connector_healthy."""
    from inandout.transport.circuit_breaker import CircuitBreaker

    connector_cfg = _make_connector_cfg("hubspot")
    engine = _make_engine()
    engine.run_sync = AsyncMock(return_value=_make_result("completed"))

    cb = CircuitBreaker("hubspot", "contacts")
    orig_draining = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]
    marked_healthy = []

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 2:
            _daemon_mod._draining = True

    async def _fake_mark_healthy(pool, connector, datatype):
        marked_healthy.append((connector, datatype))

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker", return_value=cb),
            patch("inandout.ingestion.daemon._mark_connector_unavailable", new_callable=AsyncMock),
            patch("inandout.ingestion.daemon._mark_connector_healthy",
                  side_effect=_fake_mark_healthy),
        ):
            await _daemon_mod._polling_loop(
                engine, connector_cfg, "contacts", _make_ingestion_cfg(), 60.0, set()
            )
    finally:
        _daemon_mod._draining = orig_draining

    assert engine.run_sync.call_count >= 1, "run_sync should have been called"
    assert marked_healthy, "_mark_connector_healthy should be called after a successful sync"


# ---------------------------------------------------------------------------
# Paused connector → run_sync skipped, sleep still called
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_paused_connector_skips_run_sync():
    """When is_paused returns True, run_sync must not be called."""
    engine = _make_engine()
    cb = MagicMock()
    cb.allow_request.return_value = True
    cb.state = "closed"

    orig_draining = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 1:
            _daemon_mod._draining = True

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker", return_value=cb),
            patch("inandout.ingestion.daemon.is_paused", return_value=True),
            patch("inandout.ingestion.daemon._mark_connector_unavailable", new_callable=AsyncMock),
            patch("inandout.ingestion.daemon._mark_connector_healthy", new_callable=AsyncMock),
        ):
            await _daemon_mod._polling_loop(
                engine, _make_connector_cfg(), "contacts",
                _make_ingestion_cfg(), 60.0, set(),
            )
    finally:
        _daemon_mod._draining = orig_draining

    engine.run_sync.assert_not_called()
