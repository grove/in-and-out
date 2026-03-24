"""Unit test — _polling_loop half-open recovery sequence.

After the circuit opens and recovery_timeout elapses, the next poll tick
should call run_sync again (HALF_OPEN allows one probe request).
A successful result should close the circuit.

Uses a real CircuitBreaker with a near-zero timeout so time mocking is
not needed, combining anyio.sleep patching (to control ticks) with the
real FSM.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod
from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState, reset_all


@pytest.fixture(autouse=True)
def _clean_cb():
    reset_all()
    yield
    reset_all()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_ingestion_cfg(interval: str = "0s") -> MagicMock:
    cfg = MagicMock()
    sched = MagicMock()
    sched.interval = interval
    sched.cron = None
    cfg.schedule = sched
    return cfg


def _make_connector_cfg(name: str = "recovery_test") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    return cfg


# ---------------------------------------------------------------------------
# Recovery: OPEN → HALF_OPEN → CLOSED
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_circuit_recovers_to_closed_after_successful_probe():
    """
    Sequence:
      tick 1 – run_sync raises → CLOSED → OPEN (threshold=1)
      tick 2 – circuit OPEN, recovery_timeout has elapsed → HALF_OPEN, probe allowed
               run_sync succeeds → HALF_OPEN → CLOSED
      tick 3 – drain
    """
    cb = CircuitBreaker("recovery_test", "contacts", failure_threshold=1, recovery_timeout=0.0)

    run_sync_calls: list[str] = []

    # Tick 1: raises (trips OPEN). Tick 2: succeeds (closes).
    async def _run_sync_side_effect(*args, **kwargs):
        call_n = len(run_sync_calls)
        run_sync_calls.append(f"call_{call_n}")
        if call_n == 0:
            raise RuntimeError("first call fails")
        result = MagicMock()
        result.status = "completed"
        result.error_message = None
        return result

    engine = MagicMock()
    engine._pool = MagicMock()
    engine.run_sync = AsyncMock(side_effect=_run_sync_side_effect)

    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]
    marked_unavailable: list = []
    marked_healthy: list = []

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 3:
            _daemon_mod._draining = True

    async def _fake_unavailable(pool, connector, datatype, reason=""):
        marked_unavailable.append(True)

    async def _fake_healthy(pool, connector, datatype):
        marked_healthy.append(True)

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker", return_value=cb),
            patch("inandout.ingestion.daemon._mark_connector_unavailable",
                  side_effect=_fake_unavailable),
            patch("inandout.ingestion.daemon._mark_connector_healthy",
                  side_effect=_fake_healthy),
        ):
            await _daemon_mod._polling_loop(
                engine, _make_connector_cfg(), "contacts",
                _make_ingestion_cfg(), 0.0, set(),
            )
    finally:
        _daemon_mod._draining = orig

    # Circuit should be CLOSED after the successful probe
    assert cb.state == CircuitState.closed, (
        f"Expected circuit to be CLOSED after successful probe, got {cb.state}"
    )
    # run_sync was called at least twice (once failing, once succeeding)
    assert len(run_sync_calls) >= 2, (
        f"Expected at least 2 run_sync calls, got {len(run_sync_calls)}"
    )
    # mark_healthy was called after the successful probe
    assert marked_healthy, "_mark_connector_healthy must be called after successful probe"


@pytest.mark.anyio
async def test_circuit_stays_open_when_probe_fails():
    """
    Sequence:
      tick 1 – run_sync raises → OPEN (threshold=1)
      tick 2 – HALF_OPEN probe, run_sync raises again → back to OPEN
      tick 3 – drain
    """
    cb = CircuitBreaker("recovery_test", "orders", failure_threshold=1, recovery_timeout=0.0)

    call_count = [0]

    async def _always_fail(*args, **kwargs):
        call_count[0] += 1
        raise RuntimeError("still broken")

    engine = MagicMock()
    engine._pool = MagicMock()
    engine.run_sync = AsyncMock(side_effect=_always_fail)

    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 3:
            _daemon_mod._draining = True

    try:
        with (
            patch("anyio.sleep", side_effect=_fake_sleep),
            patch("inandout.transport.circuit_breaker.get_circuit_breaker", return_value=cb),
            patch("inandout.ingestion.daemon._mark_connector_unavailable", new_callable=AsyncMock),
            patch("inandout.ingestion.daemon._mark_connector_healthy", new_callable=AsyncMock),
        ):
            await _daemon_mod._polling_loop(
                engine, _make_connector_cfg("recovery_test"), "orders",
                _make_ingestion_cfg(), 0.0, set(),
            )
    finally:
        _daemon_mod._draining = orig

    # After repeated failures the circuit must still be tripped (OPEN or HALF_OPEN),
    # not recovered to CLOSED.  With recovery_timeout=0.0 it may flip from OPEN→HALF_OPEN
    # instantly on every state-read, so we only assert it is not CLOSED.
    assert cb.state != CircuitState.closed, (
        f"Expected circuit to remain tripped after failed probes, got {cb.state}"
    )
