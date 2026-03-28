"""Unit tests for the _polling_loop exception-from-run_sync path.

When engine.run_sync raises (rather than returning a failed SyncResult),
the exception handler must:
- Call cb.record_failure().
- Call _mark_connector_unavailable once the circuit opens.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod
from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState


# ---------------------------------------------------------------------------
# Helpers (kept self-contained)
# ---------------------------------------------------------------------------

def _make_ingestion_cfg(interval: str = "60s") -> MagicMock:
    cfg = MagicMock()
    sched = MagicMock()
    sched.interval = interval
    sched.cron = None
    cfg.schedule = sched
    return cfg


def _make_connector_cfg(name: str = "hubspot") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    return cfg


async def _run_loop_one_tick(
    engine: MagicMock,
    connector_cfg: MagicMock,
    cb: CircuitBreaker,
    marked_unavailable: list,
    marked_healthy: list,
) -> None:
    """Drive _polling_loop through exactly one tick then drain."""
    orig = _daemon_mod._draining
    _daemon_mod._draining = False
    tick = [0]

    async def _fake_sleep(secs: float) -> None:
        tick[0] += 1
        if tick[0] >= 1:
            _daemon_mod._draining = True

    async def _fake_unavailable(pool, connector, datatype, reason=""):
        marked_unavailable.append((connector, datatype, reason))

    async def _fake_healthy(pool, connector, datatype):
        marked_healthy.append((connector, datatype))

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
                engine, connector_cfg, "contacts",
                _make_ingestion_cfg(), 60.0, set(),
            )
    finally:
        _daemon_mod._draining = orig


# ---------------------------------------------------------------------------
# Exception → record_failure + _mark_connector_unavailable when circuit opens
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_run_sync_exception_calls_record_failure():
    """When run_sync raises, cb.record_failure() must be called."""
    engine = MagicMock()
    engine._pool = MagicMock()
    engine.run_sync = AsyncMock(side_effect=RuntimeError("connection refused"))

    # threshold=1 so a single failure opens the circuit
    cb = CircuitBreaker("hubspot", "contacts", failure_threshold=1)

    marked_unavailable: list = []
    marked_healthy: list = []

    await _run_loop_one_tick(engine, _make_connector_cfg(), cb,
                             marked_unavailable, marked_healthy)

    assert cb.state == CircuitState.open, (
        "Circuit should be open after one failure with threshold=1"
    )


@pytest.mark.anyio
async def test_run_sync_exception_triggers_mark_unavailable_when_circuit_opens():
    """When circuit opens after run_sync raises, _mark_connector_unavailable is called."""
    engine = MagicMock()
    engine._pool = MagicMock()
    engine.run_sync = AsyncMock(side_effect=RuntimeError("upstream timeout"))

    cb = CircuitBreaker("hubspot", "contacts", failure_threshold=1)

    marked_unavailable: list = []
    marked_healthy: list = []

    await _run_loop_one_tick(engine, _make_connector_cfg("hubspot"), cb,
                             marked_unavailable, marked_healthy)

    assert marked_unavailable, "_mark_connector_unavailable should be called when circuit opens"
    connector, datatype, reason = marked_unavailable[0]
    assert connector == "hubspot"
    assert datatype == "contacts"
    assert "upstream timeout" in reason


@pytest.mark.anyio
async def test_run_sync_exception_does_not_call_mark_healthy():
    """When run_sync raises, _mark_connector_healthy must NOT be called."""
    engine = MagicMock()
    engine._pool = MagicMock()
    engine.run_sync = AsyncMock(side_effect=RuntimeError("boom"))

    cb = CircuitBreaker("hubspot", "contacts", failure_threshold=5)

    marked_unavailable: list = []
    marked_healthy: list = []

    await _run_loop_one_tick(engine, _make_connector_cfg(), cb,
                             marked_unavailable, marked_healthy)

    assert not marked_healthy, "_mark_connector_healthy must not be called on exception"
