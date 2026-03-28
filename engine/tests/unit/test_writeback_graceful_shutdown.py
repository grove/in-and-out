"""Unit tests for writeback daemon graceful shutdown (A5)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest

import inandout.writeback.daemon as wb_daemon


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine(side_effect=None) -> MagicMock:
    engine = MagicMock()
    if side_effect:
        engine.run_writeback_cycle = AsyncMock(side_effect=side_effect)
    else:
        result = MagicMock()
        result.processed = 1
        result.skipped = 0
        result.failed = 0
        engine.run_writeback_cycle = AsyncMock(return_value=result)
    return engine


def _make_connector_cfg(name: str = "test_wb") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    return cfg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_draining_true_exits_polling_loop_after_current_cycle():
    """When _draining=True, the polling loop exits after completing the current cycle."""
    wb_daemon._draining = True
    try:
        engine = _make_engine()
        connector_cfg = _make_connector_cfg()
        writeback_cfg = MagicMock()
        writeback_cfg.streaming = False

        # The loop should exit immediately because _draining is checked at the top
        with anyio.fail_after(2.0):
            await wb_daemon._writeback_polling_loop(
                engine, connector_cfg, "items", writeback_cfg,
                "_delta_test_wb_items", interval_secs=0.01
            )

        # Should not call run_writeback_cycle (exits before running the cycle)
        engine.run_writeback_cycle.assert_not_called()
    finally:
        wb_daemon._draining = False


@pytest.mark.anyio
async def test_draining_checked_after_cycle_completes():
    """Loop completes current in-flight cycle before checking drain flag."""
    call_count = 0

    async def _slow_cycle(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        # Set draining after first call completes
        wb_daemon._draining = True
        result = MagicMock()
        result.processed = 1
        result.skipped = 0
        result.failed = 0
        return result

    wb_daemon._draining = False
    try:
        engine = _make_engine(side_effect=_slow_cycle)
        connector_cfg = _make_connector_cfg()
        writeback_cfg = MagicMock()
        writeback_cfg.streaming = False

        # First iteration: draining=False, runs cycle, sets draining=True in cycle
        # Then sleeps, then next iteration top-of-loop check exits
        with anyio.fail_after(3.0):
            await wb_daemon._writeback_polling_loop(
                engine, connector_cfg, "items", writeback_cfg,
                "_delta_test_wb_items", interval_secs=0.01
            )

        # First cycle ran, then loop exited
        assert call_count == 1
    finally:
        wb_daemon._draining = False


@pytest.mark.anyio
async def test_ready_endpoint_returns_503_during_drain():
    """/ready returns 503 with status=draining when _draining=True."""
    from starlette.testclient import TestClient

    wb_daemon._draining = True
    try:
        app = wb_daemon._build_health_app()
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/ready")
        assert response.status_code == 503
        assert response.json()["status"] == "draining"
    finally:
        wb_daemon._draining = False


@pytest.mark.anyio
async def test_ready_endpoint_returns_200_when_not_draining():
    """/ready returns 200 when not draining."""
    from starlette.testclient import TestClient

    wb_daemon._draining = False
    app = wb_daemon._build_health_app()
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"


@pytest.mark.anyio
async def test_in_flight_cycle_not_interrupted_mid_batch():
    """An already-started writeback cycle completes before drain takes effect."""
    completed_cycles = []

    async def _full_cycle(*args, **kwargs):
        # Simulate a real async cycle that takes some time
        await anyio.sleep(0.01)
        completed_cycles.append("done")
        result = MagicMock()
        result.processed = 5
        result.skipped = 0
        result.failed = 0
        return result

    wb_daemon._draining = False
    try:
        engine = _make_engine(side_effect=_full_cycle)
        connector_cfg = _make_connector_cfg()
        writeback_cfg = MagicMock()
        writeback_cfg.streaming = False

        async def _set_drain_soon():
            await anyio.sleep(0.005)
            wb_daemon._draining = True

        async with anyio.create_task_group() as tg:
            tg.start_soon(_set_drain_soon)
            with anyio.fail_after(3.0):
                await wb_daemon._writeback_polling_loop(
                    engine, connector_cfg, "items", writeback_cfg,
                    "_delta_test_wb_items", interval_secs=0.01
                )

        # At least one cycle must have completed
        assert len(completed_cycles) >= 1
    finally:
        wb_daemon._draining = False
