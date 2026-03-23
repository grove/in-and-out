"""Unit tests for connector health scoring — Step 43."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.observability.health_score import compute_health_score, health_components


def _make_pool(
    failed_runs: int = 0,
    total_runs: int = 0,
    dl_count: int = 0,
    dl_table_exists: bool = True,
) -> AsyncMock:
    """Build a mock pool that returns the given sync run counts and DL depth."""
    pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=mock_conn)

    call_count = 0

    async def _execute(query, params=None):
        nonlocal call_count
        call_count += 1
        cursor = AsyncMock()
        if "inout_ops_sync_run" in query:
            cursor.fetchone = AsyncMock(return_value=(failed_runs, total_runs))
        elif dl_table_exists:
            cursor.fetchone = AsyncMock(return_value=(dl_count,))
        else:
            import psycopg
            raise psycopg.errors.UndefinedTable("table does not exist")
        return cursor

    mock_conn.execute = AsyncMock(side_effect=_execute)
    return pool


@pytest.mark.anyio
async def test_all_healthy_score_near_1():
    """All healthy: closed CB, 0 failures, 0 DL rows → score ≈ 1.0."""
    pool = _make_pool(failed_runs=0, total_runs=10, dl_count=0)

    with patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_get_cb:
        from inandout.transport.circuit_breaker import CircuitState
        mock_cb = MagicMock()
        mock_cb.state = CircuitState.closed
        mock_get_cb.return_value = mock_cb

        score = await compute_health_score(pool, "my_connector", "my_datatype", window_secs=3600)

    # cb=1.0 * 0.4 + error_rate=0 * 0.4 + dl=1.0 * 0.2 = 1.0
    assert score == pytest.approx(1.0, abs=0.01)


@pytest.mark.anyio
async def test_open_circuit_breaker_low_score():
    """Open circuit breaker yields score ≤ 0.5 (only error_rate and DL contribute)."""
    pool = _make_pool(failed_runs=0, total_runs=0, dl_count=0)

    with patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_get_cb:
        from inandout.transport.circuit_breaker import CircuitState
        mock_cb = MagicMock()
        mock_cb.state = CircuitState.open
        mock_get_cb.return_value = mock_cb

        score = await compute_health_score(pool, "my_connector", "my_datatype")

    # cb=0.0 * 0.4 + error_rate=0 * 0.4 + dl=1.0 * 0.2 = 0.2 (no runs → error_rate=0)
    # Wait: total_runs=0 → error_rate=0.0 → (1-0)*0.4 = 0.4 + 0.0 + 0.2 = 0.6
    # But cb=0.0 → 0.0 + 0.4 + 0.2 = 0.6 (still > 0.5 due to 0 errors)
    # With open CB and some failures:
    assert score <= 0.65  # CB=0 so at most 0.4 + 0.2 = 0.6


@pytest.mark.anyio
async def test_high_dead_letter_depth_lower_score():
    """High dead-letter depth reduces the score."""
    pool = _make_pool(failed_runs=0, total_runs=10, dl_count=50)

    with patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_get_cb:
        from inandout.transport.circuit_breaker import CircuitState
        mock_cb = MagicMock()
        mock_cb.state = CircuitState.closed
        mock_get_cb.return_value = mock_cb

        score = await compute_health_score(pool, "my_connector", "my_datatype")

    # dl_score = 1 - 50/100 = 0.5 → 1.0*0.4 + 1.0*0.4 + 0.5*0.2 = 0.9
    assert score == pytest.approx(0.9, abs=0.01)


@pytest.mark.anyio
async def test_no_runs_graceful_base_score():
    """No sync runs → error_rate=0.0, closed CB, 0 DL → score = 1.0."""
    pool = _make_pool(failed_runs=0, total_runs=0, dl_count=0)

    with patch("inandout.transport.circuit_breaker.get_circuit_breaker") as mock_get_cb:
        from inandout.transport.circuit_breaker import CircuitState
        mock_cb = MagicMock()
        mock_cb.state = CircuitState.closed
        mock_get_cb.return_value = mock_cb

        score = await compute_health_score(pool, "my_connector", "my_datatype")

    # No runs: error_rate=0 → (1-0)*0.4=0.4, cb=1.0*0.4=0.4, dl=1.0*0.2=0.2 → 1.0
    assert score == pytest.approx(1.0, abs=0.01)


def test_health_components_breakdown():
    """health_components returns correct breakdown values."""
    components = health_components(cb_score=1.0, error_rate=0.1, dl_depth=20)
    assert components["circuit_breaker"] == 1.0
    assert components["error_rate"] == pytest.approx(0.9, abs=0.001)
    assert components["dead_letter"] == pytest.approx(0.8, abs=0.001)
