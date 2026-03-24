"""Unit tests for T1 #47 — composite health scoring based on sync run history."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock


def _make_pool(rows: list[tuple[str]] | None = None, raise_exc: Exception | None = None):
    """Build a mock pool that returns the given rows from fetchall."""
    cursor_mock = AsyncMock()
    cursor_mock.fetchall = AsyncMock(return_value=rows or [])
    conn_mock = AsyncMock()
    conn_mock.execute = AsyncMock(return_value=cursor_mock)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx.__aexit__ = AsyncMock(return_value=None)
    pool = MagicMock()
    if raise_exc is not None:
        pool.connection = MagicMock(side_effect=raise_exc)
    else:
        pool.connection = MagicMock(return_value=ctx)
    return pool


# ---------------------------------------------------------------------------
# compute_health_score
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_completed_returns_1():
    """10 completed runs → score 1.0."""
    from inandout.observability.health_score import compute_health_score
    rows = [("completed",)] * 10
    pool = _make_pool(rows)
    score = await compute_health_score(pool, "crm", "contacts")
    assert score == 1.0


@pytest.mark.asyncio
async def test_all_failed_returns_0():
    """10 failed runs → score 0.0."""
    from inandout.observability.health_score import compute_health_score
    rows = [("failed",)] * 10
    pool = _make_pool(rows)
    score = await compute_health_score(pool, "crm", "contacts")
    assert score == 0.0


@pytest.mark.asyncio
async def test_half_completed_returns_point_five():
    """5 completed + 5 failed → score 0.5."""
    from inandout.observability.health_score import compute_health_score
    rows = [("completed",)] * 5 + [("failed",)] * 5
    pool = _make_pool(rows)
    score = await compute_health_score(pool, "crm", "contacts")
    assert score == 0.5


@pytest.mark.asyncio
async def test_mixed_statuses_correct_ratio():
    """7 completed + 1 aborted + 2 failed → 7/10 = 0.7."""
    from inandout.observability.health_score import compute_health_score
    rows = [("completed",)] * 7 + [("failed",)] * 2 + [("aborted",)] * 1
    pool = _make_pool(rows)
    score = await compute_health_score(pool, "crm", "contacts")
    assert score == 0.7


@pytest.mark.asyncio
async def test_no_rows_returns_1_optimistic():
    """No history yet → optimistic fallback of 1.0."""
    from inandout.observability.health_score import compute_health_score
    pool = _make_pool([])
    score = await compute_health_score(pool, "new_connector", "contacts")
    assert score == 1.0


@pytest.mark.asyncio
async def test_db_error_returns_1_optimistic():
    """DB failure → optimistic fallback of 1.0 (don't flap alerts)."""
    from inandout.observability.health_score import compute_health_score
    pool = _make_pool(raise_exc=Exception("connection failed"))
    score = await compute_health_score(pool, "crm", "contacts")
    assert score == 1.0


@pytest.mark.asyncio
async def test_score_is_float_not_int():
    """Returned score should always be float."""
    from inandout.observability.health_score import compute_health_score
    rows = [("completed",)] * 3
    pool = _make_pool(rows)
    score = await compute_health_score(pool, "crm", "contacts")
    assert isinstance(score, float)


@pytest.mark.asyncio
async def test_score_within_bounds():
    """Score must always be in [0.0, 1.0]."""
    from inandout.observability.health_score import compute_health_score
    for n_completed, n_failed in [(0, 5), (3, 7), (10, 0), (1, 9)]:
        rows = [("completed",)] * n_completed + [("failed",)] * n_failed
        pool = _make_pool(rows)
        score = await compute_health_score(pool, "crm", "contacts")
        assert 0.0 <= score <= 1.0


@pytest.mark.asyncio
async def test_custom_window_respected():
    """Callers can override the window size."""
    from inandout.observability.health_score import compute_health_score
    # Provide 3 rows; window=3 means all 3 are used
    rows = [("completed",), ("completed",), ("failed",)]
    pool = _make_pool(rows)
    score = await compute_health_score(pool, "crm", "contacts", window=3)
    assert pytest.approx(score, abs=1e-4) == round(2 / 3, 4)
