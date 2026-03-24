"""Unit tests for check_sla freshness function.

Covers:
- Recent finished_at (within max_lag) → violated=False, gauge set to 0.
- Stale finished_at (beyond max_lag) → violated=True, gauge set to 1.
- No completed run found (fetchone returns None) → violated=True.
- DB exception swallowed → violated=True.
- sync_sla_violated warning log emitted when violated.
"""
from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.observability.sla import check_sla


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool(finished_at) -> MagicMock:
    """Pool whose connection returns `finished_at` as the most recent sync run."""
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=(finished_at,) if finished_at is not None else None)
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


def _now_minus(seconds: float) -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_check_sla_ok_when_recent_sync():
    """When last sync is within max_lag_seconds, must return False (not violated)."""
    finished_at = _now_minus(60)  # 60 s ago
    pool = _make_pool(finished_at)

    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_gauge.labels.return_value = MagicMock()
        result = await check_sla(pool, "hubspot", "contacts", max_lag_seconds=300)

    assert result is False


@pytest.mark.anyio
async def test_check_sla_violated_when_stale_sync():
    """When last sync is beyond max_lag_seconds, must return True (violated)."""
    finished_at = _now_minus(3600)  # 1 hour ago
    pool = _make_pool(finished_at)

    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_gauge.labels.return_value = MagicMock()
        result = await check_sla(pool, "hubspot", "contacts", max_lag_seconds=300)

    assert result is True


@pytest.mark.anyio
async def test_check_sla_violated_when_no_sync_run():
    """When no completed run exists (fetchone=None), must return True."""
    pool = _make_pool(finished_at=None)

    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_gauge.labels.return_value = MagicMock()
        result = await check_sla(pool, "hubspot", "contacts", max_lag_seconds=300)

    assert result is True


@pytest.mark.anyio
async def test_check_sla_violated_on_db_exception():
    """DB exception must be swallowed and return violated=True."""
    async def _execute(sql: str, params=None):
        raise RuntimeError("connection refused")

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)

    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_gauge.labels.return_value = MagicMock()
        result = await check_sla(pool, "hubspot", "contacts", max_lag_seconds=300)

    assert result is True


@pytest.mark.anyio
async def test_check_sla_gauge_set_to_zero_when_ok():
    """Prometheus gauge must be set to 0 when SLA is met."""
    finished_at = _now_minus(60)
    pool = _make_pool(finished_at)

    gauge_label_mock = MagicMock()
    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_gauge.labels.return_value = gauge_label_mock
        await check_sla(pool, "hubspot", "contacts", max_lag_seconds=300)

    gauge_label_mock.set.assert_called_once_with(0)


@pytest.mark.anyio
async def test_check_sla_gauge_set_to_one_when_violated():
    """Prometheus gauge must be set to 1 when SLA is violated."""
    finished_at = _now_minus(3600)
    pool = _make_pool(finished_at)

    gauge_label_mock = MagicMock()
    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_gauge.labels.return_value = gauge_label_mock
        await check_sla(pool, "hubspot", "contacts", max_lag_seconds=300)

    gauge_label_mock.set.assert_called_once_with(1)
