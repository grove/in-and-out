"""Unit tests for SLA / freshness monitoring — Step 47."""
from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.observability.sla import check_sla, check_all_slas


def _make_pool(finished_at=None, completed_status=True) -> AsyncMock:
    """Build a mock pool that returns a sync run row with the given finished_at."""
    pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=mock_conn)

    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(finished_at,) if finished_at else None)
    mock_conn.execute = AsyncMock(return_value=cursor)

    return pool


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


@pytest.mark.anyio
async def test_check_sla_recent_sync_not_violated():
    """Last sync was 5 minutes ago with max_lag=3600s — SLA is NOT violated."""
    recent = _utcnow() - datetime.timedelta(minutes=5)
    pool = _make_pool(finished_at=recent)

    with patch("inandout.observability.sla.logger") as mock_log:
        violated = await check_sla(pool, "my_conn", "my_dtype", max_lag_seconds=3600)

    assert violated is False
    mock_log.warning.assert_not_called()


@pytest.mark.anyio
async def test_check_sla_old_sync_violated():
    """Last sync was 2 hours ago with max_lag=3600s — SLA IS violated."""
    old = _utcnow() - datetime.timedelta(hours=2)
    pool = _make_pool(finished_at=old)

    with patch("inandout.observability.sla.logger") as mock_log:
        violated = await check_sla(pool, "my_conn", "my_dtype", max_lag_seconds=3600)

    assert violated is True
    mock_log.warning.assert_called_once()
    call_kwargs = mock_log.warning.call_args[0]
    assert "sync_sla_violated" in call_kwargs[0]


@pytest.mark.anyio
async def test_check_sla_no_runs_violated():
    """No sync runs found — SLA IS violated (no data = stale)."""
    pool = _make_pool(finished_at=None)

    with patch("inandout.observability.sla.logger") as mock_log:
        violated = await check_sla(pool, "my_conn", "my_dtype", max_lag_seconds=3600)

    assert violated is True
    mock_log.warning.assert_called_once()


@pytest.mark.anyio
async def test_check_sla_sets_prometheus_gauge():
    """check_sla updates the Prometheus gauge correctly."""
    recent = _utcnow() - datetime.timedelta(minutes=5)
    pool = _make_pool(finished_at=recent)

    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_labels = MagicMock()
        mock_gauge.labels = MagicMock(return_value=mock_labels)

        with patch("inandout.observability.sla.logger"):
            violated = await check_sla(pool, "conn", "dtype", max_lag_seconds=3600)

    assert violated is False
    mock_gauge.labels.assert_called_once_with(connector="conn", datatype="dtype")
    mock_labels.set.assert_called_once_with(0)


@pytest.mark.anyio
async def test_check_sla_violated_gauge_set_to_1():
    """When violated, gauge is set to 1."""
    old = _utcnow() - datetime.timedelta(hours=5)
    pool = _make_pool(finished_at=old)

    with patch("inandout.observability.metrics.sync_sla_violated") as mock_gauge:
        mock_labels = MagicMock()
        mock_gauge.labels = MagicMock(return_value=mock_labels)

        with patch("inandout.observability.sla.logger"):
            violated = await check_sla(pool, "conn", "dtype", max_lag_seconds=3600)

    assert violated is True
    mock_labels.set.assert_called_once_with(1)


@pytest.mark.anyio
async def test_check_all_slas_aggregates():
    """check_all_slas iterates all connector/datatypes with max_lag_seconds."""
    recent = _utcnow() - datetime.timedelta(minutes=5)
    pool = _make_pool(finished_at=recent)

    # Build fake connector configs
    class _FakeDtype:
        def __init__(self, name, max_lag=None):
            self.ingestion = MagicMock()
            self.ingestion.schedule = MagicMock()
            self.ingestion.schedule.max_lag_seconds = max_lag

    class _FakeConnector:
        def __init__(self, name, datatypes):
            self.name = name
            self.datatypes = datatypes

    class _FakeFileCfg:
        def __init__(self, connector):
            self.connector = connector

    configs = [
        _FakeFileCfg(_FakeConnector("conn_a", {
            "orders": _FakeDtype("orders", max_lag=3600),
            "no_sla": _FakeDtype("no_sla", max_lag=None),  # No SLA — skipped
        })),
        _FakeFileCfg(_FakeConnector("conn_b", {
            "items": _FakeDtype("items", max_lag=1800),
        })),
    ]

    with patch("inandout.observability.sla.logger"):
        results = await check_all_slas(pool, configs)

    # Only connector/datatypes with max_lag_seconds are checked
    assert ("conn_a", "orders") in results
    assert ("conn_b", "items") in results
    assert ("conn_a", "no_sla") not in results

    # Both should not be violated (recent sync)
    assert results[("conn_a", "orders")] is False
    assert results[("conn_b", "items")] is False
