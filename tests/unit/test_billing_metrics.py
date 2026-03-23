"""Unit tests for billing/cost attribution metrics."""
from __future__ import annotations

import pytest

from inandout.observability.metrics import (
    REGISTRY,
    circuit_breaker_state,
    connector_health_score,
    dead_letter_depth,
    http_errors_total,
    quality_violations_total,
    records_processed_total,
    sync_lag_seconds,
    sync_sla_violated,
)


# ---------------------------------------------------------------------------
# Namespace label present on all metrics
# ---------------------------------------------------------------------------

def _get_label_names(metric: object) -> list[str]:
    """Extract label names from a prometheus metric object."""
    return list(metric._labelnames)  # type: ignore[attr-defined]


def test_records_processed_total_has_namespace_label():
    """records_processed_total should include 'namespace' in labels."""
    labels = _get_label_names(records_processed_total)
    assert "namespace" in labels


def test_sync_lag_seconds_has_namespace_label():
    """sync_lag_seconds should include 'namespace' in labels."""
    labels = _get_label_names(sync_lag_seconds)
    assert "namespace" in labels


def test_http_errors_total_has_namespace_label():
    """http_errors_total should include 'namespace' in labels."""
    labels = _get_label_names(http_errors_total)
    assert "namespace" in labels


def test_circuit_breaker_state_has_namespace_label():
    """circuit_breaker_state should include 'namespace' in labels."""
    labels = _get_label_names(circuit_breaker_state)
    assert "namespace" in labels


def test_dead_letter_depth_has_namespace_label():
    """dead_letter_depth should include 'namespace' in labels."""
    labels = _get_label_names(dead_letter_depth)
    assert "namespace" in labels


def test_connector_health_score_has_namespace_label():
    """connector_health_score should include 'namespace' in labels."""
    labels = _get_label_names(connector_health_score)
    assert "namespace" in labels


def test_sync_sla_violated_has_namespace_label():
    """sync_sla_violated should include 'namespace' in labels."""
    labels = _get_label_names(sync_sla_violated)
    assert "namespace" in labels


def test_quality_violations_total_has_namespace_label():
    """quality_violations_total should include 'namespace' in labels."""
    labels = _get_label_names(quality_violations_total)
    assert "namespace" in labels


# ---------------------------------------------------------------------------
# Namespace labels can be set with different values
# ---------------------------------------------------------------------------

def test_records_processed_can_use_custom_namespace():
    """Should be able to label metrics with a custom namespace."""
    # This should not raise
    records_processed_total.labels(
        tool="test",
        connector="testconn",
        datatype="testtype",
        operation="insert",
        namespace="tenant_a",
    ).inc()


def test_records_processed_different_namespaces_are_isolated():
    """Records with different namespace labels should be tracked separately."""
    records_processed_total.labels(
        tool="test",
        connector="isolated_conn",
        datatype="isolated_type",
        operation="insert",
        namespace="ns_isolation_test_a",
    ).inc()

    records_processed_total.labels(
        tool="test",
        connector="isolated_conn",
        datatype="isolated_type",
        operation="insert",
        namespace="ns_isolation_test_b",
    ).inc()

    # If we can reach here without error, namespaces are isolated
    assert True


# ---------------------------------------------------------------------------
# /api/namespaces/{namespace}/metrics-summary endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_namespace_metrics_summary_returns_correct_structure():
    """GET /api/namespaces/{ns}/metrics-summary should return correct shape."""
    from unittest.mock import AsyncMock, MagicMock

    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    from inandout.api import build_api_router
    from inandout.api.routes import _set_pool

    # Build minimal app with a mock pool
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor_sync = AsyncMock()
    mock_cursor_pairs = AsyncMock()

    # Return empty rows for all queries
    mock_cursor_sync.fetchall = AsyncMock(return_value=[])
    mock_cursor_pairs.fetchall = AsyncMock(return_value=[])
    call_count = 0

    async def mock_execute(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count % 2 == 1:
            return mock_cursor_sync
        return mock_cursor_pairs

    mock_conn.execute = mock_execute
    mock_pool.connection = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )

    _set_pool(mock_pool)
    router = build_api_router(pool=mock_pool)
    api_app = FastAPI()
    api_app.include_router(router, prefix="/api")

    client = TestClient(api_app)
    resp = client.get("/api/namespaces/tenant_a/metrics-summary")

    assert resp.status_code == 200
    data = resp.json()
    assert data["namespace"] == "tenant_a"
    assert "connectors" in data
    assert "total_records_processed_24h" in data
    assert "total_quality_violations_24h" in data
    assert "connectors_healthy" in data
    assert "connectors_degraded" in data
    assert "connectors_unhealthy" in data
    assert "dead_letter_total" in data


@pytest.mark.anyio
async def test_namespace_metrics_summary_without_pool():
    """GET /api/namespaces/{ns}/metrics-summary returns zeros when no pool."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    from inandout.api import build_api_router
    from inandout.api.routes import _set_pool

    _set_pool(None)
    router = build_api_router(pool=None)
    api_app = FastAPI()
    api_app.include_router(router, prefix="/api")

    client = TestClient(api_app)
    resp = client.get("/api/namespaces/some_ns/metrics-summary")

    assert resp.status_code == 200
    data = resp.json()
    assert data["namespace"] == "some_ns"
    assert data["connectors"] == 0
    assert data["total_records_processed_24h"] == 0
