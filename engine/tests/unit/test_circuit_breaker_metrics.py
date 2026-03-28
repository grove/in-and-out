"""Unit tests for CircuitBreaker._emit_metric Prometheus gauge integration.

Verifies that after state transitions, the inout_circuit_breaker_state gauge
in the shared REGISTRY reflects the expected numeric value:
  0 = closed, 0.5 = half_open, 1 = open.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState, reset_all


@pytest.fixture(autouse=True)
def _clear_registry():
    reset_all()
    yield
    reset_all()


def _gauge_value(connector: str, datatype: str) -> float:
    from inandout.observability.metrics import circuit_breaker_state
    return circuit_breaker_state.labels(
        connector=connector,
        datatype=datatype,
        namespace="public",
    )._value.get()


# ---------------------------------------------------------------------------
# CLOSED → metric = 0
# ---------------------------------------------------------------------------

def test_metric_is_zero_on_initial_closed_state():
    """A fresh circuit breaker must emit 0 (closed) after first state emission."""
    cb = CircuitBreaker("metric_test", "contacts", failure_threshold=5)
    cb.record_success()  # triggers _emit_metric from closed state
    assert _gauge_value("metric_test", "contacts") == 0.0


# ---------------------------------------------------------------------------
# Trip OPEN → metric = 1
# ---------------------------------------------------------------------------

def test_metric_is_one_when_tripped_open():
    cb = CircuitBreaker("metric_test", "deals", failure_threshold=1)
    cb.record_failure()
    assert cb.state == CircuitState.open
    assert _gauge_value("metric_test", "deals") == 1.0


# ---------------------------------------------------------------------------
# OPEN → HALF_OPEN → metric = 0.5
# ---------------------------------------------------------------------------

def test_metric_is_half_on_half_open():
    cb = CircuitBreaker("metric_test", "leads", failure_threshold=1, recovery_timeout=30.0)
    cb.record_failure()
    # Fast-forward time past recovery_timeout to trigger HALF_OPEN
    with patch("inandout.transport.circuit_breaker.time.monotonic", return_value=cb._opened_at + 31.0):
        _ = cb.state  # triggers _maybe_transition_to_half_open → _emit_metric
    assert _gauge_value("metric_test", "leads") == 0.5


# ---------------------------------------------------------------------------
# HALF_OPEN → CLOSED on success → metric = 0
# ---------------------------------------------------------------------------

def test_metric_returns_to_zero_after_recovery():
    cb = CircuitBreaker("metric_test", "orders", failure_threshold=1, recovery_timeout=30.0)
    cb.record_failure()
    with patch("inandout.transport.circuit_breaker.time.monotonic", return_value=cb._opened_at + 31.0):
        _ = cb.state
    cb.record_success()
    assert _gauge_value("metric_test", "orders") == 0.0


# ---------------------------------------------------------------------------
# reset() → metric = 0
# ---------------------------------------------------------------------------

def test_metric_is_zero_after_reset():
    cb = CircuitBreaker("metric_test", "tickets", failure_threshold=1)
    cb.record_failure()
    assert _gauge_value("metric_test", "tickets") == 1.0
    cb.reset()
    assert _gauge_value("metric_test", "tickets") == 0.0
