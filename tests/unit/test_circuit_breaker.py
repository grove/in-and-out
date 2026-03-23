"""Unit tests for circuit breaker state machine."""
from __future__ import annotations

import time

import pytest

from inandout.transport.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    get_circuit_breaker,
    reset_all,
)


@pytest.fixture(autouse=True)
def clear_registry():
    reset_all()
    yield
    reset_all()


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

def test_initial_state_is_closed():
    cb = CircuitBreaker("test", "contacts")
    assert cb.state == CircuitState.closed
    assert cb.allow_request() is True


# ---------------------------------------------------------------------------
# CLOSED → OPEN
# ---------------------------------------------------------------------------

def test_trips_open_after_threshold():
    cb = CircuitBreaker("test", "contacts", failure_threshold=3)
    for _ in range(3):
        cb.record_failure()
    assert cb.state == CircuitState.open
    assert cb.allow_request() is False


def test_does_not_trip_below_threshold():
    cb = CircuitBreaker("test", "contacts", failure_threshold=3)
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.closed


# ---------------------------------------------------------------------------
# Success resets failure counter
# ---------------------------------------------------------------------------

def test_success_resets_counter():
    cb = CircuitBreaker("test", "contacts", failure_threshold=3)
    cb.record_failure()
    cb.record_failure()
    cb.record_success()
    # Needs 3 more failures to trip
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.closed
    cb.record_failure()
    assert cb.state == CircuitState.open


# ---------------------------------------------------------------------------
# OPEN → HALF_OPEN
# ---------------------------------------------------------------------------

def test_transitions_to_half_open_after_timeout(monkeypatch):
    cb = CircuitBreaker("test", "contacts", failure_threshold=1, recovery_timeout=1.0)
    cb.record_failure()
    assert cb.state == CircuitState.open

    # Advance time past recovery timeout
    start = cb._opened_at
    monkeypatch.setattr(
        "inandout.transport.circuit_breaker.time.monotonic",
        lambda: start + 2.0,
    )
    assert cb.state == CircuitState.half_open
    assert cb.allow_request() is True


def test_remains_open_before_timeout(monkeypatch):
    cb = CircuitBreaker("test", "contacts", failure_threshold=1, recovery_timeout=60.0)
    cb.record_failure()
    # Don't advance time
    assert cb.state == CircuitState.open
    assert cb.allow_request() is False


# ---------------------------------------------------------------------------
# HALF_OPEN → CLOSED / OPEN
# ---------------------------------------------------------------------------

def test_half_open_success_closes_circuit(monkeypatch):
    cb = CircuitBreaker("test", "contacts", failure_threshold=1, recovery_timeout=1.0)
    cb.record_failure()
    start = cb._opened_at
    monkeypatch.setattr(
        "inandout.transport.circuit_breaker.time.monotonic",
        lambda: start + 2.0,
    )
    assert cb.state == CircuitState.half_open
    cb.record_success()
    assert cb.state == CircuitState.closed


def test_half_open_failure_re_opens(monkeypatch):
    cb = CircuitBreaker("test", "contacts", failure_threshold=1, recovery_timeout=1.0)
    cb.record_failure()
    start = cb._opened_at
    monkeypatch.setattr(
        "inandout.transport.circuit_breaker.time.monotonic",
        lambda: start + 2.0,
    )
    assert cb.state == CircuitState.half_open
    cb.record_failure()
    assert cb.state == CircuitState.open


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_get_circuit_breaker_returns_same_instance():
    cb1 = get_circuit_breaker("myconn", "dtype")
    cb2 = get_circuit_breaker("myconn", "dtype")
    assert cb1 is cb2


def test_get_circuit_breaker_different_keys_different_instances():
    cb1 = get_circuit_breaker("conn_a", "dtype")
    cb2 = get_circuit_breaker("conn_b", "dtype")
    assert cb1 is not cb2


def test_reset_all_clears_registry():
    get_circuit_breaker("conn_x", "dtype")
    reset_all()
    cb = get_circuit_breaker("conn_x", "dtype")
    assert cb.state == CircuitState.closed  # fresh instance
