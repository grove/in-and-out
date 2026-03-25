"""Unit tests for circuit breaker exponential backoff enhancement."""
from __future__ import annotations

import time
import pytest

from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState


def test_circuit_breaker_exponential_backoff_on_repeated_failures():
    """Circuit breaker should apply exponential backoff when reopening from half_open."""
    cb = CircuitBreaker(
        "test_conn",
        "test_dt",
        failure_threshold=2,
        recovery_timeout=10.0,
        backoff_multiplier=2.0,
        max_recovery_timeout=100.0,
    )
    
    # Initial closed state
    assert cb.state == CircuitState.closed
    assert cb._current_recovery_timeout == 10.0
    
    # Trip to open
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.open
    assert cb._open_count == 1
    assert cb._current_recovery_timeout == 10.0  # No backoff on first open
    
    # Simulate time passing
    cb._opened_at = time.monotonic() - 11.0
    assert cb.state == CircuitState.half_open
    
    # Half-open probe fails → should reopen with backoff
    cb.record_failure()
    assert cb.state == CircuitState.open
    assert cb._open_count == 2
    assert cb._current_recovery_timeout == 20.0  # 10 * 2
    
    # Simulate time passing again
    cb._opened_at = time.monotonic() - 21.0
    assert cb.state == CircuitState.half_open
    
    # Fail again → backoff again
    cb.record_failure()
    assert cb.state == CircuitState.open
    assert cb._open_count == 3
    assert cb._current_recovery_timeout == 40.0  # 20 * 2


def test_circuit_breaker_backoff_caps_at_max():
    """Circuit breaker backoff should not exceed max_recovery_timeout."""
    cb = CircuitBreaker(
        "test_conn",
        "test_dt",
        failure_threshold=1,
        recovery_timeout=10.0,
        backoff_multiplier=10.0,
        max_recovery_timeout=50.0,
    )
    
    # Open and reopen multiple times
    cb.record_failure()
    assert cb._current_recovery_timeout == 10.0
    
    cb._opened_at = time.monotonic() - 11.0
    cb.state  # Trigger transition to half_open
    cb.record_failure()
    assert cb._current_recovery_timeout == 50.0  # Capped at max (10*10=100, capped to 50)
    
    # Another failure shouldn't increase beyond cap
    cb._opened_at = time.monotonic() - 51.0
    cb.state
    cb.record_failure()
    assert cb._current_recovery_timeout == 50.0


def test_circuit_breaker_success_resets_backoff():
    """Successful recovery should reset backoff to base timeout."""
    cb = CircuitBreaker(
        "test_conn",
        "test_dt",
        failure_threshold=1,
        recovery_timeout=10.0,
        backoff_multiplier=2.0,
    )
    
    # Open and fail in half_open
    cb.record_failure()
    cb._opened_at = time.monotonic() - 11.0
    cb.state
    cb.record_failure()
    assert cb._current_recovery_timeout == 20.0
    assert cb._open_count == 2
    
    # Now succeed from half_open
    cb._opened_at = time.monotonic() - 21.0
    cb.state
    cb.record_success()
    
    assert cb.state == CircuitState.closed
    assert cb._current_recovery_timeout == 10.0  # Reset to base
    assert cb._open_count == 0


def test_circuit_breaker_reset_clears_backoff():
    """Manual reset should clear backoff state."""
    cb = CircuitBreaker(
        "test_conn",
        "test_dt",
        failure_threshold=1,
        recovery_timeout=10.0,
        backoff_multiplier=2.0,
    )
    
    # Open and backoff
    cb.record_failure()
    cb._opened_at = time.monotonic() - 11.0
    cb.state
    cb.record_failure()
    assert cb._current_recovery_timeout == 20.0
    assert cb._open_count == 2
    
    # Manual reset
    cb.reset()
    
    assert cb.state == CircuitState.closed
    assert cb._current_recovery_timeout == 10.0
    assert cb._open_count == 0
