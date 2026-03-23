"""Per-(connector, datatype) circuit breaker with CLOSED → OPEN → HALF_OPEN states."""
from __future__ import annotations

import time
from enum import StrEnum

import structlog

logger = structlog.get_logger(__name__)


class CircuitState(StrEnum):
    closed = "closed"        # Normal operation
    open = "open"            # Failing: reject requests immediately
    half_open = "half_open"  # Probe: allow one request to test recovery


class CircuitBreaker:
    """Finite-state machine circuit breaker for a single (connector, datatype) pair.

    Thresholds:
    - failure_threshold: consecutive failures needed to trip CLOSED → OPEN (default 5)
    - recovery_timeout: seconds before OPEN → HALF_OPEN (default 60)

    Callers must:
    - Call `before_request()` and check the returned bool; False means reject fast.
    - Call `record_success()` or `record_failure()` after each attempt.
    """

    def __init__(
        self,
        connector: str,
        datatype: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> None:
        self.connector = connector
        self.datatype = datatype
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state = CircuitState.closed
        self._consecutive_failures = 0
        self._opened_at: float | None = None

        self._log = logger.bind(connector=connector, datatype=datatype)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        self._maybe_transition_to_half_open()
        return self._state

    def allow_request(self) -> bool:
        """Return True if the request should proceed; False to reject fast."""
        self._maybe_transition_to_half_open()
        if self._state == CircuitState.closed:
            return True
        if self._state == CircuitState.half_open:
            return True
        # OPEN
        return False

    def record_success(self) -> None:
        """Record a successful request; transitions HALF_OPEN → CLOSED."""
        self._consecutive_failures = 0
        if self._state != CircuitState.closed:
            self._log.info("circuit_breaker_closed", previous=self._state)
            self._state = CircuitState.closed
            self._opened_at = None
            self._emit_metric()

    def record_failure(self) -> None:
        """Record a failed request; may trip CLOSED → OPEN or keep OPEN."""
        self._consecutive_failures += 1
        if self._state == CircuitState.half_open:
            self._trip_open()
        elif self._state == CircuitState.closed:
            if self._consecutive_failures >= self.failure_threshold:
                self._trip_open()

    def reset(self) -> None:
        """Force the circuit breaker back to CLOSED state (used by control commands)."""
        previous = self._state
        self._state = CircuitState.closed
        self._consecutive_failures = 0
        self._opened_at = None
        self._log.info("circuit_breaker_reset", previous=previous)
        self._emit_metric()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _trip_open(self) -> None:
        self._state = CircuitState.open
        self._opened_at = time.monotonic()
        self._log.warning(
            "circuit_breaker_opened",
            failures=self._consecutive_failures,
            recovery_timeout=self.recovery_timeout,
        )
        self._emit_metric()

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state == CircuitState.open
            and self._opened_at is not None
            and time.monotonic() - self._opened_at >= self.recovery_timeout
        ):
            self._state = CircuitState.half_open
            self._log.info("circuit_breaker_half_open")
            self._emit_metric()

    def _emit_metric(self) -> None:
        try:
            from inandout.observability.metrics import circuit_breaker_state
            state_int = {"closed": 0, "half_open": 0.5, "open": 1}.get(self._state.value, 0)
            circuit_breaker_state.labels(
                connector=self.connector,
                datatype=self.datatype,
                namespace="public",
            ).set(state_int)
        except Exception:
            pass


# Module-level registry: (connector, datatype) → CircuitBreaker
_registry: dict[tuple[str, str], CircuitBreaker] = {}


def get_circuit_breaker(
    connector: str,
    datatype: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
) -> CircuitBreaker:
    """Return (creating if needed) the circuit breaker for a connector/datatype pair."""
    key = (connector, datatype)
    if key not in _registry:
        _registry[key] = CircuitBreaker(
            connector, datatype, failure_threshold, recovery_timeout
        )
    return _registry[key]


def reset_all() -> None:
    """Reset all circuit breakers — used in tests."""
    _registry.clear()
