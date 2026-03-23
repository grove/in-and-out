"""Prometheus metrics registry and metric definitions."""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()


def _counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    try:
        return Counter(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        # Already registered — return existing
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


def _gauge(name: str, documentation: str, labelnames: list[str]) -> Gauge:
    try:
        return Gauge(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


# Records processed
records_processed_total: Counter = _counter(
    "inout_records_processed_total",
    "Total records processed by operation",
    ["tool", "connector", "datatype", "operation"],
)

# Sync lag
sync_lag_seconds: Gauge = _gauge(
    "inout_sync_lag_seconds",
    "Seconds since the last successful sync for this connector/datatype",
    ["tool", "connector", "datatype"],
)

# HTTP errors
http_errors_total: Counter = _counter(
    "inout_http_errors_total",
    "Total HTTP errors by status code, connector, and datatype",
    ["connector", "datatype", "status_code"],
)

# Circuit breaker state (0=closed, 1=open, 2=half_open)
circuit_breaker_state: Gauge = _gauge(
    "inout_circuit_breaker_state",
    "Circuit breaker state (0=closed 1=open 2=half_open)",
    ["connector", "datatype"],
)

# Dead letter queue depth
dead_letter_depth: Gauge = _gauge(
    "inout_dead_letter_depth",
    "Number of unresolved dead-letter records",
    ["connector", "datatype"],
)


def configure_metrics() -> None:
    """No-op for now — metrics are registered at import time."""
    pass
