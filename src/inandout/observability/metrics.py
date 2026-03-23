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
    ["tool", "connector", "datatype", "operation", "namespace"],
)

# Sync lag
sync_lag_seconds: Gauge = _gauge(
    "inout_sync_lag_seconds",
    "Seconds since the last successful sync for this connector/datatype",
    ["tool", "connector", "datatype", "namespace"],
)

# HTTP errors
http_errors_total: Counter = _counter(
    "inout_http_errors_total",
    "Total HTTP errors by status code, connector, and datatype",
    ["connector", "datatype", "status_code", "namespace"],
)

# Circuit breaker state (0=closed, 1=open, 2=half_open)
circuit_breaker_state: Gauge = _gauge(
    "inout_circuit_breaker_state",
    "Circuit breaker state (0=closed 1=open 2=half_open)",
    ["connector", "datatype", "namespace"],
)

# Dead letter queue depth
dead_letter_depth: Gauge = _gauge(
    "inout_dead_letter_depth",
    "Number of unresolved dead-letter records",
    ["connector", "datatype", "namespace"],
)

# Connector health score (0.0–1.0)
connector_health_score: Gauge = _gauge(
    "inout_connector_health_score",
    "Composite health score for connector/datatype (0.0=unhealthy, 1.0=healthy)",
    ["connector", "datatype", "namespace"],
)

# SLA violation gauge (1=violated, 0=ok)
sync_sla_violated: Gauge = _gauge(
    "inout_sync_sla_violated",
    "Whether the sync SLA has been violated (1=violated, 0=ok)",
    ["connector", "datatype", "namespace"],
)

# Data quality violations counter
quality_violations_total: Counter = _counter(
    "inout_quality_violations_total",
    "Total data quality rule violations by connector, datatype, and rule",
    ["connector", "datatype", "rule", "namespace"],
)

# Three-way conflict detection counter
conflicts_detected_total: Counter = _counter(
    "inout_conflicts_detected_total",
    "Total three-way merge conflicts detected during writeback",
    ["connector", "datatype", "resolution", "namespace"],
)


# Intra-sync deduplication counter
intra_sync_duplicates_total: Counter = _counter(
    "inout_intra_sync_duplicates_total",
    "Total intra-sync duplicate records skipped (same external_id seen twice in same run)",
    ["connector", "datatype"],
)

# Replication slot lag gauge
replication_slot_lag_bytes: Gauge = _gauge(
    "inout_replication_slot_lag_bytes",
    "Replication slot lag in bytes",
    ["slot_name"],
)


# Soft-delete resurrection counter (A6/T1 #41)
records_resurrected_total: Counter = _counter(
    "inout_records_resurrected_total",
    "Total records resurrected from soft-delete tombstone state",
    ["table"],
)

# Source unavailability counter (A8)
source_unavailable_total: Counter = _counter(
    "inout_source_unavailable_total",
    "Total times a connector/datatype was marked source-unavailable after exhausting retries",
    ["connector", "datatype"],
)

# Pagination drift events counter (A2 T1 #38)
pagination_drift_events_total: Counter = _counter(
    "inout_pagination_drift_events_total",
    "Total pagination drift events detected during sync",
    ["connector", "datatype"],
)

# Federation fan-out routing counter
federation_routed_total: Counter = _counter(
    "inout_federation_routed_total",
    "Total health reports successfully fanned-out to the federation table per destination",
    ["connector", "datatype", "destination"],
)


def configure_metrics() -> None:
    """No-op for now — metrics are registered at import time."""
    pass
