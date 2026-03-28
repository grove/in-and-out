"""Unit tests for observability setup."""
from __future__ import annotations

import pytest
from prometheus_client import Counter, Gauge


def test_configure_logging_json_info():
    from inandout.observability.logging import configure_logging
    configure_logging("json", "info")  # should not raise


def test_configure_logging_text_debug():
    from inandout.observability.logging import configure_logging
    configure_logging("text", "debug")  # should not raise


def test_configure_tracing_disabled():
    from inandout.observability.tracing import configure_tracing
    configure_tracing(enabled=False)  # should not raise


def test_configure_tracing_enabled_no_endpoint():
    from inandout.observability.tracing import configure_tracing
    configure_tracing(enabled=True)  # should not raise — no OTLP export


def test_records_processed_total_is_counter():
    from inandout.observability.metrics import records_processed_total
    assert isinstance(records_processed_total, Counter)


def test_http_errors_total_is_counter():
    from inandout.observability.metrics import http_errors_total
    assert isinstance(http_errors_total, Counter)


def test_sync_lag_seconds_is_gauge():
    from inandout.observability.metrics import sync_lag_seconds
    assert isinstance(sync_lag_seconds, Gauge)


def test_circuit_breaker_state_is_gauge():
    from inandout.observability.metrics import circuit_breaker_state
    assert isinstance(circuit_breaker_state, Gauge)


def test_records_processed_labels():
    from inandout.observability.metrics import records_processed_total
    # Should be labelable with required labels (including namespace)
    counter = records_processed_total.labels(
        tool="ingestion",
        connector="test",
        datatype="contacts",
        operation="insert",
        namespace="public",
    )
    counter.inc()  # should not raise


def test_http_errors_labels():
    from inandout.observability.metrics import http_errors_total
    counter = http_errors_total.labels(
        connector="test",
        datatype="contacts",
        status_code="503",
        namespace="public",
    )
    counter.inc()  # should not raise


def test_prometheus_generate_latest_contains_all_custom_metric_names():
    """All custom metrics must appear by name in generate_latest(REGISTRY) output."""
    from prometheus_client import generate_latest
    from inandout.observability.metrics import REGISTRY

    output = generate_latest(REGISTRY).decode()

    expected = [
        "inout_records_processed_total",
        "inout_sync_lag_seconds",
        "inout_http_errors_total",
        "inout_circuit_breaker_state",
        "inout_dead_letter_depth",
        "inout_connector_health_score",
        "inout_sync_sla_violated",
        "inout_quality_violations_total",
        "inout_conflicts_detected_total",
        "inout_intra_sync_duplicates_total",
        "inout_replication_slot_lag_bytes",
        "inout_records_resurrected_total",
        "inout_source_unavailable_total",
        "inout_pagination_drift_events_total",
        "inout_federation_routed_total",
    ]

    missing = [name for name in expected if name not in output]
    assert not missing, f"Metrics absent from Prometheus /metrics output: {missing}"
