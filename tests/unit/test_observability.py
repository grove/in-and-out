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
