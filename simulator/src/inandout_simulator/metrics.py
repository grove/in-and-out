"""Prometheus metrics registry and metric definitions for the simulator."""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge

REGISTRY = CollectorRegistry()


def _counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    try:
        return Counter(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


def _gauge(name: str, documentation: str, labelnames: list[str]) -> Gauge:
    try:
        return Gauge(name, documentation, labelnames, registry=REGISTRY)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[return-value]


# API requests served (list / detail / write) by the mock connector API
sim_api_requests_total: Counter = _counter(
    "sim_api_requests_total",
    "Total mock-API requests handled by the simulator",
    ["connector", "datatype", "method"],
)

# Webhook events dispatched outbound to the engine
sim_webhooks_dispatched_total: Counter = _counter(
    "sim_webhooks_dispatched_total",
    "Total outbound webhook events dispatched by the simulator",
    ["connector", "datatype", "operation"],
)

# Webhook dispatch failures (non-2xx or network error)
sim_webhook_errors_total: Counter = _counter(
    "sim_webhook_errors_total",
    "Total outbound webhook dispatch failures",
    ["connector", "datatype", "operation"],
)

# Record counts held in the store (snapshot gauge, sampled per connector/datatype)
sim_records_stored: Gauge = _gauge(
    "sim_records_stored",
    "Number of records currently held in the simulator store",
    ["connector", "datatype"],
)
