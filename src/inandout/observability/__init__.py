"""Observability setup: structured logging, Prometheus metrics, OpenTelemetry tracing."""
from inandout.observability.logging import configure_logging
from inandout.observability.metrics import configure_metrics, REGISTRY
from inandout.observability.tracing import configure_tracing

__all__ = ["configure_logging", "configure_metrics", "configure_tracing", "REGISTRY"]
