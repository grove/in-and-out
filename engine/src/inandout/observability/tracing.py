"""OpenTelemetry tracing configuration."""
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased


def configure_tracing(
    enabled: bool = False,
    otlp_endpoint: str | None = None,
    sample_rate: float = 1.0,
    service_name: str = "inandout",
) -> None:
    """Configure OpenTelemetry tracing."""
    if not enabled:
        trace.set_tracer_provider(trace.NoOpTracerProvider())
        return

    resource = Resource.create({"service.name": service_name})
    sampler = TraceIdRatioBased(sample_rate)
    provider = TracerProvider(resource=resource, sampler=sampler)

    if otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)
