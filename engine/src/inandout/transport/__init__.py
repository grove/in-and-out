"""Transport adapter interface + HTTP adapter implementation.

Separates orchestration logic (scheduling, checkpointing, error classification)
from transport-specific logic (HTTP request/response, pagination, auth injection).
The HTTP adapter is the first — and initially only — implementation.
"""
from inandout.transport.base import TransportAdapter
from inandout.transport.http import HttpTransportAdapter

__all__ = [
    "TransportAdapter",
    "HttpTransportAdapter",
]
