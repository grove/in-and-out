"""Federation package — multi-instance coordination via inout_ops_federation.

Each running daemon instance periodically writes a heartbeat row to
``inout_ops_federation`` that records which connector/datatype pairs it owns,
their health state, and when it last reported. Stale rows (no heartbeat for
``stale_after`` seconds) indicate dead instances and can be queried by the
readiness endpoint or by operators.
"""
from __future__ import annotations

from inandout.federation.heartbeat import FederationHeartbeat, report_heartbeat
from inandout.federation.registry import FederationRegistry, InstanceStatus

__all__ = [
    "FederationHeartbeat",
    "report_heartbeat",
    "FederationRegistry",
    "InstanceStatus",
]
