"""Federation heartbeat — each daemon instance records its presence.

A background loop calls ``report_heartbeat`` on a configurable interval
(default 30 s).  The row schema matches ``inout_ops_federation`` as created
by migration 007.
"""
from __future__ import annotations

import asyncio
import socket
import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog

from inandout.observability.metrics import federation_heartbeat_failures_total

logger = structlog.get_logger(__name__)

# Stable per-process instance_id. Generated once at import time so that all
# calls from one process share the same identifier.
_INSTANCE_ID: str = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def get_instance_id() -> str:
    """Return the stable instance identifier for the current process."""
    return _INSTANCE_ID


@dataclass
class ConnectorDataypeHealth:
    connector: str
    datatype: str
    health_score: float = 1.0           # 1.0 = healthy, 0.0 = dead
    last_sync_at: str | None = None     # ISO-8601 UTC string or None
    circuit_breaker_state: str = "closed"
    dead_letter_depth: int = 0


@dataclass
class FederationHeartbeat:
    """Accumulates per-(connector, datatype) health snapshots to report."""

    namespace: str = "public"
    _slots: dict[tuple[str, str], ConnectorDataypeHealth] = field(
        default_factory=dict, repr=False
    )

    def update(
        self,
        connector: str,
        datatype: str,
        health_score: float = 1.0,
        last_sync_at: str | None = None,
        circuit_breaker_state: str = "closed",
        dead_letter_depth: int = 0,
    ) -> None:
        """Update or create a health snapshot for (connector, datatype)."""
        self._slots[(connector, datatype)] = ConnectorDataypeHealth(
            connector=connector,
            datatype=datatype,
            health_score=health_score,
            last_sync_at=last_sync_at,
            circuit_breaker_state=circuit_breaker_state,
            dead_letter_depth=dead_letter_depth,
        )

    def snapshots(self) -> list[ConnectorDataypeHealth]:
        return list(self._slots.values())


async def report_heartbeat(
    pool: Any,
    heartbeat: FederationHeartbeat,
    instance_id: str | None = None,
) -> int:
    """Upsert all heartbeat rows into inout_ops_federation.

    Returns the number of rows written.
    """
    iid = instance_id or _INSTANCE_ID
    snapshots = heartbeat.snapshots()
    if not snapshots:
        return 0

    written = 0
    try:
        async with pool.connection() as conn:
            for snap in snapshots:
                await conn.execute(
                    """
                    INSERT INTO inout_ops_federation
                        (instance_id, namespace, connector, datatype,
                         health_score, last_sync_at, circuit_breaker_state,
                         dead_letter_depth, reported_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (instance_id, connector, datatype) DO UPDATE
                    SET namespace             = EXCLUDED.namespace,
                        health_score          = EXCLUDED.health_score,
                        last_sync_at          = EXCLUDED.last_sync_at,
                        circuit_breaker_state = EXCLUDED.circuit_breaker_state,
                        dead_letter_depth     = EXCLUDED.dead_letter_depth,
                        reported_at           = NOW()
                    """,
                    [
                        iid,
                        heartbeat.namespace,
                        snap.connector,
                        snap.datatype,
                        snap.health_score,
                        snap.last_sync_at,
                        snap.circuit_breaker_state,
                        snap.dead_letter_depth,
                    ],
                )
                written += 1
            await conn.commit()
    except Exception as exc:
        # Heartbeat failure must never crash the daemon
        logger.warning("federation_heartbeat_failed", error=str(exc))
        federation_heartbeat_failures_total.inc()

    return written


async def heartbeat_loop(
    pool: Any,
    heartbeat: FederationHeartbeat,
    interval_secs: float = 30.0,
    should_stop: Any = None,
    instance_id: str | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Background coroutine: call report_heartbeat every interval_secs.

    Parameters
    ----------
    pool:
        asyncpg connection pool.
    heartbeat:
        Shared FederationHeartbeat object updated by the daemon's sync loops.
    interval_secs:
        How often to write heartbeat rows. Default 30 s.
    should_stop:
        Optional callable() → bool; when True the loop exits cleanly.
    instance_id:
        Override the process-level instance_id (useful in tests).
    stop_event:
        Optional asyncio.Event; when set the sleep wakes immediately so the
        loop exits without waiting for the full interval_secs.
    """
    log = logger.bind(instance_id=instance_id or _INSTANCE_ID)
    log.info("federation_heartbeat_loop_started", interval_secs=interval_secs)
    while True:
        if should_stop and should_stop():
            log.info("federation_heartbeat_loop_stopping")
            break
        try:
            if stop_event is not None:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_secs)
            else:
                await asyncio.sleep(interval_secs)
        except asyncio.TimeoutError:
            pass
        if should_stop and should_stop():
            break
        await report_heartbeat(pool, heartbeat, instance_id=instance_id)
