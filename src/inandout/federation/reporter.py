"""Federation reporter — publishes instance health data to shared table."""
from __future__ import annotations

import os
import socket
from datetime import datetime, timezone
from typing import Any

import structlog

from inandout.observability.metrics import federation_routed_total

logger = structlog.get_logger(__name__)


def _default_instance_id() -> str:
    """Generate a unique instance ID based on hostname and PID."""
    return f"{socket.gethostname()}:{os.getpid()}"


class FederationReporter:
    """Reports connector health data to the inout_ops_federation table."""

    def __init__(
        self,
        pool: Any,
        instance_id: str,
        namespace: str = "public",
    ) -> None:
        self._pool = pool
        self.instance_id = instance_id
        self.namespace = namespace

    async def report(
        self,
        connector: str,
        datatype: str,
        health_score: float,
        last_sync_at: Any,
        circuit_state: str,
        dl_depth: int,
    ) -> None:
        """Upsert health data for this instance/connector/datatype into federation table.

        Parameters
        ----------
        connector:
            Connector name.
        datatype:
            Datatype name.
        health_score:
            Current composite health score (0.0-1.0).
        last_sync_at:
            Timestamp of the last sync (datetime or None).
        circuit_state:
            Circuit breaker state string.
        dl_depth:
            Current dead-letter queue depth.
        """
        try:
            async with self._pool.connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO inout_ops_federation
                        (instance_id, namespace, connector, datatype,
                         health_score, last_sync_at, circuit_breaker_state,
                         dead_letter_depth, reported_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (instance_id, connector, datatype) DO UPDATE SET
                        namespace             = EXCLUDED.namespace,
                        health_score          = EXCLUDED.health_score,
                        last_sync_at          = EXCLUDED.last_sync_at,
                        circuit_breaker_state = EXCLUDED.circuit_breaker_state,
                        dead_letter_depth     = EXCLUDED.dead_letter_depth,
                        reported_at           = NOW()
                    """,
                    [
                        self.instance_id,
                        self.namespace,
                        connector,
                        datatype,
                        health_score,
                        last_sync_at,
                        circuit_state,
                        dl_depth,
                    ],
                )
                await conn.commit()
            destination = f"{self.namespace}/{connector}/{datatype}"
            federation_routed_total.labels(
                connector=connector,
                datatype=datatype,
                destination=destination,
            ).inc()
        except Exception as exc:
            logger.warning(
                "federation_report_failed",
                instance_id=self.instance_id,
                connector=connector,
                datatype=datatype,
                error=str(exc),
            )

    async def cleanup_stale(self, max_age_secs: float = 300.0) -> int:
        """Delete stale federation rows older than *max_age_secs*.

        Returns
        -------
        int
            Number of rows deleted.
        """
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    """
                    DELETE FROM inout_ops_federation
                    WHERE reported_at < NOW() - INTERVAL '1 second' * %s
                    """,
                    [max_age_secs],
                )
                await conn.commit()
                # psycopg3: rowcount on DELETE
                deleted = cur.rowcount if cur.rowcount is not None else 0
                logger.info("federation_cleanup_stale", deleted=deleted, max_age_secs=max_age_secs)
                return deleted
        except Exception as exc:
            logger.warning("federation_cleanup_stale_failed", error=str(exc))
            return 0
