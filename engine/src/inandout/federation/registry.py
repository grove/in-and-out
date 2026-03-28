"""Federation registry — query active instances from inout_ops_federation.

Used by readiness endpoints and operator tools to inspect which daemon
instances are alive and which connectors they own.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# An instance is considered alive if it has reported within this many seconds.
DEFAULT_STALE_AFTER_SECS: float = 90.0


@dataclass
class InstanceStatus:
    instance_id: str
    namespace: str
    connector: str
    datatype: str
    health_score: float
    last_sync_at: str | None
    circuit_breaker_state: str
    dead_letter_depth: int
    reported_at: datetime
    is_alive: bool


class FederationRegistry:
    """Query helper for the inout_ops_federation table."""

    def __init__(self, pool: Any, stale_after_secs: float = DEFAULT_STALE_AFTER_SECS) -> None:
        self._pool = pool
        self._stale_after_secs = stale_after_secs

    async def list_instances(
        self,
        connector: str | None = None,
        datatype: str | None = None,
        alive_only: bool = False,
    ) -> list[InstanceStatus]:
        """Return all (or filtered) instance rows from inout_ops_federation."""
        now = datetime.now(timezone.utc)
        conditions: list[str] = []
        params: list[Any] = []

        if alive_only:
            conditions.append(
                f"reported_at > NOW() - INTERVAL '{int(self._stale_after_secs)} seconds'"
            )
        if connector:
            conditions.append("connector = %s")
            params.append(connector)
        if datatype:
            conditions.append("datatype = %s")
            params.append(datatype)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"""
            SELECT instance_id, namespace, connector, datatype,
                   health_score, last_sync_at, circuit_breaker_state,
                   dead_letter_depth, reported_at
            FROM inout_ops_federation
            {where}
            ORDER BY reported_at DESC
        """
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(sql, params)
                rows = await cur.fetchall()
                col_names = [d[0] for d in (cur.description or [])]
        except Exception as exc:
            logger.warning("federation_registry_query_failed", error=str(exc))
            return []

        results: list[InstanceStatus] = []
        for row in rows:
            d = dict(zip(col_names, row))
            reported_at: datetime = d["reported_at"]
            if reported_at.tzinfo is None:
                reported_at = reported_at.replace(tzinfo=timezone.utc)
            age_secs = (now - reported_at).total_seconds()
            results.append(
                InstanceStatus(
                    instance_id=d["instance_id"],
                    namespace=d.get("namespace", "public"),
                    connector=d["connector"],
                    datatype=d["datatype"],
                    health_score=float(d.get("health_score") or 1.0),
                    last_sync_at=str(d["last_sync_at"]) if d.get("last_sync_at") else None,
                    circuit_breaker_state=d.get("circuit_breaker_state") or "closed",
                    dead_letter_depth=int(d.get("dead_letter_depth") or 0),
                    reported_at=reported_at,
                    is_alive=age_secs <= self._stale_after_secs,
                )
            )
        return results

    async def list_alive_instances(
        self,
        connector: str | None = None,
        datatype: str | None = None,
    ) -> list[InstanceStatus]:
        """Convenience: return only recently-reported (alive) instances."""
        return await self.list_instances(
            connector=connector, datatype=datatype, alive_only=True
        )

    async def get_instance(self, instance_id: str) -> list[InstanceStatus]:
        """Return all rows for a specific instance_id."""
        now = datetime.now(timezone.utc)
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    """
                    SELECT instance_id, namespace, connector, datatype,
                           health_score, last_sync_at, circuit_breaker_state,
                           dead_letter_depth, reported_at
                    FROM inout_ops_federation
                    WHERE instance_id = %s
                    ORDER BY connector, datatype
                    """,
                    [instance_id],
                )
                rows = await cur.fetchall()
                col_names = [d[0] for d in (cur.description or [])]
        except Exception as exc:
            logger.warning("federation_registry_get_instance_failed", error=str(exc))
            return []

        results: list[InstanceStatus] = []
        for row in rows:
            d = dict(zip(col_names, row))
            reported_at: datetime = d["reported_at"]
            if reported_at.tzinfo is None:
                reported_at = reported_at.replace(tzinfo=timezone.utc)
            age_secs = (now - reported_at).total_seconds()
            results.append(
                InstanceStatus(
                    instance_id=d["instance_id"],
                    namespace=d.get("namespace", "public"),
                    connector=d["connector"],
                    datatype=d["datatype"],
                    health_score=float(d.get("health_score") or 1.0),
                    last_sync_at=str(d["last_sync_at"]) if d.get("last_sync_at") else None,
                    circuit_breaker_state=d.get("circuit_breaker_state") or "closed",
                    dead_letter_depth=int(d.get("dead_letter_depth") or 0),
                    reported_at=reported_at,
                    is_alive=age_secs <= self._stale_after_secs,
                )
            )
        return results

    async def evict_stale(self, older_than_secs: float | None = None) -> int:
        """Delete rows for instances that haven't reported within the stale window.

        Returns the count of rows deleted.
        """
        threshold = older_than_secs or (self._stale_after_secs * 3)
        try:
            async with self._pool.connection() as conn:
                cur = await conn.execute(
                    f"DELETE FROM inout_ops_federation "
                    f"WHERE reported_at < NOW() - INTERVAL '{int(threshold)} seconds'"
                )
                deleted = cur.rowcount or 0
                await conn.commit()
            if deleted:
                logger.info("federation_evicted_stale_instances", count=deleted)
            return deleted
        except Exception as exc:
            logger.warning("federation_evict_failed", error=str(exc))
            return 0
