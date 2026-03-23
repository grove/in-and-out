"""SLA / freshness monitoring for connector syncs."""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


async def check_sla(
    pool: Any,
    connector: str,
    datatype: str,
    max_lag_seconds: int,
) -> bool:
    """Check whether the sync SLA is violated for a connector/datatype.

    Returns True if violated (lag > max_lag_seconds or no sync run found),
    False if the SLA is met.

    Side effects:
    - Sets the ``inout_sync_sla_violated`` Prometheus gauge to 1 (violated) or 0 (ok).
    - Logs a structured ``sync_sla_violated`` warning when violated.
    """
    from inandout.observability.metrics import sync_sla_violated as _sla_gauge

    violated = True
    try:
        async with pool.connection() as conn:
            row = await (await conn.execute(
                """
                SELECT finished_at
                FROM inout_ops_sync_run
                WHERE connector = %s AND datatype = %s AND status = 'completed'
                ORDER BY finished_at DESC
                LIMIT 1
                """,
                [connector, datatype],
            )).fetchone()

        if row and row[0] is not None:
            import datetime
            finished_at = row[0]
            # psycopg3 returns timezone-aware datetimes for TIMESTAMPTZ
            if finished_at.tzinfo is None:
                finished_at = finished_at.replace(tzinfo=datetime.timezone.utc)
            now = datetime.datetime.now(datetime.timezone.utc)
            lag = (now - finished_at).total_seconds()
            violated = lag > max_lag_seconds
        else:
            # No completed run found — SLA is violated
            violated = True
    except Exception as exc:
        logger.warning("check_sla_db_error", connector=connector, datatype=datatype, error=str(exc))
        violated = True

    gauge_val = 1 if violated else 0
    try:
        _sla_gauge.labels(connector=connector, datatype=datatype).set(gauge_val)
    except Exception:
        pass

    if violated:
        logger.warning(
            "sync_sla_violated",
            connector=connector,
            datatype=datatype,
            max_lag_seconds=max_lag_seconds,
        )

    return violated


async def check_all_slas(pool: Any, connector_configs: list) -> dict[tuple[str, str], bool]:
    """Check SLA for all connector/datatype pairs that have max_lag_seconds configured.

    Returns a dict mapping (connector, datatype) → violated (bool).
    """
    results: dict[tuple[str, str], bool] = {}
    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
            if dtype_cfg.ingestion is None:
                continue
            schedule = dtype_cfg.ingestion.schedule
            if schedule.max_lag_seconds is None:
                continue
            violated = await check_sla(
                pool,
                connector_cfg.name,
                dtype_name,
                schedule.max_lag_seconds,
            )
            results[(connector_cfg.name, dtype_name)] = violated
    return results
