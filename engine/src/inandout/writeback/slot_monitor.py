"""Replication slot health monitoring (T2 #32).

Polls pg_replication_slots for lag and triggers fallback to polling
mode when lag exceeds the configured threshold.
"""
from __future__ import annotations

from typing import Any, Callable

import anyio
import structlog


logger = structlog.get_logger(__name__)


async def get_slot_lag(
    pool: Any,
    slot_name: str,
) -> tuple[int, float] | None:
    """Query pg_replication_slots for the given slot's lag.

    Returns:
        (lag_bytes, lag_secs) or None if slot not found / error.
    """
    async with pool.connection() as conn:
        try:
            row = await (await conn.execute(
                """
                SELECT
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes,
                    EXTRACT(EPOCH FROM (NOW() - confirmed_flush_lsn_time))::FLOAT AS lag_secs
                FROM pg_replication_slots
                WHERE slot_name = %s
                """,
                [slot_name],
            )).fetchone()
        except Exception as exc:
            logger.warning("replication_slot_query_failed", slot_name=slot_name, error=str(exc))
            return None

    if row is None:
        logger.warning("replication_slot_not_found", slot_name=slot_name)
        return None

    lag_bytes = int(row[0]) if row[0] is not None else 0
    lag_secs = float(row[1]) if row[1] is not None else 0.0
    return lag_bytes, lag_secs


async def monitor_replication_slot(
    pool: Any,
    config: Any,  # ReplicationSlotConfig
    on_fallback: Callable[[], None],
    should_stop: Callable[[], bool] | None = None,
) -> None:
    """Poll the replication slot every poll_interval_secs.

    - If lag_bytes > warn_lag_bytes: log ERROR and update gauge
    - If lag_bytes > max_lag_bytes: call on_fallback() and log CRITICAL
    """
    from inandout.observability.metrics import replication_slot_lag_bytes as lag_gauge

    slot_name = config.slot_name
    if not slot_name:
        return

    while True:
        if should_stop is not None and should_stop():
            logger.info("slot_monitor_draining", slot_name=slot_name)
            break
        try:
            result = await get_slot_lag(pool, slot_name)
            if result is None:
                await anyio.sleep(config.poll_interval_secs)
                if should_stop is not None and should_stop():
                    break
                continue

            lag_bytes, lag_secs = result

            try:
                lag_gauge.labels(slot_name=slot_name).set(lag_bytes)
            except Exception:
                pass

            if lag_bytes > config.max_lag_bytes:
                logger.critical(
                    "replication_slot_lag_critical_fallback",
                    slot_name=slot_name,
                    lag_bytes=lag_bytes,
                    lag_secs=lag_secs,
                    max_lag_bytes=config.max_lag_bytes,
                )
                try:
                    on_fallback()
                except Exception:
                    pass
            elif lag_bytes > config.warn_lag_bytes:
                logger.error(
                    "replication_slot_lag_high",
                    slot_name=slot_name,
                    lag_bytes=lag_bytes,
                    lag_secs=lag_secs,
                    warn_lag_bytes=config.warn_lag_bytes,
                )
            else:
                logger.debug(
                    "replication_slot_lag_ok",
                    slot_name=slot_name,
                    lag_bytes=lag_bytes,
                    lag_secs=lag_secs,
                )
        except Exception as exc:
            logger.error("slot_monitor_error", slot_name=slot_name, error=str(exc))

        await anyio.sleep(config.poll_interval_secs)
        if should_stop is not None and should_stop():
            break
