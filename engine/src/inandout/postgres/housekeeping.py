"""Periodic housekeeping: purge old rows from operational and history tables."""
from __future__ import annotations

import structlog
from psycopg_pool import AsyncConnectionPool

from inandout.config._duration import parse_duration
from inandout.config.tool import HousekeepingConfig

logger = structlog.get_logger(__name__)


def _to_pg_interval(duration_str: str) -> str:
    """Convert a duration string like "90d" to a PostgreSQL interval string like "90 days"."""
    secs = parse_duration(duration_str)
    days = int(secs / 86400)
    return f"{days} days"


async def run_housekeeping(
    pool: AsyncConnectionPool,
    housekeeping_cfg: HousekeepingConfig,
    connector_datatypes: list[tuple[str, str]],
) -> dict:
    """
    Delete rows older than configured retention windows.
    Returns counts of deleted rows per table.
    """
    retention = housekeeping_cfg.retention

    sync_run_interval = _to_pg_interval(retention.sync_run_log)
    dead_letter_interval = _to_pg_interval(retention.dead_letter)
    history_interval = _to_pg_interval(retention.history_table)
    webhook_seq_interval = _to_pg_interval(getattr(retention, "webhook_route_seq", "7d"))
    writeback_result_interval = _to_pg_interval(getattr(retention, "writeback_result", "30d"))
    writeback_dl_interval = _to_pg_interval(getattr(retention, "writeback_dead_letter", "30d"))
    _ds_raw = getattr(retention, "desired_state_processed", "90d")
    desired_state_interval = _to_pg_interval(_ds_raw if isinstance(_ds_raw, str) else "90d")

    totals: dict[str, int] = {}

    async with pool.connection() as conn:
        # Purge sync run log
        cur = await conn.execute(
            f"DELETE FROM inout_ops_sync_run WHERE finished_at < NOW() - INTERVAL '{sync_run_interval}'"
        )
        totals["sync_run"] = cur.rowcount or 0

        # Purge webhook route-sequence table (rows grow unbounded without TTL)
        try:
            cur = await conn.execute(
                f"DELETE FROM inout_ops_webhook_route_seq WHERE updated_at < NOW() - INTERVAL '{webhook_seq_interval}'"
            )
            totals["webhook_route_seq"] = cur.rowcount or 0
        except Exception:
            pass  # Table may not exist on older DBs

        # Purge writeback audit/result rows, but only rows old enough that no
        # active crash-recovery dedup anchor can reference them.  We protect rows
        # written within the last 24 hours regardless of the configured window so
        # a process that crashes and restarts within a day can still deduplicate.
        try:
            cur = await conn.execute(
                f"""
                DELETE FROM inout_ops_writeback_result
                WHERE processed_at < NOW() - INTERVAL '{writeback_result_interval}'
                  AND processed_at < NOW() - INTERVAL '1 day'
                """
            )
            totals["writeback_result"] = cur.rowcount or 0
        except Exception:
            pass  # Table may not exist on older DBs

        # Purge dead-letter tables (ingestion)
        for connector, datatype in connector_datatypes:
            dl_table = f"inout_dl_ingestion_{connector}_{datatype}"
            try:
                cur = await conn.execute(
                    f"DELETE FROM {dl_table} WHERE failed_at < NOW() - INTERVAL '{dead_letter_interval}'"
                )
                totals[f"dl_{connector}_{datatype}"] = cur.rowcount or 0
            except Exception:
                pass  # Table may not exist

        # Purge history tables
        for connector, datatype in connector_datatypes:
            hist_table = f"inout_src_{connector}_{datatype}_history"
            try:
                cur = await conn.execute(
                    f"DELETE FROM {hist_table} WHERE _ingested_at < NOW() - INTERVAL '{history_interval}'"
                )
                totals[f"hist_{connector}_{datatype}"] = cur.rowcount or 0
            except Exception:
                pass  # Table may not exist

        # Purge writeback dead-letter tables
        for connector, datatype in connector_datatypes:
            wbdl_table = f"inout_dl_writeback_{connector}_{datatype}"
            try:
                cur = await conn.execute(
                    f"DELETE FROM {wbdl_table} WHERE failed_at < NOW() - INTERVAL '{writeback_dl_interval}'"
                )
                totals[f"wbdl_{connector}_{datatype}"] = cur.rowcount or 0
            except Exception:
                pass  # Table may not exist yet

        # Purge processed desired-state rows past retention — only rows that have been
        # successfully processed (_processed_at IS NOT NULL) so unprocessed rows are never touched.
        for connector, datatype in connector_datatypes:
            dst_table = f"inout_dst_{connector}_{datatype}"
            try:
                cur = await conn.execute(
                    f"""
                    DELETE FROM {dst_table}
                    WHERE _processed_at IS NOT NULL
                      AND _processed_at < NOW() - INTERVAL '{desired_state_interval}'
                    """
                )
                totals[f"dst_{connector}_{datatype}"] = cur.rowcount or 0
            except Exception:
                pass  # Table may not exist yet

        await conn.commit()

    logger.info("housekeeping_complete", totals=totals)
    return totals


async def purge_by_external_id(
    pool: AsyncConnectionPool,
    connector: str,
    datatype: str,
    external_id: str,
) -> dict:
    """
    GDPR-compliant targeted purge: delete all records for a specific external_id.
    
    Purges from:
    - Main datatype table (inout_{connector}_{datatype})
    - History table (inout_{connector}_{datatype}_history)
    - Last-written-state (inout_lwstate_{connector}_{datatype})
    - Desired-state tables (inout_dst_{connector}_{datatype})
    - Dead-letter table (inout_dead_letter)
    - Writeback result table (inout_writeback_result)
    
    Returns count of rows deleted per table.
    """
    totals: dict[str, int] = {}
    
    async with pool.connection() as conn:
        # Main table
        main_table = f"inout_{connector}_{datatype}"
        try:
            cur = await conn.execute(
                f"DELETE FROM {main_table} WHERE external_id = %s",
                [external_id],
            )
            totals["main"] = cur.rowcount or 0
        except Exception as exc:
            logger.warning("gdpr_purge_main_failed", table=main_table, error=str(exc))
            totals["main"] = 0
        
        # History table
        history_table = f"{main_table}_history"
        try:
            cur = await conn.execute(
                f"DELETE FROM {history_table} WHERE external_id = %s",
                [external_id],
            )
            totals["history"] = cur.rowcount or 0
        except Exception:
            totals["history"] = 0
        
        # Last-written-state
        lwstate_table = f"inout_lwstate_{connector}_{datatype}"
        try:
            cur = await conn.execute(
                f"DELETE FROM {lwstate_table} WHERE external_id = %s",
                [external_id],
            )
            totals["lwstate"] = cur.rowcount or 0
        except Exception:
            totals["lwstate"] = 0
        
        # Desired-state
        dst_table = f"inout_dst_{connector}_{datatype}"
        try:
            cur = await conn.execute(
                f"DELETE FROM {dst_table} WHERE external_id = %s",
                [external_id],
            )
            totals["desired_state"] = cur.rowcount or 0
        except Exception:
            totals["desired_state"] = 0
        
        # Dead-letter
        try:
            cur = await conn.execute(
                """
                DELETE FROM inout_dead_letter
                WHERE connector = %s AND datatype = %s AND external_id = %s
                """,
                [connector, datatype, external_id],
            )
            totals["dead_letter"] = cur.rowcount or 0
        except Exception:
            totals["dead_letter"] = 0
        
        # Writeback result
        try:
            cur = await conn.execute(
                """
                DELETE FROM inout_writeback_result
                WHERE connector = %s AND datatype = %s AND external_id = %s
                """,
                [connector, datatype, external_id],
            )
            totals["writeback_result"] = cur.rowcount or 0
        except Exception:
            totals["writeback_result"] = 0
        
        await conn.commit()
    
    logger.info(
        "gdpr_purge_complete",
        connector=connector,
        datatype=datatype,
        external_id=external_id,
        totals=totals,
    )
    return totals
