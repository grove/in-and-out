"""Privacy purge utility (B6).

Implements GDPR-style right-to-erasure for a specific external_id across
all tables maintained by in-and-out.
"""
from __future__ import annotations

import structlog
from psycopg_pool import AsyncConnectionPool

from inandout.ingestion.privacy import PurgeResult

logger = structlog.get_logger(__name__)


async def purge_by_external_id(
    pool: AsyncConnectionPool,
    connector: str,
    datatype: str,
    external_id: str,
    namespace: str = "public",
) -> PurgeResult:
    """Purge all data for *external_id* across all in-and-out tables.

    Steps:
    1. Tombstone source table row (clear raw/data, set _deleted=TRUE)
    2. Delete history rows
    3. Delete lwstate rows
    4. Delete dead-letter rows
    5. Delete writeback result rows
    6. Delete webhook log rows

    Returns a PurgeResult with per-table row counts.
    """
    result = PurgeResult(
        connector=connector,
        datatype=datatype,
        external_id=external_id,
    )

    src_table = f"inout_src_{connector}_{datatype}"
    hist_table = f"inout_src_{connector}_{datatype}_history"
    lwstate_table = f"inout_dst_{connector}_{datatype}_lwstate"
    dl_table = f"inout_dl_ingestion_{connector}_{datatype}"

    if namespace and namespace != "public":
        src_table = f"{namespace}.{src_table}"
        hist_table = f"{namespace}.{hist_table}"
        lwstate_table = f"{namespace}.{lwstate_table}"
        dl_table = f"{namespace}.{dl_table}"

    log = logger.bind(
        connector=connector, datatype=datatype, external_id=external_id
    )

    async with pool.connection() as conn:
        async with conn.transaction():
            # 1. Tombstone source table row
            try:
                cur = await conn.execute(
                    f"""
                    UPDATE {src_table}
                    SET _deleted = TRUE,
                        _deleted_at = NOW(),
                        raw = '{{}}'::jsonb,
                        data = '{{}}'::jsonb
                    WHERE external_id = %s
                    """,
                    [external_id],
                )
                result.tables_purged["source"] = cur.rowcount or 0
            except Exception as exc:
                log.warning("purge_source_failed", error=str(exc))
                result.tables_purged["source"] = 0

            # 2. Delete history rows
            try:
                cur = await conn.execute(
                    f"DELETE FROM {hist_table} WHERE external_id = %s",
                    [external_id],
                )
                result.tables_purged["history"] = cur.rowcount or 0
            except Exception:
                result.tables_purged["history"] = 0

            # 3. Delete lwstate rows
            try:
                cur = await conn.execute(
                    f"DELETE FROM {lwstate_table} WHERE external_id = %s",
                    [external_id],
                )
                result.tables_purged["lwstate"] = cur.rowcount or 0
            except Exception:
                result.tables_purged["lwstate"] = 0

            # 4. Delete dead-letter rows
            try:
                cur = await conn.execute(
                    f"DELETE FROM {dl_table} WHERE external_id = %s",
                    [external_id],
                )
                result.tables_purged["dead_letter"] = cur.rowcount or 0
            except Exception:
                result.tables_purged["dead_letter"] = 0

            # 5. Delete writeback result rows
            try:
                cur = await conn.execute(
                    """
                    DELETE FROM inout_ops_writeback_result
                    WHERE connector = %s AND datatype = %s AND external_id = %s
                    """,
                    [connector, datatype, external_id],
                )
                result.tables_purged["writeback_result"] = cur.rowcount or 0
            except Exception:
                result.tables_purged["writeback_result"] = 0

            # 6. Delete webhook log rows
            try:
                cur = await conn.execute(
                    """
                    DELETE FROM inout_ops_webhook_log
                    WHERE connector = %s AND datatype = %s AND external_id = %s
                    """,
                    [connector, datatype, external_id],
                )
                result.tables_purged["webhook_log"] = cur.rowcount or 0
            except Exception:
                result.tables_purged["webhook_log"] = 0

    log.info("privacy_purge_complete", tables_purged=result.tables_purged)
    return result
