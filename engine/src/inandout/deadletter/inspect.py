"""Dead-letter queue inspection helpers."""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


async def fetch_dead_letter_rows(
    pool: Any,
    connector: str,
    datatype: str,
    limit: int = 20,
) -> list[dict]:
    """Fetch rows from the dead-letter table for a given connector/datatype.

    Args:
        pool: AsyncConnectionPool
        connector: Connector name.
        datatype: Datatype name.
        limit: Maximum number of rows to return.

    Returns:
        List of dicts with keys: id, external_id, raw, error_message,
        error_class, failed_at, requeue_count.
    """
    from inandout.postgres.schema import dead_letter_table_name

    table = dead_letter_table_name("ingestion", connector, datatype)

    try:
        async with pool.connection() as conn:
            rows = await (await conn.execute(
                f"""
                SELECT id, external_id, raw, error_message, error_class,
                       failed_at, requeue_count
                FROM {table}
                ORDER BY failed_at DESC
                LIMIT %s
                """,
                [limit],
            )).fetchall()

        return [
            {
                "id": r[0],
                "external_id": r[1],
                "raw": r[2],
                "error_message": r[3],
                "error_class": r[4],
                "failed_at": r[5],
                "requeue_count": r[6],
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning("dead_letter_fetch_error", connector=connector, datatype=datatype, error=str(exc))
        return []
