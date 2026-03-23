"""Watermark read/write helpers."""
from __future__ import annotations

from uuid import UUID

import psycopg


async def get_watermark(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
) -> str | None:
    row = await (await conn.execute(
        "SELECT watermark_value FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
        [connector, datatype],
    )).fetchone()
    return row[0] if row else None


async def set_watermark(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    watermark_type: str,
    watermark_value: str,
    run_id: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO inout_ops_watermark
            (connector, datatype, watermark_type, watermark_value, updated_at, updated_by_run_id)
        VALUES (%s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (connector, datatype)
        DO UPDATE SET
            watermark_type     = EXCLUDED.watermark_type,
            watermark_value    = EXCLUDED.watermark_value,
            updated_at         = EXCLUDED.updated_at,
            updated_by_run_id  = EXCLUDED.updated_by_run_id
        """,
        [connector, datatype, watermark_type, watermark_value, run_id],
    )
