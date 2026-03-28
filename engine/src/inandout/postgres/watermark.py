"""Watermark read/write helpers."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import psycopg
from psycopg_pool import AsyncConnectionPool


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
    conn_or_pool: psycopg.AsyncConnection | AsyncConnectionPool | Any,
    connector: str,
    datatype: str,
    watermark_type: str,
    watermark_value: str,
    run_id: UUID,
    *,
    pool: AsyncConnectionPool | None = None,
) -> None:
    """Upsert the watermark.

    When `conn_or_pool` is an AsyncConnection (has an `execute` method),
    uses it directly (no acquire/release). When it is an AsyncConnectionPool,
    acquires a new connection. Pass `pool=pool` as keyword arg only if you
    want to use the pool separately while passing a conn as first arg — in
    that case the first arg is used.

    Backward compatible: existing callers pass a `conn` as first arg and it works.
    """
    # Determine whether the first arg is a connection or a pool
    sql = """
        INSERT INTO inout_ops_watermark
            (connector, datatype, watermark_type, watermark_value, updated_at, updated_by_run_id)
        VALUES (%s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (connector, datatype)
        DO UPDATE SET
            watermark_type     = EXCLUDED.watermark_type,
            watermark_value    = EXCLUDED.watermark_value,
            updated_at         = EXCLUDED.updated_at,
            updated_by_run_id  = EXCLUDED.updated_by_run_id
        """
    params = [connector, datatype, watermark_type, watermark_value, run_id]

    if hasattr(conn_or_pool, "execute"):
        # It's a connection — use directly
        await conn_or_pool.execute(sql, params)
    else:
        # It's a pool — acquire a connection
        async with conn_or_pool.connection() as acquired_conn:
            await acquired_conn.execute(sql, params)
            await acquired_conn.commit()
