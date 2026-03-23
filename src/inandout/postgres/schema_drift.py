"""Schema drift detection and orphan column pruning."""
from __future__ import annotations

import structlog

import psycopg

logger = structlog.get_logger(__name__)


async def detect_schema_drift(
    conn: psycopg.AsyncConnection,
    table_name: str,
    observed_keys: set[str],
) -> list[str]:
    """Return columns present in the DB but absent from *observed_keys*.

    System columns (those whose names start with ``_``) are excluded from
    the comparison — they are managed by the ingestion engine itself.

    Parameters
    ----------
    conn:
        An open async psycopg connection.
    table_name:
        Fully-qualified (or unqualified) table name, e.g. ``inout_src_hub_contacts``
        or ``tenant_a.inout_src_hub_contacts``.
    observed_keys:
        Set of field names seen in the latest full sync.

    Returns
    -------
    list[str]
        Orphaned column names (exist in DB but not in *observed_keys*).
    """
    # Separate schema from table name if present.
    if "." in table_name:
        schema_part, table_part = table_name.split(".", 1)
    else:
        schema_part = "public"
        table_part = table_name

    cur = await conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name  = %s
        ORDER BY ordinal_position
        """,
        [schema_part, table_part],
    )
    rows = await cur.fetchall()
    db_columns = {row[0] for row in rows}

    orphans = [
        col
        for col in db_columns
        if not col.startswith("_") and col not in observed_keys
    ]
    return orphans


async def prune_orphan_columns(
    conn: psycopg.AsyncConnection,
    table_name: str,
    orphan_columns: list[str],
) -> int:
    """Drop *orphan_columns* from *table_name*.

    Parameters
    ----------
    conn:
        An open async psycopg connection.
    table_name:
        Fully-qualified table name.
    orphan_columns:
        Column names to drop.

    Returns
    -------
    int
        Number of columns actually dropped.
    """
    dropped = 0
    for col in orphan_columns:
        # Use psycopg's sql module to safely quote identifiers.
        from psycopg import sql as pgsql

        stmt = pgsql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {}").format(
            pgsql.Identifier(*table_name.split(".")) if "." in table_name
            else pgsql.Identifier(table_name),
            pgsql.Identifier(col),
        )
        await conn.execute(stmt)
        logger.info("orphan_column_dropped", table=table_name, column=col)
        dropped += 1
    return dropped
