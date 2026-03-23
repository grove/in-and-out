"""Fan-in join: enrich writeback rows from other source tables."""
from __future__ import annotations

from typing import Any

import structlog

from inandout.config.writeback import JoinSource
from inandout.postgres.schema import source_table_name

logger = structlog.get_logger(__name__)


async def enrich_with_join_sources(
    pool: Any,
    row: dict[str, Any],
    join_sources: list[JoinSource],
    namespace: str = "public",
) -> dict[str, Any]:
    """Enrich a delta row with fields from join source tables.

    For each JoinSource:
    - Looks up the source table for connector/datatype
    - Fetches the specified fields where external_id = row[join_key]
    - Merges those fields into the row (does NOT overwrite primary row fields)

    Returns the enriched row dict.
    """
    enriched = dict(row)

    for join_src in join_sources:
        join_key = join_src.join_key
        join_val = row.get(join_key)
        if join_val is None:
            logger.warning(
                "fan_in_missing_join_key",
                connector=join_src.connector,
                datatype=join_src.datatype,
                join_key=join_key,
            )
            continue

        table = source_table_name(join_src.connector, join_src.datatype, namespace)
        fields_sql = ", ".join(f"data->'{f}' AS {f}" for f in join_src.fields)
        query = f"SELECT {fields_sql} FROM {table} WHERE external_id = %s LIMIT 1"

        try:
            async with pool.connection() as conn:
                cur = await conn.execute(query, [str(join_val)])
                col_names = [desc[0] for desc in cur.description or []]
                src_row = await cur.fetchone()
        except Exception as exc:
            logger.warning(
                "fan_in_query_failed",
                connector=join_src.connector,
                datatype=join_src.datatype,
                error=str(exc),
            )
            continue

        if src_row is None:
            logger.warning(
                "fan_in_row_not_found",
                connector=join_src.connector,
                datatype=join_src.datatype,
                join_key=join_key,
                join_val=join_val,
            )
            continue

        src_data = dict(zip(col_names, src_row))
        # Merge: join source fields do NOT overwrite primary fields
        for k, v in src_data.items():
            if k not in enriched:
                enriched[k] = v

    return enriched
