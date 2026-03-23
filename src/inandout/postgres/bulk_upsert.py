"""Bulk upsert support for ingestion engine."""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

import orjson
import psycopg
import structlog
from psycopg import sql as pgsql

logger = structlog.get_logger(__name__)


def _compute_raw_hash(record: dict[str, Any]) -> str:
    return hashlib.sha256(
        orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


async def bulk_upsert_records(
    conn: psycopg.AsyncConnection,
    table: str,
    records: list[dict[str, Any]],
    primary_key: str,
    run_id: uuid.UUID,
) -> tuple[int, int]:
    """Bulk upsert a list of pre-mapped records into *table*.

    Parameters
    ----------
    conn:
        Open async psycopg connection.
    table:
        Fully-qualified table name, e.g. ``inout_src_hubspot_contacts``.
    records:
        List of dicts mapping column name → value.  Each record must contain
        at least the *primary_key* column.
    primary_key:
        Name of the primary-key column (single column only for bulk path).
    run_id:
        UUID of the current sync run.

    Returns
    -------
    tuple[int, int]
        (inserted, updated) counts.
    """
    if not records:
        return 0, 0

    # Compute hashes and augment records with system columns.
    augmented: list[dict[str, Any]] = []
    for rec in records:
        raw_hash = _compute_raw_hash(rec)
        aug = dict(rec)
        aug["_raw_hash"] = raw_hash
        aug["_run_id"] = str(run_id)
        augmented.append(aug)

    # Collect all column names (union across all records for schema flexibility).
    all_cols: list[str] = []
    seen_cols: set[str] = set()
    for aug in augmented:
        for col in aug:
            if col not in seen_cols:
                all_cols.append(col)
                seen_cols.add(col)

    # Add _ingested_at after the rest (will be set to NOW() via expression).
    # We'll handle it separately in the VALUES clause as a SQL expression.

    # -------------------------------------------------------------------
    # Fetch existing hashes for the primary-key values in this batch so
    # we can do no-op detection without an extra round-trip per row.
    # -------------------------------------------------------------------
    pk_values = [aug[primary_key] for aug in augmented if primary_key in aug]

    existing_hashes: dict[str, str] = {}
    if pk_values:
        pk_col_id = pgsql.Identifier(primary_key)
        table_id = _table_identifier(table)
        hash_col_id = pgsql.Identifier("_raw_hash")

        # Build IN (%s, %s, ...) query
        placeholders = pgsql.SQL(", ").join(pgsql.Placeholder() * len(pk_values))
        query = pgsql.SQL(
            "SELECT {pk}, {hash} FROM {tbl} WHERE {pk} IN ({phs})"
        ).format(
            pk=pk_col_id,
            hash=hash_col_id,
            tbl=table_id,
            phs=placeholders,
        )
        cur = await conn.execute(query, pk_values)
        rows = await cur.fetchall()
        for row in rows:
            existing_hashes[str(row[0])] = str(row[1])

    # -------------------------------------------------------------------
    # Separate records into inserts, updates, and no-ops.
    # -------------------------------------------------------------------
    to_insert: list[dict[str, Any]] = []
    to_update: list[dict[str, Any]] = []

    for aug in augmented:
        pk_val = str(aug.get(primary_key, ""))
        new_hash = aug["_raw_hash"]
        if pk_val not in existing_hashes:
            to_insert.append(aug)
        elif existing_hashes[pk_val] != new_hash:
            to_update.append(aug)
        # else: no-op (same hash)

    inserted = 0
    updated = 0

    # -------------------------------------------------------------------
    # INSERT new records
    # -------------------------------------------------------------------
    if to_insert:
        ins_cols = list(all_cols) + ["_ingested_at"]
        col_ids = pgsql.SQL(", ").join(pgsql.Identifier(c) for c in all_cols)
        # Build values rows
        value_rows = []
        params: list[Any] = []
        for aug in to_insert:
            row_placeholders = pgsql.SQL(", ").join(
                pgsql.Placeholder() * len(all_cols)
            )
            value_rows.append(
                pgsql.SQL("(") + row_placeholders + pgsql.SQL(", NOW())")
            )
            for col in all_cols:
                params.append(aug.get(col))

        values_clause = pgsql.SQL(", ").join(value_rows)
        ins_cols_with_at = pgsql.SQL(", ").join(
            [pgsql.Identifier(c) for c in all_cols] + [pgsql.Identifier("_ingested_at")]
        )
        pk_id = pgsql.Identifier(primary_key)

        stmt = pgsql.SQL(
            "INSERT INTO {tbl} ({cols}) VALUES {vals} "
            "ON CONFLICT ({pk}) DO NOTHING"
        ).format(
            tbl=_table_identifier(table),
            cols=ins_cols_with_at,
            vals=values_clause,
            pk=pk_id,
        )
        await conn.execute(stmt, params)
        inserted = len(to_insert)

    # -------------------------------------------------------------------
    # UPDATE changed records one-by-one (still safer than bulk UPDATE)
    # -------------------------------------------------------------------
    for aug in to_update:
        pk_val = aug[primary_key]
        set_pairs = pgsql.SQL(", ").join(
            pgsql.SQL("{col} = {ph}").format(
                col=pgsql.Identifier(col), ph=pgsql.Placeholder()
            )
            for col in all_cols
            if col != primary_key
        )
        set_pairs = pgsql.SQL("{pairs}, _ingested_at = NOW()").format(pairs=set_pairs)
        upd_params = [aug.get(col) for col in all_cols if col != primary_key]
        upd_params.append(pk_val)

        stmt = pgsql.SQL(
            "UPDATE {tbl} SET {sets} WHERE {pk} = {ph}"
        ).format(
            tbl=_table_identifier(table),
            sets=set_pairs,
            pk=pgsql.Identifier(primary_key),
            ph=pgsql.Placeholder(),
        )
        await conn.execute(stmt, upd_params)
        updated += 1

    logger.debug(
        "bulk_upsert_complete",
        table=table,
        inserted=inserted,
        updated=updated,
        noop=len(augmented) - inserted - updated,
    )
    return inserted, updated


def _table_identifier(table: str) -> pgsql.Composed | pgsql.Identifier:
    """Return a safe psycopg SQL identifier for the table name."""
    if "." in table:
        schema, tbl = table.split(".", 1)
        return pgsql.Identifier(schema, tbl)
    return pgsql.Identifier(table)
