"""Schema migration helpers for connector version changes."""
from __future__ import annotations

import re

import psycopg
import structlog
from psycopg import sql as pgsql

logger = structlog.get_logger(__name__)

# Patterns to parse diff strings produced by LocalSchemaRegistry.diff_schemas()
_ADDED_RE = re.compile(r"added column '([^']+)'")
_REMOVED_RE = re.compile(r"removed column '([^']+)'")
_TYPE_CHANGED_RE = re.compile(r"column '([^']+)' type changed:")


async def apply_schema_migrations(
    conn: psycopg.AsyncConnection,
    table: str,
    diff: list[str],
    field_mappings: object = None,  # unused, kept for API symmetry
    prune: bool = False,
) -> list[str]:
    """Apply DDL changes derived from schema diff strings.

    Parameters
    ----------
    conn:
        Open async psycopg connection.
    table:
        Fully-qualified (or bare) table name.
    diff:
        List of human-readable diff strings from ``LocalSchemaRegistry.diff_schemas()``.
    field_mappings:
        Current field mappings (reserved for future use).
    prune:
        When True, drop columns that appear in *diff* as removed.

    Returns
    -------
    list[str]
        Executed DDL strings.
    """
    executed: list[str] = []
    table_id = _table_identifier(table)

    for change in diff:
        added_m = _ADDED_RE.search(change)
        removed_m = _REMOVED_RE.search(change)
        type_m = _TYPE_CHANGED_RE.search(change)

        if added_m:
            col = added_m.group(1)
            stmt = pgsql.SQL(
                "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} TEXT"
            ).format(tbl=table_id, col=pgsql.Identifier(col))
            await conn.execute(stmt)
            ddl_str = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS \"{col}\" TEXT"
            executed.append(ddl_str)
            logger.info("schema_migration_applied", table=table, ddl=ddl_str)

        elif removed_m:
            col = removed_m.group(1)
            if prune:
                stmt = pgsql.SQL(
                    "ALTER TABLE {tbl} DROP COLUMN IF EXISTS {col}"
                ).format(tbl=table_id, col=pgsql.Identifier(col))
                await conn.execute(stmt)
                ddl_str = f"ALTER TABLE {table} DROP COLUMN IF EXISTS \"{col}\""
                executed.append(ddl_str)
                logger.info("schema_migration_applied", table=table, ddl=ddl_str)
            else:
                logger.info(
                    "schema_migration_skipped_removed_column",
                    table=table,
                    column=col,
                    reason="prune_orphan_columns=False",
                )

        elif type_m:
            col = type_m.group(1)
            logger.warning(
                "schema_migration_type_change_skipped",
                table=table,
                column=col,
                reason="automatic type changes are too risky; update manually",
                change=change,
            )

    return executed


def _table_identifier(table: str) -> pgsql.Composed | pgsql.Identifier:
    if "." in table:
        schema, tbl = table.split(".", 1)
        return pgsql.Identifier(schema, tbl)
    return pgsql.Identifier(table)
