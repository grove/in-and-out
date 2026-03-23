"""PostgreSQL schema management: table naming, DDL generation, table provisioning."""
from __future__ import annotations

import psycopg

# Current migration count (migrations 001–020); checked at startup (B7)
SCHEMA_VERSION: int = 22


# Table naming convention per GOAL.md
def source_table_name(
    connector: str,
    datatype: str,
    namespace: str = "public",
    shared_table: str | None = None,
) -> str:
    if shared_table:
        base = f"inout_src_{shared_table}"
    else:
        base = f"inout_src_{connector}_{datatype}"
    if namespace and namespace != "public":
        return f"{namespace}.{base}"
    return base


def source_history_table_name(connector: str, datatype: str, namespace: str = "public") -> str:
    base = f"inout_src_{connector}_{datatype}_history"
    if namespace and namespace != "public":
        return f"{namespace}.{base}"
    return base


def dead_letter_table_name(tool: str, connector: str, datatype: str, namespace: str = "public") -> str:
    base = f"inout_dl_{tool}_{connector}_{datatype}"
    if namespace and namespace != "public":
        return f"{namespace}.{base}"
    return base


def _schema_prefix_ddl(namespace: str) -> str:
    """Return a CREATE SCHEMA statement when namespace != 'public'."""
    if namespace and namespace != "public":
        return f"CREATE SCHEMA IF NOT EXISTS {namespace};\n"
    return ""


# DDL for per-datatype source table (created at runtime when a new connector is loaded)
def source_table_ddl(connector: str, datatype: str, namespace: str = "public") -> str:
    table = source_table_name(connector, datatype, namespace)
    prefix = _schema_prefix_ddl(namespace)
    return f"""{prefix}CREATE TABLE IF NOT EXISTS {table} (
    external_id TEXT NOT NULL,
    data        JSONB NOT NULL,
    raw         JSONB NOT NULL,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _sync_run_id    UUID,
    _raw_hash       TEXT NOT NULL,
    _deleted        BOOLEAN NOT NULL DEFAULT FALSE,
    _deleted_at     TIMESTAMPTZ,
    _schema_version INTEGER NOT NULL DEFAULT 1,
    _source_version TEXT,
    _last_written   JSONB,
    _lineage        JSONB,
    PRIMARY KEY (external_id)
);
CREATE INDEX IF NOT EXISTS {table.replace(".", "_")}_ingested_at_idx ON {table} (_ingested_at);""".strip()


def source_history_table_ddl(connector: str, datatype: str, namespace: str = "public") -> str:
    hist = source_history_table_name(connector, datatype, namespace)
    prefix = _schema_prefix_ddl(namespace)
    return f"""{prefix}CREATE TABLE IF NOT EXISTS {hist} (
    _history_id     BIGSERIAL PRIMARY KEY,
    external_id     TEXT NOT NULL,
    data            JSONB NOT NULL,
    raw             JSONB NOT NULL,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _sync_run_id    UUID,
    _raw_hash       TEXT NOT NULL,
    _deleted        BOOLEAN NOT NULL DEFAULT FALSE,
    _deleted_at     TIMESTAMPTZ,
    _schema_version INTEGER NOT NULL DEFAULT 1,
    _source_version TEXT
);
CREATE INDEX IF NOT EXISTS {hist.replace(".", "_")}_external_id_idx ON {hist} (external_id, _ingested_at DESC);""".strip()


def dead_letter_table_ddl(tool: str, connector: str, datatype: str, namespace: str = "public") -> str:
    table = dead_letter_table_name(tool, connector, datatype, namespace)
    prefix = _schema_prefix_ddl(namespace)
    return f"""{prefix}CREATE TABLE IF NOT EXISTS {table} (
    id              BIGSERIAL PRIMARY KEY,
    external_id     TEXT,
    raw             JSONB,
    error_message   TEXT NOT NULL,
    error_class     TEXT NOT NULL DEFAULT 'unknown',
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sync_run_id     UUID,
    requeued_at     TIMESTAMPTZ,
    requeue_count   INT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS {table.replace(".", "_")}_failed_at_idx ON {table} (failed_at DESC);""".strip()


async def ensure_dead_letter_table(
    conn: psycopg.AsyncConnection,
    tool: str,
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> None:
    await conn.execute(dead_letter_table_ddl(tool, connector, datatype, namespace))


async def ensure_source_table(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    namespace: str = "public",
    shared_table: str | None = None,
) -> None:
    """Create the source table for a connector/datatype pair if it doesn't exist."""
    table = source_table_name(connector, datatype, namespace, shared_table=shared_table)
    # For shared tables, the base DDL uses the shared table name
    if shared_table:
        base_ddl = source_table_ddl_for_name(table, namespace)
        await conn.execute(base_ddl)
    else:
        await conn.execute(source_table_ddl(connector, datatype, namespace))
    # Ensure _lineage column exists on older tables (ALTER TABLE ... ADD COLUMN IF NOT EXISTS)
    await conn.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS _lineage JSONB"
    )
    # For shared (fan-in) tables, add _connector column with default
    if shared_table:
        await conn.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
            f"_connector TEXT NOT NULL DEFAULT '{connector}'"
        )
        # Change the primary key to (external_id, _connector) if possible
        # We do this by creating a unique index if it doesn't exist yet
        idx_name = f"{table.replace('.', '_')}_fanin_pk_idx"
        await conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} "
            f"ON {table} (external_id, _connector)"
        )


def source_table_ddl_for_name(table: str, namespace: str = "public") -> str:
    """Generate CREATE TABLE DDL when the table name is already known."""
    prefix = _schema_prefix_ddl(namespace)
    return f"""{prefix}CREATE TABLE IF NOT EXISTS {table} (
    external_id TEXT NOT NULL,
    data        JSONB NOT NULL,
    raw         JSONB NOT NULL,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _sync_run_id    UUID,
    _raw_hash       TEXT NOT NULL,
    _deleted        BOOLEAN NOT NULL DEFAULT FALSE,
    _deleted_at     TIMESTAMPTZ,
    _schema_version INTEGER NOT NULL DEFAULT 1,
    _source_version TEXT,
    _last_written   JSONB,
    _lineage        JSONB,
    PRIMARY KEY (external_id)
);
CREATE INDEX IF NOT EXISTS {table.replace(".", "_")}_ingested_at_idx ON {table} (_ingested_at);""".strip()


async def ensure_source_history_table(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> None:
    await conn.execute(source_history_table_ddl(connector, datatype, namespace))


# Operational tables DDL (also used in the Alembic migration)
OPERATIONAL_TABLES_DDL = """
CREATE TABLE IF NOT EXISTS inout_ops_sync_run (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector       TEXT NOT NULL,
    datatype        TEXT NOT NULL,
    mode            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    records_fetched  INT NOT NULL DEFAULT 0,
    records_inserted INT NOT NULL DEFAULT 0,
    records_updated  INT NOT NULL DEFAULT 0,
    records_deleted  INT NOT NULL DEFAULT 0,
    records_errored  INT NOT NULL DEFAULT 0,
    error_message   TEXT,
    error_detail    JSONB,
    CONSTRAINT valid_status CHECK (status IN ('running', 'completed', 'failed', 'skipped', 'aborted'))
);

CREATE TABLE IF NOT EXISTS inout_ops_watermark (
    connector           TEXT NOT NULL,
    datatype            TEXT NOT NULL,
    watermark_type      TEXT NOT NULL,
    watermark_value     TEXT NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by_run_id   UUID REFERENCES inout_ops_sync_run(id),
    PRIMARY KEY (connector, datatype)
);

CREATE TABLE IF NOT EXISTS inout_ops_control (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_tool     TEXT,
    connector       TEXT,
    datatype        TEXT,
    command         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    payload         JSONB,
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    issued_by       TEXT,
    acknowledged_at TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    result          JSONB,
    CONSTRAINT valid_status CHECK (status IN ('pending', 'acknowledged', 'completed', 'failed'))
);
""".strip()
