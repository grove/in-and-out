"""PostgreSQL schema management: table naming, DDL generation, table provisioning."""
from __future__ import annotations

import psycopg

# Current migration count (migrations 001–020); checked at startup (B7)
SCHEMA_VERSION: int = 22

# Schema contract mode: 'evolve' (default, create tables) or 'freeze' (assert only)
_schema_contract: str = "evolve"


def set_schema_contract(mode: str) -> None:
    """Set schema contract mode: 'evolve' or 'freeze'.

    In 'freeze' mode, ensure_source_table and friends will raise RuntimeError
    if the table does not already exist, rather than creating it.
    """
    global _schema_contract
    if mode not in ("evolve", "freeze"):
        raise ValueError(f"Invalid schema_contract: {mode!r}")
    _schema_contract = mode


def get_schema_contract() -> str:
    return _schema_contract


# Table naming convention per GOAL.md
def source_table_name(
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> str:
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
    if _schema_contract == "freeze":
        await _assert_table_exists(conn, dead_letter_table_name(tool, connector, datatype, namespace))
        return
    await conn.execute(dead_letter_table_ddl(tool, connector, datatype, namespace))


async def ensure_source_table(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> None:
    """Create the source table for a connector/datatype pair if it doesn't exist.

    In 'freeze' mode, asserts the table exists and raises RuntimeError if not.
    """
    table = source_table_name(connector, datatype, namespace)
    if _schema_contract == "freeze":
        await _assert_table_exists(conn, table)
        return
    await conn.execute(source_table_ddl(connector, datatype, namespace))
    # Ensure _lineage column exists on older tables (ALTER TABLE ... ADD COLUMN IF NOT EXISTS)
    await conn.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS _lineage JSONB"
    )


async def ensure_source_history_table(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> None:
    if _schema_contract == "freeze":
        await _assert_table_exists(conn, source_history_table_name(connector, datatype, namespace))
        return
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


async def _assert_table_exists(conn: psycopg.AsyncConnection, table: str) -> None:
    """Raise RuntimeError if the table does not exist (freeze mode)."""
    # Handle schema-qualified names
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "public", table
    result = await (await conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        [schema, name],
    )).fetchone()
    if result is None:
        raise RuntimeError(
            f"Table '{table}' does not exist and schema_contract='freeze'. "
            "The schema-manager must create it before ingest can run."
        )
