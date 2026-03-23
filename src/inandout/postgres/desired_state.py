"""Desired-state table helpers for writeback (inout_dst_* and inout_dst_*_lwstate).

Per GOAL.md:
  inout_dst_{connector}_{datatype}      — desired state rows (what OSI-Mapping wants written)
  inout_dst_{connector}_{datatype}_lwstate — last-written state (what was last written to target)
"""
from __future__ import annotations

import psycopg


# ---------------------------------------------------------------------------
# Table naming
# ---------------------------------------------------------------------------


def desired_state_table_name(connector: str, datatype: str, namespace: str = "public") -> str:
    base = f"inout_dst_{connector}_{datatype}"
    if namespace and namespace != "public":
        return f"{namespace}.{base}"
    return base


def lwstate_table_name(connector: str, datatype: str, namespace: str = "public") -> str:
    base = f"inout_dst_{connector}_{datatype}_lwstate"
    if namespace and namespace != "public":
        return f"{namespace}.{base}"
    return base


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def desired_state_table_ddl(connector: str, datatype: str, namespace: str = "public") -> str:
    """DDL for the desired-state table.

    Each row represents one entity that OSI-Mapping wants to be present in
    the target system. The ``_action`` column carries the intended operation
    (insert / update / delete).
    """
    table = desired_state_table_name(connector, datatype, namespace)
    schema_prefix = f"CREATE SCHEMA IF NOT EXISTS {namespace};\n" if namespace and namespace != "public" else ""
    return f"""{schema_prefix}CREATE TABLE IF NOT EXISTS {table} (
    external_id     TEXT NOT NULL PRIMARY KEY,
    data            JSONB NOT NULL,
    _action         TEXT NOT NULL DEFAULT 'update',
    _schema_version INTEGER NOT NULL DEFAULT 1,
    _created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _sync_run_id    UUID,
    CONSTRAINT valid_action CHECK (_action IN ('insert', 'update', 'delete', 'upsert'))
);
CREATE INDEX IF NOT EXISTS {table.replace(".", "_")}_updated_at_idx
    ON {table} (_updated_at DESC);""".strip()


def lwstate_table_ddl(connector: str, datatype: str, namespace: str = "public") -> str:
    """DDL for the last-written-state table.

    Mirrors the desired-state table structure but stores the state that was
    last successfully written to the target API.  Used for three-way conflict
    detection: base (lwstate) vs local (desired_state) vs remote (API GET).
    """
    table = lwstate_table_name(connector, datatype, namespace)
    schema_prefix = f"CREATE SCHEMA IF NOT EXISTS {namespace};\n" if namespace and namespace != "public" else ""
    return f"""{schema_prefix}CREATE TABLE IF NOT EXISTS {table} (
    external_id     TEXT NOT NULL PRIMARY KEY,
    data            JSONB NOT NULL,
    _written_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _sync_run_id    UUID
);""".strip()


# ---------------------------------------------------------------------------
# Runtime provisioning
# ---------------------------------------------------------------------------


async def ensure_desired_state_table(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> None:
    """Create the desired-state table if it doesn't exist.

    Also sets REPLICA IDENTITY FULL so logical replication change events carry
    full before/after row values (T2 #22).
    """
    table = desired_state_table_name(connector, datatype, namespace)
    await conn.execute(desired_state_table_ddl(connector, datatype, namespace))
    # Idempotent — safe to call on existing tables
    await conn.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")


async def ensure_lwstate_table(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    namespace: str = "public",
) -> None:
    """Create the last-written-state table if it doesn't exist.

    Also sets REPLICA IDENTITY FULL (T2 #22).
    """
    table = lwstate_table_name(connector, datatype, namespace)
    await conn.execute(lwstate_table_ddl(connector, datatype, namespace))
    await conn.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")


async def upsert_desired_state(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    external_id: str,
    data: dict,
    action: str = "update",
    run_id: object = None,
    namespace: str = "public",
) -> None:
    """Insert or update a desired-state row for (connector, datatype, external_id)."""
    import orjson

    table = desired_state_table_name(connector, datatype, namespace)
    data_json = orjson.dumps(data).decode()
    run_id_val = str(run_id) if run_id is not None else None
    await conn.execute(
        f"""
        INSERT INTO {table} (external_id, data, _action, _updated_at, _sync_run_id)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (external_id) DO UPDATE
        SET data = EXCLUDED.data,
            _action = EXCLUDED._action,
            _updated_at = NOW(),
            _sync_run_id = EXCLUDED._sync_run_id
        """,
        [external_id, data_json, action, run_id_val],
    )


async def upsert_lwstate(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    external_id: str,
    data: dict,
    run_id: object = None,
    namespace: str = "public",
) -> None:
    """Record what was last successfully written for an external_id."""
    import orjson

    table = lwstate_table_name(connector, datatype, namespace)
    data_json = orjson.dumps(data).decode()
    run_id_val = str(run_id) if run_id is not None else None
    await conn.execute(
        f"""
        INSERT INTO {table} (external_id, data, _written_at, _sync_run_id)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (external_id) DO UPDATE
        SET data = EXCLUDED.data,
            _written_at = NOW(),
            _sync_run_id = EXCLUDED._sync_run_id
        """,
        [external_id, data_json, run_id_val],
    )


async def get_lwstate(
    conn: psycopg.AsyncConnection,
    connector: str,
    datatype: str,
    external_id: str,
    namespace: str = "public",
) -> dict | None:
    """Fetch the last-written-state for an external_id, or None if not found."""
    import orjson

    table = lwstate_table_name(connector, datatype, namespace)
    try:
        row = await (await conn.execute(
            f"SELECT data FROM {table} WHERE external_id = %s",
            [external_id],
        )).fetchone()
    except Exception:
        return None

    if row is None:
        return None
    data = row[0]
    if isinstance(data, dict):
        return data
    if isinstance(data, (str, bytes)):
        try:
            return orjson.loads(data)
        except Exception:
            return None
    return None
