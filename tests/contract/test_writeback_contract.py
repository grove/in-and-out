"""
Contract tests: validate writeback desired-state and lwstate tables.

These tests verify the schema contract for inout_dst_* tables as defined in
docs/SCHEMA_CONTRACT.md, sections 2 and 3.
"""
from __future__ import annotations

import pytest

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)


async def _get_columns(conn, table_name: str) -> dict[str, dict]:
    rows = await (await conn.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """,
        [table_name],
    )).fetchall()
    return {
        row[0]: {"data_type": row[1], "is_nullable": row[2]}
        for row in rows
    }


async def _ensure_desired_state_table(conn, connector: str, datatype: str) -> str:
    """Create a minimal inout_dst_* table for testing."""
    table = f"inout_dst_{connector}_{datatype}"
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            action          TEXT NOT NULL,
            cluster_id      TEXT,
            external_id     TEXT,
            data            JSONB NOT NULL DEFAULT '{{}}',
            base            JSONB,
            _status         TEXT NOT NULL DEFAULT 'pending',
            _processed_at   TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    await conn.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    return table


async def _ensure_lwstate_table(conn, connector: str, datatype: str) -> str:
    """Create a minimal inout_dst_*_lwstate table for testing."""
    table = f"inout_dst_{connector}_{datatype}_lwstate"
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            external_id         TEXT NOT NULL,
            connector           TEXT NOT NULL,
            datatype            TEXT NOT NULL,
            written_state       JSONB NOT NULL,
            written_etag        TEXT,
            written_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            written_by_run_id   UUID,
            PRIMARY KEY (external_id, connector, datatype)
        )
    """)
    await conn.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    return table


@pytest.mark.anyio
async def test_desired_state_table_required_columns(pool):
    """inout_dst_* must have all required columns."""
    connector = "wb_contract"
    datatype = "contacts"

    async with pool.connection() as conn:
        table = await _ensure_desired_state_table(conn, connector, datatype)
        await conn.commit()

    async with pool.connection() as conn:
        cols = await _get_columns(conn, f"inout_dst_{connector}_{datatype}")

    required = ["id", "action", "cluster_id", "external_id", "data",
                "base", "_status", "_processed_at", "created_at"]
    for col in required:
        assert col in cols, f"Missing required column in inout_dst_*: {col}"


@pytest.mark.anyio
async def test_lwstate_table_required_columns(pool):
    """inout_dst_*_lwstate must have all required columns."""
    connector = "wb_contract_lw"
    datatype = "contacts"

    async with pool.connection() as conn:
        await _ensure_lwstate_table(conn, connector, datatype)
        await conn.commit()

    async with pool.connection() as conn:
        cols = await _get_columns(conn, f"inout_dst_{connector}_{datatype}_lwstate")

    required = ["external_id", "connector", "datatype", "written_state",
                "written_etag", "written_at", "written_by_run_id"]
    for col in required:
        assert col in cols, f"Missing required column in lwstate table: {col}"


@pytest.mark.anyio
async def test_lwstate_primary_key_composite(pool):
    """lwstate PK must be (external_id, connector, datatype)."""
    connector = "wb_pk_test"
    datatype = "items"

    async with pool.connection() as conn:
        await _ensure_lwstate_table(conn, connector, datatype)
        await conn.commit()

    # Verify duplicate insert raises on (external_id, connector, datatype)
    table = f"inout_dst_{connector}_{datatype}_lwstate"
    async with pool.connection() as conn:
        await conn.execute(
            f"""
            INSERT INTO {table} (external_id, connector, datatype, written_state)
            VALUES ('ext_001', %s, %s, '{{}}'::jsonb)
            """,
            [connector, datatype],
        )
        await conn.commit()

    import psycopg
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            await conn.execute(
                f"""
                INSERT INTO {table} (external_id, connector, datatype, written_state)
                VALUES ('ext_001', %s, %s, '{{"updated": true}}'::jsonb)
                """,
                [connector, datatype],
            )


@pytest.mark.anyio
async def test_desired_state_table_replica_identity_full(pool):
    """inout_dst_* tables must have REPLICA IDENTITY FULL set.

    Required by T2 #22: REPLICA IDENTITY FULL is a prerequisite for logical
    replication change events to carry full before/after row values, enabling
    the base-aware three-way merge (T2 #4).
    """
    connector = "wb_ri_full"
    datatype = "contacts"

    async with pool.connection() as conn:
        table = await _ensure_desired_state_table(conn, connector, datatype)
        await conn.commit()

    # pg_class.relreplident: 'd'=default, 'f'=full, 'i'=index, 'n'=nothing
    async with pool.connection() as conn:
        row = await (await conn.execute(
            """
            SELECT relreplident
            FROM pg_class
            WHERE relname = %s
            """,
            [f"inout_dst_{connector}_{datatype}"],
        )).fetchone()

    assert row is not None, "Table not found in pg_class"
    assert row[0] == "f", (
        f"Expected REPLICA IDENTITY FULL ('f'), got '{row[0]}'. "
        "Set 'ALTER TABLE ... REPLICA IDENTITY FULL' on desired-state tables."
    )


@pytest.mark.anyio
async def test_lwstate_table_replica_identity_full(pool):
    """inout_dst_*_lwstate tables must also have REPLICA IDENTITY FULL.

    The last-written-state table participates in the three-way conflict
    detection protocol (T2 #3, #9) and is read via logical replication
    change events; REPLICA IDENTITY FULL is required for full row capture.
    """
    connector = "wb_lw_ri_full"
    datatype = "widgets"

    async with pool.connection() as conn:
        await _ensure_lwstate_table(conn, connector, datatype)
        await conn.commit()

    async with pool.connection() as conn:
        row = await (await conn.execute(
            """
            SELECT relreplident
            FROM pg_class
            WHERE relname = %s
            """,
            [f"inout_dst_{connector}_{datatype}_lwstate"],
        )).fetchone()

    assert row is not None, "lwstate table not found in pg_class"
    assert row[0] == "f", (
        f"Expected REPLICA IDENTITY FULL ('f') on lwstate, got '{row[0]}'."
    )
