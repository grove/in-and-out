"""
Contract tests: validate inout_src_* ingestion output tables against schema contract.

These tests verify that the PostgreSQL schema produced by in-and-out ingestion
matches the contract defined in docs/SCHEMA_CONTRACT.md, sections 1 and 6.
"""
from __future__ import annotations

import uuid

import pytest

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_columns(conn, table_name: str) -> dict[str, dict]:
    """Return {column_name: {data_type, is_nullable}} from information_schema."""
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


async def _get_constraints(conn, table_name: str) -> list[str]:
    """Return list of constraint types for a table."""
    rows = await (await conn.execute(
        """
        SELECT constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = %s
        """,
        [table_name],
    )).fetchall()
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_source_table_has_required_columns(pool):
    """inout_src_* tables must have all required columns with correct types."""
    connector = "contract_test"
    datatype = "items"

    async with pool.connection() as conn:
        from inandout.postgres.schema import ensure_source_table
        await ensure_source_table(conn, connector, datatype)
        await conn.commit()

    table_name = f"inout_src_{connector}_{datatype}"
    async with pool.connection() as conn:
        cols = await _get_columns(conn, table_name)

    # Required columns from SCHEMA_CONTRACT.md
    required = {
        "external_id": "text",
        "data": "jsonb",
        "raw": "jsonb",
        "_ingested_at": "timestamp with time zone",
        "_sync_run_id": "uuid",
        "_raw_hash": "text",
        "_deleted": "boolean",
        "_deleted_at": "timestamp with time zone",
        "_schema_version": "integer",
        "_source_version": "text",
        "_last_written": "jsonb",
        "_lineage": "jsonb",
    }

    for col_name, expected_type in required.items():
        assert col_name in cols, f"Missing required column: {col_name}"
        actual_type = cols[col_name]["data_type"]
        assert actual_type == expected_type, (
            f"Column {col_name}: expected type {expected_type!r}, got {actual_type!r}"
        )


@pytest.mark.anyio
async def test_source_table_external_id_not_null(pool):
    """external_id must have NOT NULL constraint."""
    connector = "contract_test_nn"
    datatype = "items"

    async with pool.connection() as conn:
        from inandout.postgres.schema import ensure_source_table
        await ensure_source_table(conn, connector, datatype)
        await conn.commit()

    table_name = f"inout_src_{connector}_{datatype}"
    async with pool.connection() as conn:
        cols = await _get_columns(conn, table_name)

    assert cols["external_id"]["is_nullable"] == "NO"


@pytest.mark.anyio
async def test_source_table_primary_key_on_external_id(pool):
    """Source table must have PRIMARY KEY on external_id."""
    connector = "contract_test_pk"
    datatype = "entities"

    async with pool.connection() as conn:
        from inandout.postgres.schema import ensure_source_table
        await ensure_source_table(conn, connector, datatype)
        await conn.commit()

    table_name = f"inout_src_{connector}_{datatype}"
    async with pool.connection() as conn:
        constraints = await _get_constraints(conn, table_name)

    assert "PRIMARY KEY" in constraints


@pytest.mark.anyio
async def test_upsert_on_conflict_external_id(pool):
    """Upserting a record twice should update, not insert a duplicate."""
    from inandout.ingestion.engine import _upsert_record
    from inandout.postgres.schema import ensure_source_table

    connector = "upsert_contract"
    datatype = "widgets"
    run_id = uuid.uuid4()

    async with pool.connection() as conn:
        await ensure_source_table(conn, connector, datatype)
        await conn.commit()

    table = f"inout_src_{connector}_{datatype}"

    # First insert
    async with pool.connection() as conn:
        inserted, updated, _resurrected = await _upsert_record(
            conn, table, "ext_001", {"name": "Widget A"}, "hash_v1", run_id
        )
        await conn.commit()
    assert inserted == 1
    assert updated == 0

    # Upsert with changed data
    async with pool.connection() as conn:
        inserted2, updated2, _resurrected2 = await _upsert_record(
            conn, table, "ext_001", {"name": "Widget A Updated"}, "hash_v2", run_id
        )
        await conn.commit()
    assert inserted2 == 0
    assert updated2 == 1

    # Verify only one row
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE external_id = 'ext_001'"
        )).fetchone()
    assert row[0] == 1
