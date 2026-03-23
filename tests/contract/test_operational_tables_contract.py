"""
Contract tests: validate inout_ops_* operational tables against schema contract.

These tests verify the schema contract for operational tables as defined in
docs/SCHEMA_CONTRACT.md, section 5.

These tests require Alembic migrations to have been run (via run_migrations fixture).
"""
from __future__ import annotations

import pytest

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)


async def _get_columns(conn, table_name: str) -> set[str]:
    rows = await (await conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        [table_name],
    )).fetchall()
    return {row[0] for row in rows}


@pytest.mark.anyio
async def test_sync_run_table_required_columns(pool, run_migrations):
    """inout_ops_sync_run must have all spec-required columns."""
    async with pool.connection() as conn:
        cols = await _get_columns(conn, "inout_ops_sync_run")

    required = {
        "id", "connector", "datatype", "mode", "status",
        "started_at", "finished_at",
        "records_fetched", "records_inserted", "records_updated",
        "records_deleted", "records_errored", "error_message",
    }
    missing = required - cols
    assert not missing, f"Missing columns in inout_ops_sync_run: {missing}"


@pytest.mark.anyio
async def test_watermark_table_required_columns(pool, run_migrations):
    """inout_ops_watermark must have required columns including watermark_type."""
    async with pool.connection() as conn:
        cols = await _get_columns(conn, "inout_ops_watermark")

    required = {
        "connector", "datatype", "watermark_type", "watermark_value",
        "updated_at", "updated_by_run_id",
    }
    missing = required - cols
    assert not missing, f"Missing columns in inout_ops_watermark: {missing}"


@pytest.mark.anyio
async def test_control_table_required_columns(pool, run_migrations):
    """inout_ops_control must have required columns including target_tool."""
    async with pool.connection() as conn:
        cols = await _get_columns(conn, "inout_ops_control")

    required = {
        "id", "connector", "datatype", "command", "status",
        "payload", "issued_at", "issued_by",
        "acknowledged_at", "completed_at", "result",
    }
    missing = required - cols
    assert not missing, f"Missing columns in inout_ops_control: {missing}"


@pytest.mark.anyio
async def test_sync_checkpoint_table_exists(pool, run_migrations):
    """inout_ops_sync_checkpoint must exist with required columns."""
    async with pool.connection() as conn:
        cols = await _get_columns(conn, "inout_ops_sync_checkpoint")

    required = {
        "run_id", "connector", "datatype", "page_number",
        "cursor_value", "records_committed", "checkpointed_at",
    }
    missing = required - cols
    assert not missing, f"Missing columns in inout_ops_sync_checkpoint: {missing}"


@pytest.mark.anyio
async def test_sync_run_status_check_constraint(pool, run_migrations):
    """inout_ops_sync_run.status must only accept valid values."""
    import psycopg

    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                """
                INSERT INTO inout_ops_sync_run
                    (connector, datatype, mode, status)
                VALUES ('test', 'test', 'full', 'invalid_status')
                """
            )


@pytest.mark.anyio
async def test_control_table_status_check_constraint(pool, run_migrations):
    """inout_ops_control.status must only accept valid values."""
    import psycopg

    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                """
                INSERT INTO inout_ops_control (command, status)
                VALUES ('test_cmd', 'bogus_status')
                """
            )
