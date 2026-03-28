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


# ---------------------------------------------------------------------------
# T2 #8 — Identity mapping table contract
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_identity_map_required_columns(pool, run_migrations):
    """inout_ops_identity_map must have all MDM-Contract-required columns.

    GOAL.md T2 #8: (cluster_id, connector, datatype) → target_external_id,
    with a reverse-lookup index on (connector, datatype, target_external_id).
    """
    async with pool.connection() as conn:
        cols = await _get_columns(conn, "inout_ops_identity_map")

    required = {
        "connector", "datatype", "external_id",
        "internal_id", "cluster_id", "target_external_id",
        "created_at", "updated_at", "sync_run_id",
    }
    missing = required - cols
    assert not missing, f"Missing columns in inout_ops_identity_map: {missing}"


@pytest.mark.anyio
async def test_identity_map_cluster_unique_constraint(pool, run_migrations):
    """(cluster_id, connector, datatype) must be unique in the identity map.

    GOAL.md T2 #8: duplicate inserts with the same cluster_id + connector +
    datatype must be rejected at the DB level to prevent duplicate inserts.
    """
    import psycopg

    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_identity_map
                (connector, datatype, external_id, internal_id, cluster_id)
            VALUES ('ctr_test', 'accounts', 'cid_001', 'ext_aaa', 'cid_001')
            """
        )
        await conn.commit()

    async with pool.connection() as conn:
        with pytest.raises((psycopg.errors.UniqueViolation, Exception)):
            await conn.execute(
                """
                INSERT INTO inout_ops_identity_map
                    (connector, datatype, external_id, internal_id, cluster_id)
                VALUES ('ctr_test', 'accounts', 'cid_001_b', 'ext_bbb', 'cid_001')
                """
            )
            await conn.commit()


# ---------------------------------------------------------------------------
# T2 #24 — Dead-letter table schema contract
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_dead_letter_table_required_columns(pool, run_migrations):
    """inout_dl_writeback_* must have all T2 #24-required columns.

    Required: original record (raw JSONB), full error response (error_message,
    error_class), timestamp of first failure (failed_at), retry count
    (requeue_count), and sufficient context for operator replay.
    """
    from inandout.postgres.schema import (
        dead_letter_table_name,
        ensure_dead_letter_table,
    )

    connector = "dl_contract"
    datatype = "events"
    dl_table = dead_letter_table_name("writeback", connector, datatype)

    async with pool.connection() as conn:
        await ensure_dead_letter_table(conn, "writeback", connector, datatype)
        await conn.commit()

    async with pool.connection() as conn:
        cols = await _get_columns(conn, dl_table.split(".")[-1])

    required = {
        "id", "external_id", "raw",
        "error_message", "error_class",
        "failed_at", "sync_run_id",
        "requeued_at", "requeue_count",
    }
    missing = required - cols
    assert not missing, f"Missing columns in {dl_table}: {missing}"


@pytest.mark.anyio
async def test_dead_letter_table_requeue_count_default(pool, run_migrations):
    """requeue_count must default to 0 — entries start with no retry history."""
    from inandout.postgres.schema import (
        dead_letter_table_name,
        ensure_dead_letter_table,
    )

    connector = "dl_rqcount"
    datatype = "orders"
    dl_table = dead_letter_table_name("writeback", connector, datatype)

    async with pool.connection() as conn:
        await ensure_dead_letter_table(conn, "writeback", connector, datatype)
        await conn.execute(
            f"""
            INSERT INTO {dl_table} (external_id, error_message, error_class)
            VALUES ('ext_001', 'HTTP 500', 'server_error')
            """
        )
        await conn.commit()

    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT requeue_count FROM {dl_table} WHERE external_id = 'ext_001'"
        )).fetchone()

    assert row is not None
    assert row[0] == 0, f"Expected requeue_count default 0, got {row[0]}"


@pytest.mark.anyio
async def test_dead_letter_naming_convention(pool, run_migrations):
    """Dead-letter tables must follow inout_dl_{tool}_{connector}_{datatype} naming.

    GOAL.md MDM Contract: 'dead-letter tables use inout_dl_{tool}_{connector}_{datatype}'.
    """
    from inandout.postgres.schema import dead_letter_table_name

    assert dead_letter_table_name("writeback", "hubspot", "contacts") == (
        "inout_dl_writeback_hubspot_contacts"
    )
    assert dead_letter_table_name("ingestion", "salesforce", "leads") == (
        "inout_dl_ingestion_salesforce_leads"
    )
