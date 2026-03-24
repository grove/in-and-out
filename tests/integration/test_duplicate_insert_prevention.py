"""Integration test: T2 #14 — duplicate insert prevention.

Verifies that the writeback engine will not re-issue an HTTP POST for a
row that was already successfully dispatched (crash-and-replay scenario).

The duplicate detection queries ``inout_ops_writeback_result`` — a real
PostgreSQL table provisioned by Alembic migrations — making this an
integration test.

GOAL.md T2 #14: validate against the write log before every insert that
the entity has not already been successfully delivered; only failed records
in a partially failed batch should be retried.
"""
from __future__ import annotations

import os
import re
import uuid

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(base_url: str = "https://api.dedup-test.example.com") -> ConnectorConfig:
    return ConnectorConfig(
        name="dedup_wb",
        system="DedupTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="dedup_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "leads": DatatypeConfig(
                writeback=_make_writeback_cfg(),
            ),
        },
    )


def _make_writeback_cfg() -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update"],
        operations=OperationsConfig(
            insert=OperationConfig(method="POST", path="/v1/leads"),
            update=UpdateOperationConfig(
                operation=OperationConfig(method="PATCH", path="/v1/leads/${external_id}"),
            ),
        ),
        enable_crash_recovery=True,
    )


async def _create_delta_table(pool, table_name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                external_id TEXT,
                name        TEXT,
                email       TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert',
                _cluster_id TEXT
            )
        """)
        await conn.commit()


async def _seed_already_dispatched(
    pool,
    connector: str,
    datatype: str,
    delta_table: str,
    external_ids: list[str],
) -> None:
    """Pre-populate inout_ops_writeback_result to simulate a previous successful run."""
    async with pool.connection() as conn:
        for eid in external_ids:
            await conn.execute(
                """
                INSERT INTO inout_ops_writeback_result
                    (connector, datatype, delta_table, external_id, action,
                     status, processed_at)
                VALUES (%s, %s, %s, %s, 'insert', 'ok', NOW())
                ON CONFLICT DO NOTHING
                """,
                [connector, datatype, delta_table, eid],
            )
        await conn.commit()


# ---------------------------------------------------------------------------
# Test 1: Already-dispatched insert is NOT re-issued
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_duplicate_insert_is_not_reissued(pool, run_migrations):
    """A row already recorded as 'ok' in the write log must not produce a POST.

    Scenario:
      1. Seed delta table with row for 'lead_001'.
      2. Pre-populate inout_ops_writeback_result as if 'lead_001' was already
         successfully inserted in the PREVIOUS cycle.
      3. Run a new writeback cycle.
      4. Assert that no HTTP POST was made for 'lead_001'.

    GOAL.md T2 #14: verifying against the write log before every insert.
    """
    os.environ["INOUT_CREDENTIAL_DEDUP_KEY"] = "dummy"

    connector = _make_connector()
    wb_cfg = _make_writeback_cfg()
    delta_table = "_delta_dedup_t2_14_no_reissue"

    await _create_delta_table(pool, delta_table)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {delta_table} "
            f"(external_id, name, email, _action, _cluster_id) "
            f"VALUES (%s, %s, %s, 'insert', %s)",
            ["lead_001", "Alice", "alice@example.com", "cid_alice"],
        )
        await conn.commit()

    # Pre-seed write log — this external_id was already successfully dispatched
    await _seed_already_dispatched(
        pool,
        connector.name,
        "leads",
        delta_table,
        ["lead_001"],
    )

    post_calls: list[str] = []

    with respx.mock(assert_all_called=False):
        import respx as _respx
        _respx.post(url="https://api.dedup-test.example.com/v1/leads").mock(
            side_effect=lambda req: (
                post_calls.append(str(req.url))
                or httpx.Response(201, json={"id": "new_lead_xxx"})
            )
        )

        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(
            connector=connector,
            datatype="leads",
            writeback_cfg=wb_cfg,
            delta_table=delta_table,
        )

    assert len(post_calls) == 0, (
        f"Engine re-issued HTTP POST for already-dispatched insert: {post_calls}"
    )
    # The row was skipped (counted as skipped, not failed)
    assert result.failed == 0, f"Unexpected failures: {result.failed}"


# ---------------------------------------------------------------------------
# Test 2: Only the non-duplicated rows in a mixed batch are dispatched
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_partial_batch_only_new_rows_dispatched(pool, run_migrations):
    """In a batch of 3 inserts, only rows not in the write log are dispatched.

    GOAL.md T2 #14: only failed records in a partially failed batch should
    be retried — successfully written ones must never be retried.
    """
    os.environ["INOUT_CREDENTIAL_DEDUP_KEY"] = "dummy"

    connector = _make_connector()
    wb_cfg = _make_writeback_cfg()
    delta_table = "_delta_dedup_t2_14_partial"

    await _create_delta_table(pool, delta_table)

    for i, (eid, name) in enumerate([
        ("lead_010", "Bob"),
        ("lead_011", "Carol"),   # this one was already dispatched
        ("lead_012", "Dave"),
    ]):
        async with pool.connection() as conn:
            await conn.execute(
                f"INSERT INTO {delta_table} "
                f"(external_id, name, _action) VALUES (%s, %s, 'insert')",
                [eid, name],
            )
            await conn.commit()

    # Pre-seed: lead_011 was already successfully sent
    await _seed_already_dispatched(
        pool,
        connector.name,
        "leads",
        delta_table,
        ["lead_011"],
    )

    dispatched_ids: list[str] = []

    with respx.mock(assert_all_called=False) as mock_router:
        mock_router.post("https://api.dedup-test.example.com/v1/leads").mock(
            side_effect=lambda req: (
                dispatched_ids.append(
                    __import__("json").loads(req.content).get("external_id", "?")
                )
                or httpx.Response(201, json={"id": "new_id"})
            )
        )

        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(
            connector=connector,
            datatype="leads",
            writeback_cfg=wb_cfg,
            delta_table=delta_table,
        )

    # Only lead_010 and lead_012 should have been dispatched
    assert "lead_011" not in dispatched_ids, (
        "lead_011 was dispatched despite being in the write log"
    )
    assert result.failed == 0, f"Unexpected failures: {result.failed}"
    # 2 new + 1 skipped = correct accounting
    assert result.processed + result.skipped >= 2


# ---------------------------------------------------------------------------
# Test 3: First insert is not in the log — must be dispatched
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_first_time_insert_is_dispatched(pool, run_migrations):
    """An insert with no prior write-log record must always proceed.

    This guards against over-eager deduplication: if the write log is
    empty (or the row is genuinely new), the HTTP POST must be issued.
    GOAL.md T2 #14.
    """
    os.environ["INOUT_CREDENTIAL_DEDUP_KEY"] = "dummy"

    connector = _make_connector()
    wb_cfg = _make_writeback_cfg()
    delta_table = "_delta_dedup_t2_14_firsttime"

    await _create_delta_table(pool, delta_table)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {delta_table} "
            f"(external_id, name, _action) VALUES ('lead_new', 'Eve', 'insert')",
        )
        await conn.commit()

    # No pre-seeded write log — this is a truly new insert

    dispatched: list[str] = []

    with respx.mock(assert_all_called=False) as mock_router:
        mock_router.post("https://api.dedup-test.example.com/v1/leads").mock(
            side_effect=lambda req: (
                dispatched.append("lead_new")
                or httpx.Response(201, json={"id": "ext_eve"})
            )
        )

        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(
            connector=connector,
            datatype="leads",
            writeback_cfg=wb_cfg,
            delta_table=delta_table,
        )

    assert "lead_new" in dispatched, (
        "First-time insert was not dispatched — over-eager deduplication"
    )
    assert result.processed >= 1
