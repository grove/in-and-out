"""Integration tests for T2 #24: dead-letter queue exhaustion and replay.

When a writeback row fails on every attempt up to ``max_retry_count``,
the engine must move it to the dead-letter table
(``inout_dl_writeback_{connector}_{datatype}``) so it doesn't block
subsequent batches.  Operators can then replay dead-letter rows back
into the delta table, allowing them to be retried.

GOAL.md T2 #24: "Failed rows accumulate in DLQ; replay via control table
restores them."
"""
from __future__ import annotations

import os
import uuid

import pytest
import respx
import httpx

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
from inandout.deadletter.writeback import (
    fetch_writeback_dead_letter_rows,
    replay_writeback_dead_letter_rows,
)
from inandout.postgres.schema import dead_letter_table_name
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

_CONNECTOR = "dl_test"
_DATATYPE = "campaigns"
_BASE_URL = "https://api.dl-test.example.com"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
os.environ["INOUT_CREDENTIAL_DL_TEST_KEY"] = "dummy"


def _make_connector(max_retry_count: int = 1) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DLTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="dl_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert", "update"],
                    enable_crash_recovery=False,
                    max_retry_count=max_retry_count,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{_DATATYPE}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                    ),
                )
            )
        },
    )


async def _create_delta_table(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_DELTA_TABLE} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert',
                _cluster_id TEXT
            )
        """)
        await conn.commit()


_DL_TABLE = dead_letter_table_name("writeback", _CONNECTOR, _DATATYPE)


async def _clear_tables(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"DELETE FROM {_DELTA_TABLE}")
        await conn.commit()
    # Clear DLQ table if it exists — ignore if not yet created
    async with pool.connection() as conn:
        try:
            await conn.execute(f"DELETE FROM {_DL_TABLE}")
            await conn.commit()
        except Exception:
            await conn.rollback()


async def _seed_rows(pool, rows: list[dict]) -> None:
    async with pool.connection() as conn:
        for row in rows:
            await conn.execute(
                f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) "
                "VALUES (%(external_id)s, %(name)s, %(_action)s)",
                row,
            )
        await conn.commit()


async def _seed_failure_in_audit(pool, external_id: str) -> None:
    """Manually insert a 'failed' audit row so failure_count_for_row returns ≥1."""
    async with pool.connection() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO inout_ops_writeback_result
                    (connector, datatype, delta_table, external_id, action, status, processed_at)
                VALUES (%s, %s, %s, %s, 'insert', 'failed', NOW())
                """,
                [_CONNECTOR, _DATATYPE, _DELTA_TABLE, external_id],
            )
            await conn.commit()
        except Exception:
            await conn.rollback()


@pytest.mark.anyio
async def test_failed_row_moved_to_dead_letter(pool):
    """T2 #24: row that hits max_retry_count is moved to the dead-letter table."""
    await _create_delta_table(pool)
    await _clear_tables(pool)

    external_id = "camp-dl-1"
    await _seed_rows(pool, [{"external_id": external_id, "name": "Bad Campaign", "_action": "insert"}])

    # Pre-seed a failure record so failure_count_for_row returns 1 (matching max_retry_count=1)
    await _seed_failure_in_audit(pool, external_id)

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    # This cycle will fail again (500) → trigger dead-letter promotion
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(500, json={"error": "internal error"})
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    assert result.failed >= 1, f"Expected at least 1 failure, got {result.failed}"

    # Dead-letter table must contain the row
    dl_rows = await fetch_writeback_dead_letter_rows(pool, _CONNECTOR, _DATATYPE, limit=10)
    dl_ext_ids = [r["external_id"] for r in dl_rows]
    assert external_id in dl_ext_ids, (
        f"Expected {external_id!r} in dead-letter rows, got {dl_ext_ids}"
    )


@pytest.mark.anyio
async def test_dead_letter_replay_returns_row_to_delta(pool):
    """T2 #24: replaying a DLQ row marks it as requeued and un-dead-letters the delta row."""
    await _create_delta_table(pool)
    await _clear_tables(pool)

    external_id = "camp-replay-1"

    # Directly insert into dead-letter table via move_to_dead_letter helper
    from inandout.deadletter.writeback import move_to_dead_letter

    await move_to_dead_letter(
        pool,
        _CONNECTOR,
        _DATATYPE,
        external_id=external_id,
        action="insert",
        payload_snapshot={"name": "Replay Campaign"},
        error_message="simulated 500",
        delta_table=_DELTA_TABLE,
    )

    # Seed a matching dead_lettered row in the delta table (as move_to_dead_letter sets)
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, 'dead_lettered')",
            [external_id, "Replay Campaign"],
        )
        await conn.commit()

    # Replay from dead-letter
    summary = await replay_writeback_dead_letter_rows(
        pool, _CONNECTOR, _DATATYPE, _DELTA_TABLE, limit=10
    )

    assert summary["replayed"] >= 1, (
        f"Expected at least 1 replayed row, got {summary}"
    )
    assert summary["errors"] == 0, f"Expected no replay errors, got {summary}"

    # Delta row should no longer be 'dead_lettered'
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT _action FROM {_DELTA_TABLE} WHERE external_id = %s",
            [external_id],
        )).fetchone()

    assert row is not None
    assert row[0] != "dead_lettered", (
        f"Delta row _action should be restored after replay, got {row[0]!r}"
    )


@pytest.mark.anyio
async def test_below_retry_limit_not_dead_lettered(pool):
    """T2 #24: row below max_retry_count must NOT be moved to dead-letter yet."""
    await _create_delta_table(pool)
    await _clear_tables(pool)

    external_id = "camp-ok-1"
    await _seed_rows(pool, [{"external_id": external_id, "name": "Young Campaign", "_action": "insert"}])
    # No prior failures seeded. After one writeback failure _write_writeback_feedback
    # writes 1 row to the audit table before _auto_dead_letter_exceeded_rows runs.
    # With max_retry_count=2, failure_count=1 is still below the threshold.

    connector = _make_connector(max_retry_count=2)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(503, json={"error": "service unavailable"})
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    assert result.failed >= 1

    # failure_count_for_row returns 1 after writeback feedback is written.
    # 1 < 2 (max_retry_count), so the row must NOT be in the dead-letter table.
    dl_rows = await fetch_writeback_dead_letter_rows(pool, _CONNECTOR, _DATATYPE, limit=10)
    dl_ext_ids = [r["external_id"] for r in dl_rows]
    assert external_id not in dl_ext_ids, (
        f"{external_id!r} must not be in dead-letter until retry limit exhausted; "
        f"found in DLQ: {dl_ext_ids}"
    )


@pytest.mark.anyio
async def test_max_retry_exhaustion_moves_to_dead_letter(pool):
    """T2 #24: exhausting max_retry_count moves the row to the dead-letter table.

    With max_retry_count=2, pre-seed 2 failure records then trigger a third
    failure.  The writeback engine must move the row to the DLQ because
    failure_count (2 pre-seeded + 1 just recorded = 3) > max_retry_count (2).
    """
    await _create_delta_table(pool)
    await _clear_tables(pool)

    external_id = "camp-exhaust-1"
    await _seed_rows(pool, [{"external_id": external_id, "name": "Exhausted Campaign", "_action": "insert"}])

    # Pre-seed 2 failures (failure_count already at the limit)
    await _seed_failure_in_audit(pool, external_id)
    await _seed_failure_in_audit(pool, external_id)

    connector = _make_connector(max_retry_count=2)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    # Third attempt fails → triggers DLQ promotion
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(500, json={"error": "still broken"})
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    assert result.failed >= 1, f"Expected at least 1 failure, got {result.failed}"

    dl_rows = await fetch_writeback_dead_letter_rows(pool, _CONNECTOR, _DATATYPE, limit=10)
    dl_ext_ids = [r["external_id"] for r in dl_rows]
    assert external_id in dl_ext_ids, (
        f"Expected {external_id!r} in dead-letter after exhausting max_retry_count=2; "
        f"got DLQ rows: {dl_ext_ids}"
    )
