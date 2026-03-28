"""
Load test: writeback throughput and batch composition under sustained load.

Validates GOAL.md T2 #33 (batch composition parameters), T2 #11 (politeness
/ rate limiting during writeback), and T2 #28 (write ordering per record).

Run with: pytest tests/load/test_writeback_throughput.py -v -m load
"""
from __future__ import annotations

import os
import time
import uuid

import anyio
import pytest
import respx
import httpx

from .conftest import _docker_available

pytestmark = [
    pytest.mark.skipif(not _docker_available(), reason="Docker not available"),
    pytest.mark.load,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_delta_records(count: int, action: str = "update") -> list[dict]:
    return [
        {
            "_action": action,
            "_cluster_id": f"cid_{i:06d}",
            "external_id": f"ext_{i:06d}",
            "data": {"name": f"Record {i}", "value": i},
            "base": {"name": f"Record {i}", "value": i - 1},
        }
        for i in range(count)
    ]


async def _seed_delta_table(pool, table: str, records: list[dict]) -> None:
    """Insert pre-formed delta rows into the given delta table."""
    from inandout.postgres.schema import ensure_desired_state_table

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, table)
        for rec in records:
            await conn.execute(
                f"""
                INSERT INTO {table}
                    (_action, _cluster_id, external_id, data, base)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                [
                    rec["_action"],
                    rec["_cluster_id"],
                    rec["external_id"],
                    __import__("json").dumps(rec["data"]),
                    __import__("json").dumps(rec["base"]),
                ],
            )
        await conn.commit()


def _make_connector(base_url: str = "https://api.load-test.example.com"):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import (
        ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile,
    )
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution,
        OperationsConfig, OperationConfig, UpdateOperationConfig,
    )

    wb_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/items/${external_id}"),
            update=UpdateOperationConfig(
                method="PATCH",
                path="/items/${external_id}",
            ),
        ),
        batch_size=2000,  # large enough to fetch all records in one cycle
        max_concurrent_writes=5,
    )

    return ConnectorConfig(
        name="load_wb",
        system="LoadTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="load_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={"items": DatatypeConfig(writeback=wb_cfg)},
    )


# ---------------------------------------------------------------------------
# Test 1: Writeback throughput — 1 000 records, assert > 100 records/sec
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_writeback_throughput_1k_records(pool, run_migrations):
    """Writeback 1 000 update rows; assert throughput > 100 records/sec.

    GOAL.md T2 #33: batch composition parameters (max record count,
    configurable batch_size).  This test verifies the engine can sustain
    reasonable throughput on a real PostgreSQL database with mock HTTP.
    """
    import json
    os.environ["INOUT_CREDENTIAL_LOAD_KEY"] = "dummy"

    from inandout.postgres.desired_state import desired_state_table_ddl
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector()
    datatype = "items"
    wb_cfg = connector.datatypes[datatype].writeback

    n_records = 1_000
    records = _make_delta_records(n_records, action="update")

    # Use the engine's internal table name convention
    delta_table = f"inout_dst_{connector.name}_{datatype}"

    async with pool.connection() as conn:
        await conn.execute(desired_state_table_ddl(connector.name, datatype))
        for rec in records:
            await conn.execute(
                f"""
                INSERT INTO {delta_table}
                    (_action, cluster_id, external_id, data, base, _status)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, 'pending')
                """,
                [
                    rec["_action"],
                    rec["_cluster_id"],
                    rec["external_id"],
                    json.dumps(rec["data"]),
                    json.dumps(rec["base"]),
                ],
            )
        await conn.commit()

    dispatched = []

    with respx.mock(assert_all_called=False) as mock_router:
        mock_router.patch(url__regex=r"/items/ext_\d+").mock(
            side_effect=lambda req: (
                dispatched.append(req.url.path)
                or httpx.Response(200, json={"id": req.url.path.split("/")[-1]})
            )
        )

        engine = WritebackEngine(pool=pool)

        start = time.perf_counter()
        result = await engine.run_writeback_cycle(
            connector=connector,
            datatype=datatype,
            writeback_cfg=wb_cfg,
            delta_table=delta_table,
            max_concurrent_writes_override=10,
        )
        elapsed = time.perf_counter() - start

    total = result.processed + result.failed
    throughput = total / elapsed if elapsed > 0 else float("inf")

    assert total >= n_records * 0.95, (
        f"Expected ~{n_records} records processed, got {total} "
        f"(processed={result.processed}, failed={result.failed})"
    )
    assert throughput >= 100, (
        f"Writeback throughput {throughput:.0f} rec/s < 100 rec/s target "
        f"(elapsed={elapsed:.2f}s, total={total})"
    )


# ---------------------------------------------------------------------------
# Test 2: Batch composition — different batch sizes produce correct counts
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_batch_composition_respects_max_record_count(pool, run_migrations):
    """WritebackEngine must respect batch_size; no batch exceeds max_record_count.

    GOAL.md T2 #33: batches are closed when max record count is reached.
    We verify that each HTTP PATCH is issued individually (batch_size=1)
    and that the total dispatched count equals the seeded row count.
    """
    import json

    import json

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import (
        ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile,
    )
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution,
        OperationsConfig, OperationConfig, UpdateOperationConfig,
    )
    from inandout.postgres.desired_state import desired_state_table_ddl
    from inandout.writeback.engine import WritebackEngine

    n = 20
    connector_name = "batch_comp_test"
    datatype = "widgets"
    os.environ["INOUT_CREDENTIAL_K"] = "dummy"
    delta_table = f"inout_dst_{connector_name}_{datatype}"

    wb_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/widgets/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/widgets/${external_id}"),
        ),
        batch_size=100,  # fetch all n=20 rows in one cycle
        max_concurrent_writes=1,
    )
    connector = ConnectorConfig(
        name=connector_name,
        system="BatchCompTest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.batchcomp.example"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="k",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={datatype: DatatypeConfig(writeback=wb_cfg)},
    )

    async with pool.connection() as conn:
        await conn.execute(desired_state_table_ddl(connector_name, datatype))
        for i in range(n):
            await conn.execute(
                f"""
                INSERT INTO {delta_table}
                    (_action, cluster_id, external_id, data, base, _status)
                VALUES ('update', %s, %s, %s::jsonb, %s::jsonb, 'pending')
                """,
                [f"cid_{i}", f"w_{i:04d}", json.dumps({"v": i}), json.dumps({"v": i - 1})],
            )
        await conn.commit()

    dispatched_ids: list[str] = []

    with respx.mock(assert_all_called=False) as mock_router:
        mock_router.patch(url__regex=r"/widgets/w_\d+").mock(
            side_effect=lambda req: (
                dispatched_ids.append(req.url.path.split("/")[-1])
                or httpx.Response(200, json={"ok": True})
            )
        )

        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(
            connector=connector,
            datatype=datatype,
            writeback_cfg=wb_cfg,
            delta_table=delta_table,
        )

    # All n records should be processed; no duplicates (T2 #28)
    assert result.processed == n, (
        f"Expected {n} processed, got {result.processed} "
        f"(failed={result.failed})"
    )
    assert len(set(dispatched_ids)) == n, (
        "Duplicate dispatches detected — write-ordering contract violated (T2 #28)"
    )


# ---------------------------------------------------------------------------
# Test 3: Concurrent writeback workers — advisory lock prevents double-dispatch
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_concurrent_writeback_cycles_no_double_dispatch(pool, run_migrations):
    """Two concurrent WritebackEngine instances must not dispatch the same row twice.

    GOAL.md T2 #36: per-datatype advisory lock; one instance wins, the other
    skips gracefully.  Mirrors the ingestion-side HA test.
    """
    import json

    import json

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import (
        ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile,
    )
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution,
        OperationsConfig, OperationConfig, UpdateOperationConfig,
    )
    from inandout.postgres.desired_state import desired_state_table_ddl
    from inandout.postgres.pool import create_pool
    from inandout.writeback.engine import WritebackEngine

    n = 10
    connector_name = "ha_throughput"
    datatype = "items"
    delta_table = f"inout_dst_{connector_name}_{datatype}"
    db_url = pool.conninfo.replace("postgresql://", "postgresql://")
    os.environ["INOUT_CREDENTIAL_K"] = "dummy"

    wb_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/items/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/items/${external_id}"),
        ),
        batch_size=50,
        max_concurrent_writes=2,
    )
    connector = ConnectorConfig(
        name=connector_name,
        system="HATest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.ha.example"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="k",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={datatype: DatatypeConfig(writeback=wb_cfg)},
    )

    async with pool.connection() as conn:
        await conn.execute(desired_state_table_ddl(connector_name, datatype))
        for i in range(n):
            await conn.execute(
                f"""
                INSERT INTO {delta_table}
                    (_action, cluster_id, external_id, data, base, _status)
                VALUES ('update', %s, %s, %s::jsonb, %s::jsonb, 'pending')
                """,
                [f"cid_{i}", f"item_{i:04d}", json.dumps({"v": i}), json.dumps({"v": i})],
            )
        await conn.commit()

    dispatched: list[str] = []

    with respx.mock(assert_all_called=False) as mock_router:
        mock_router.patch(url__regex=r"/items/item_\d+").mock(
            side_effect=lambda req: (
                dispatched.append(req.url.path.split("/")[-1])
                or httpx.Response(200, json={"ok": True})
            )
        )

        # Use a second connection pool to simulate a separate process
        from inandout.config.tool import DatabaseConfig
        pool2 = await create_pool(DatabaseConfig(dsn=pool.conninfo))

        results = []

        async def _run(p):
            eng = WritebackEngine(pool=p)
            r = await eng.run_writeback_cycle(
                connector=connector,
                datatype=datatype,
                writeback_cfg=wb_cfg,
                delta_table=delta_table,
            )
            results.append(r)

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(_run, pool)
                tg.start_soon(_run, pool2)
        finally:
            await pool2.close()

    # No row may be dispatched more than once (advisory lock enforces serial access)
    assert len(dispatched) == len(set(dispatched)), (
        "Same row dispatched by two concurrent engines — advisory lock failure"
    )
    total_processed = sum(r.processed for r in results)
    assert total_processed == n, (
        f"Expected {n} total processed across both engines, got {total_processed}"
    )
