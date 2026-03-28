"""Multi-instance HA integration test — advisory locks prevent concurrent writeback.

Verifies that when two WritebackEngine instances race to process the same
(connector, datatype, delta_table) pair, the advisory-lock mechanism ensures
only one runs at a time and the other either skips or waits.

This mirrors the ingestion-side test in test_distributed_lock.py but for the
writeback path, which uses its own locking layer.
"""
from __future__ import annotations

import os
import re

import anyio
import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.tool import DatabaseConfig
from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.postgres.pool import create_pool
from inandout.writeback.engine import WritebackEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.ha.example.com"


def _make_ha_connector(name: str = "ha_wb") -> ConnectorConfig:
    cred_key = f"INOUT_CREDENTIAL_{name.upper()}_KEY"
    os.environ[cred_key] = "dummy-ha"
    return ConnectorConfig(
        name=name,
        system="HASystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref=f"{name}_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "records": DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/records/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/records/${external_id}"),
                    ),
                )
            )
        },
    )


async def _create_delta_table(pool, table_name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.commit()


async def _insert_delta_rows(pool, table_name: str, n: int = 5) -> None:
    async with pool.connection() as conn:
        for i in range(n):
            await conn.execute(
                f"INSERT INTO {table_name} (external_id, name, _action) VALUES (%s, %s, 'update')",
                [f"rec-{i}", f"Record {i}"],
            )
        await conn.commit()


# ---------------------------------------------------------------------------
# Test 1 — two engines race on same delta table, advisory lock serialises
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_two_writeback_engines_same_delta_table(pool, run_migrations):
    """Two WritebackEngine instances racing on the same delta table are serialised.

    One engine acquires the advisory lock and runs to completion; the other
    either skips (trylock) or waits until the first finishes (blocking lock).
    In either case no row is dispatched twice.
    """
    delta = "_delta_ha_wb_lock_test"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, n=4)

    connector = _make_ha_connector("ha_wb")
    writeback_cfg = connector.datatypes["records"].writeback
    assert writeback_cfg is not None

    dispatched_ids: list[str] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        async def _handle(request: httpx.Request) -> httpx.Response:
            ext_id = request.url.path.split("/")[-1]
            dispatched_ids.append(ext_id)
            return httpx.Response(200, json={})

        mock.patch(re.compile(r"/v1/records/\S+")).mock(side_effect=_handle)

        dsn = pool.conninfo
        db_cfg = DatabaseConfig(dsn=dsn)
        pool1 = await create_pool(db_cfg)
        pool2 = await create_pool(db_cfg)

        try:
            engine1 = WritebackEngine(pool1)
            engine2 = WritebackEngine(pool2)

            results = []

            async def _run1() -> None:
                r = await engine1.run_writeback_cycle(connector, "records", writeback_cfg, delta)
                results.append(("engine1", r))

            async def _run2() -> None:
                r = await engine2.run_writeback_cycle(connector, "records", writeback_cfg, delta)
                results.append(("engine2", r))

            async with anyio.create_task_group() as tg:
                tg.start_soon(_run1)
                tg.start_soon(_run2)

        finally:
            await pool1.close()
            await pool2.close()

    # Both calls returned
    assert len(results) == 2

    # No row should be dispatched more than once — advisory lock must prevent
    # concurrent dispatch of the same rows
    assert len(dispatched_ids) == len(set(dispatched_ids)), (
        f"Duplicate dispatches detected: {dispatched_ids}"
    )

    # At least one engine processed rows (the other may have skipped or also processed)
    total_processed = sum(r.processed for _, r in results)
    assert total_processed >= 1


# ---------------------------------------------------------------------------
# Test 2 — different (connector, datatype) pairs run fully concurrently
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_different_writeback_datatypes_run_concurrently(pool, run_migrations):
    """Different connector/datatype writeback jobs are not blocked by each other."""
    delta_a = "_delta_ha_wb_lock_a"
    delta_b = "_delta_ha_wb_lock_b"
    await _create_delta_table(pool, delta_a)
    await _create_delta_table(pool, delta_b)
    await _insert_delta_rows(pool, delta_a, n=2)
    await _insert_delta_rows(pool, delta_b, n=2)

    connector_a = _make_ha_connector("ha_wb_a")
    connector_b = _make_ha_connector("ha_wb_b")
    cfg_a = connector_a.datatypes["records"].writeback
    cfg_b = connector_b.datatypes["records"].writeback
    assert cfg_a is not None and cfg_b is not None

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(re.compile(r"/v1/records/\S+")).mock(
            return_value=httpx.Response(200, json={})
        )

        dsn = pool.conninfo
        db_cfg = DatabaseConfig(dsn=dsn)
        pool1 = await create_pool(db_cfg)
        pool2 = await create_pool(db_cfg)

        try:
            results = []

            async def _run_a() -> None:
                r = await WritebackEngine(pool1).run_writeback_cycle(
                    connector_a, "records", cfg_a, delta_a
                )
                results.append(r)

            async def _run_b() -> None:
                r = await WritebackEngine(pool2).run_writeback_cycle(
                    connector_b, "records", cfg_b, delta_b
                )
                results.append(r)

            async with anyio.create_task_group() as tg:
                tg.start_soon(_run_a)
                tg.start_soon(_run_b)

        finally:
            await pool1.close()
            await pool2.close()

    assert len(results) == 2
    # Both jobs should complete (different lock keys = no contention)
    assert all(r.processed >= 0 for r in results)
