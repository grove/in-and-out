"""Integration tests for T2 #36: Per-Datatype Concurrency Control (writeback).

At most one active writeback operation may run per datatype per connector at
any given time.  A concurrent attempt — whether from the same instance or a
different one — must either skip gracefully (logged warning) with ``skipped=1``
in the result, or serialize behind the first.  The lock is implemented via
``pg_try_advisory_lock``.

GOAL.md T2 #36: "At most one active writeback operation may run per datatype
per connector at any given time.  Concurrent attempts … must either queue and
serialize, or the later attempt must abort gracefully with a logged warning.
The lock must be implemented using PostgreSQL advisory locks."
"""
from __future__ import annotations

import os

import anyio
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

_CONNECTOR = "wb_lock_test"
_DATATYPE = "accounts"
_BASE_URL = "https://api.wb-lock-test.example.com"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
os.environ.setdefault("INOUT_CREDENTIAL_WB_LOCK_KEY", "dummy")


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="WbLockSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="wb_lock_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    enable_crash_recovery=False,
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


async def _clear_delta(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"DELETE FROM {_DELTA_TABLE}")
        await conn.commit()


async def _seed_rows(pool, rows: list[dict]) -> None:
    async with pool.connection() as conn:
        for row in rows:
            await conn.execute(
                f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) "
                "VALUES (%(external_id)s, %(name)s, %(_action)s)",
                row,
            )
        await conn.commit()


@pytest.mark.anyio
async def test_concurrent_cycles_one_skips(pool, run_migrations):
    """T2 #36: two concurrent writeback cycles for the same connector+datatype;
    one must succeed (or be blocked), the other must be skipped (skipped=1)."""
    await _create_delta_table(pool)
    await _clear_delta(pool)
    await _seed_rows(pool, [
        {"external_id": f"acct-lock-{i}", "name": f"Account {i}", "_action": "insert"}
        for i in range(5)
    ])

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    results = []

    async def _run_cycle() -> None:
        with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
            mock.post(f"/v1/{_DATATYPE}").mock(
                return_value=httpx.Response(201, json={"id": "new-id"})
            )
            engine = WritebackEngine(pool=pool)
            r = await engine.run_writeback_cycle(
                connector, _DATATYPE, wb_cfg, _DELTA_TABLE
            )
            results.append(r)

    async with anyio.create_task_group() as tg:
        tg.start_soon(_run_cycle)
        tg.start_soon(_run_cycle)

    skipped = sum(1 for r in results if getattr(r, "skipped", 0) >= 1)
    assert skipped >= 1, (
        f"At least one concurrent writeback cycle must be skipped; "
        f"results: succeeded={[r.succeeded for r in results]}, skipped={[getattr(r,'skipped',0) for r in results]}"
    )


@pytest.mark.anyio
async def test_second_cycle_succeeds_after_first_completes(pool, run_migrations):
    """T2 #36: after the first cycle releases its advisory lock, a subsequent
    cycle must acquire it and process normally."""
    await _create_delta_table(pool)
    await _clear_delta(pool)
    await _seed_rows(pool, [
        {"external_id": "acct-seq-1", "name": "Sequential Acct", "_action": "insert"}
    ])

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": "new-id"})
        )

        engine = WritebackEngine(pool=pool)
        # First cycle — processes the row
        r1 = await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, _DELTA_TABLE
        )

    assert getattr(r1, "skipped", 0) == 0, (
        "First cycle must not be skipped"
    )

    # Seed a new row for the second cycle
    await _seed_rows(pool, [
        {"external_id": "acct-seq-2", "name": "Second Acct", "_action": "insert"}
    ])

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": "new-id-2"})
        )

        engine2 = WritebackEngine(pool=pool)
        r2 = await engine2.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, _DELTA_TABLE
        )

    assert getattr(r2, "skipped", 0) == 0, (
        "Second sequential cycle must acquire the lock and not be skipped"
    )


@pytest.mark.anyio
async def test_different_datatypes_run_independently(pool, run_migrations):
    """T2 #36: advisory locks are per (connector, datatype); cycles for
    different datatypes must not block each other."""
    # Two separate delta tables for different datatypes
    dt_a = "accounts_a"
    dt_b = "accounts_b"
    delta_a = f"inout_delta_{_CONNECTOR}_{dt_a}"
    delta_b = f"inout_delta_{_CONNECTOR}_{dt_b}"

    for table, dt in [(delta_a, dt_a), (delta_b, dt_b)]:
        async with pool.connection() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    external_id TEXT,
                    name        TEXT,
                    _action     TEXT NOT NULL DEFAULT 'insert',
                    _cluster_id TEXT
                )
            """)
            await conn.execute(f"DELETE FROM {table}")
            await conn.execute(
                f"INSERT INTO {table} (external_id, name, _action) VALUES (%s, %s, 'insert')",
                [f"{dt}-row-1", f"Row for {dt}"],
            )
            await conn.commit()

    # Connector with both datatypes
    connector_ab = ConnectorConfig(
        name=_CONNECTOR,
        system="WbLockSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="wb_lock_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            dt_a: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    enable_crash_recovery=False,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{dt_a}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{dt_a}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{dt_a}/${{external_id}}"),
                    ),
                )
            ),
            dt_b: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    enable_crash_recovery=False,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{dt_b}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{dt_b}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{dt_b}/${{external_id}}"),
                    ),
                )
            ),
        },
    )

    wb_cfg_a = connector_ab.datatypes[dt_a].writeback
    wb_cfg_b = connector_ab.datatypes[dt_b].writeback

    results = {}

    async def _run_a() -> None:
        with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
            mock.post(f"/v1/{dt_a}").mock(return_value=httpx.Response(201, json={"id": "a-new"}))
            engine = WritebackEngine(pool=pool)
            results["a"] = await engine.run_writeback_cycle(connector_ab, dt_a, wb_cfg_a, delta_a)

    async def _run_b() -> None:
        with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
            mock.post(f"/v1/{dt_b}").mock(return_value=httpx.Response(201, json={"id": "b-new"}))
            engine = WritebackEngine(pool=pool)
            results["b"] = await engine.run_writeback_cycle(connector_ab, dt_b, wb_cfg_b, delta_b)

    # Run both concurrently — they target different datatypes so must not block each other
    async with anyio.create_task_group() as tg:
        tg.start_soon(_run_a)
        tg.start_soon(_run_b)

    # Both must complete (neither skipped)
    assert getattr(results.get("a"), "skipped", 0) == 0, (
        "Cycle for dt_a must not be skipped — it uses a distinct lock from dt_b"
    )
    assert getattr(results.get("b"), "skipped", 0) == 0, (
        "Cycle for dt_b must not be skipped — it uses a distinct lock from dt_a"
    )
