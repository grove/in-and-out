"""Integration tests for T2 #21: transaction-level atomicity in writeback.

When multiple desired-state rows share the same ``_group_id``, they form an
atomic transaction group.  If any member fails, the remaining unprocessed
members must be immediately routed to the dead-letter queue rather than
attempting their writes — preventing a partial-success state in the target.
"""
from __future__ import annotations

import os
import re

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig,
    ProtectionLevel,
    ConflictResolution,
    OperationsConfig,
    OperationConfig,
    UpdateOperationConfig,
)
from inandout.writeback.engine import WritebackEngine
from inandout.postgres.schema import dead_letter_table_name


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available",
)

_CONNECTOR = "atomicity_test"
_DATATYPE = "orders"
_BASE_URL = "https://api.atomicity-test.example.com"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="AtomicityTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="atomicity_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update", "insert", "delete"],
                    enable_crash_recovery=False,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{_DATATYPE}"),
                        delete=OperationConfig(method="DELETE", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                    ),
                )
            )
        },
    )


async def _create_delta_table_with_group(pool, table_name: str) -> None:
    """Delta table that includes the _group_id column needed for atomicity tests."""
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                external_id TEXT,
                name        TEXT,
                amount      INTEGER,
                _action     TEXT NOT NULL DEFAULT 'update',
                _cluster_id TEXT,
                _group_id   TEXT
            )
        """)
        await conn.commit()


async def _insert_group_rows(
    pool,
    table_name: str,
    rows: list[dict],
) -> None:
    async with pool.connection() as conn:
        for row in rows:
            await conn.execute(
                f"""
                INSERT INTO {table_name}
                    (external_id, name, amount, _action, _group_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    row["external_id"],
                    row.get("name"),
                    row.get("amount"),
                    row.get("_action", "update"),
                    row.get("_group_id"),
                ],
            )
        await conn.commit()


@pytest.mark.anyio
async def test_group_failure_aborts_remaining_members(pool, run_migrations):
    """When a group member fails, subsequent members are sent to the dead-letter queue.

    T2 #21: Three rows share the same _group_id ("txn-001").  The first row
    (order-A) fails with HTTP 422.  The remaining rows (order-B, order-C) must
    be aborted and routed to the dead-letter queue rather than being written.
    """
    os.environ["INOUT_CREDENTIAL_ATOMICITY_KEY"] = "dummy"

    delta_table = "_delta_atomicity_group_abort"
    await _create_delta_table_with_group(pool, delta_table)
    await _insert_group_rows(pool, delta_table, [
        {"external_id": "order-A", "name": "Order A", "amount": 100, "_action": "update", "_group_id": "txn-001"},
        {"external_id": "order-B", "name": "Order B", "amount": 200, "_action": "update", "_group_id": "txn-001"},
        {"external_id": "order-C", "name": "Order C", "amount": 300, "_action": "update", "_group_id": "txn-001"},
    ])

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    assert wb_cfg is not None
    engine = WritebackEngine(pool)

    dispatched_ids: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        ext_id = request.url.path.split("/")[-1]
        dispatched_ids.append(ext_id)
        if ext_id == "order-A":
            # First member fails — this should abort order-B and order-C
            return httpx.Response(422, json={"error": "invalid order", "id": ext_id})
        return httpx.Response(200, json={"id": ext_id})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(re.compile(r"/v1/orders/[\w-]+")).mock(side_effect=handle)

        result = await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, delta_table,
            # Sequential dispatch ensures order-A fails before order-B/C are dispatched
            max_concurrent_writes_override=1,
        )

    # order-A made an HTTP call and failed; order-B and order-C were aborted (no HTTP call)
    assert "order-A" in dispatched_ids, "order-A must attempt the HTTP call"
    assert "order-B" not in dispatched_ids, "order-B must be aborted without HTTP call"
    assert "order-C" not in dispatched_ids, "order-C must be aborted without HTTP call"

    # Total failed: 1 (HTTP failure) + 2 (group abort) = 3
    assert result.failed == 3, f"Expected 3 failed, got {result.failed}"
    assert result.processed == 0, "No rows should succeed"

    # The aborted rows must appear in the dead-letter table
    dl_table = dead_letter_table_name("writeback", _CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        dl_rows = await (await conn.execute(
            f"SELECT external_id, error_class FROM {dl_table} ORDER BY external_id"
        )).fetchall()

    dl_external_ids = {row[0] for row in dl_rows}
    assert "order-B" in dl_external_ids, "order-B must be in dead-letter queue"
    assert "order-C" in dl_external_ids, "order-C must be in dead-letter queue"
    # Verify the error_class identifies group abort origin
    dl_classes = {row[1] for row in dl_rows}
    assert any("group_partial_failure" in c for c in dl_classes), (
        f"Expected group_partial_failure error_class in DLQ, got: {dl_classes}"
    )


@pytest.mark.anyio
async def test_different_group_ids_are_independent(pool, run_migrations):
    """A failure in one group must not abort rows belonging to a different group.

    T2 #21: Row from group "txn-A" fails; row from group "txn-B" must still
    be dispatched normally without being contaminated by the txn-A failure.
    """
    os.environ["INOUT_CREDENTIAL_ATOMICITY_KEY"] = "dummy"

    delta_table = "_delta_atomicity_group_isolation"
    await _create_delta_table_with_group(pool, delta_table)
    await _insert_group_rows(pool, delta_table, [
        {"external_id": "rec-alpha", "name": "Alpha", "_action": "update", "_group_id": "txn-A"},
        {"external_id": "rec-beta",  "name": "Beta",  "_action": "update", "_group_id": "txn-B"},
    ])

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    assert wb_cfg is not None
    engine = WritebackEngine(pool)

    dispatched_ids: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        ext_id = request.url.path.split("/")[-1]
        dispatched_ids.append(ext_id)
        if ext_id == "rec-alpha":
            return httpx.Response(422, json={"error": "bad record"})
        return httpx.Response(200, json={"id": ext_id})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(re.compile(r"/v1/orders/[\w-]+")).mock(side_effect=handle)

        result = await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, delta_table,
            max_concurrent_writes_override=1,
        )

    assert "rec-alpha" in dispatched_ids, "rec-alpha must be dispatched"
    assert "rec-beta" in dispatched_ids, "rec-beta must still be dispatched despite txn-A failure"

    # rec-alpha fails; rec-beta succeeds
    assert result.failed == 1, f"Only rec-alpha should fail; got {result.failed}"
    assert result.processed == 1, f"rec-beta should succeed; got {result.processed}"


@pytest.mark.anyio
async def test_singleton_rows_without_group_id_are_unaffected(pool, run_migrations):
    """Rows without a _group_id are independent singletons not affected by group aborts.

    T2 #21: A singleton row (no _group_id) must be processed normally even when
    another group's member fails in the same cycle.
    """
    os.environ["INOUT_CREDENTIAL_ATOMICITY_KEY"] = "dummy"

    delta_table = "_delta_atomicity_singleton"
    await _create_delta_table_with_group(pool, delta_table)
    await _insert_group_rows(pool, delta_table, [
        # Group row that will fail
        {"external_id": "grp-row", "name": "Grouped", "_action": "update", "_group_id": "txn-X"},
        # Singleton row (no _group_id) that must succeed independently
        {"external_id": "singleton-row", "name": "Singleton", "_action": "update", "_group_id": None},
    ])

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    assert wb_cfg is not None
    engine = WritebackEngine(pool)

    dispatched_ids: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        ext_id = request.url.path.split("/")[-1]
        dispatched_ids.append(ext_id)
        if ext_id == "grp-row":
            return httpx.Response(422, json={"error": "group row failed"})
        return httpx.Response(200, json={"id": ext_id})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(re.compile(r"/v1/orders/[\w-]+")).mock(side_effect=handle)

        result = await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, delta_table,
            max_concurrent_writes_override=1,
        )

    assert "grp-row" in dispatched_ids
    assert "singleton-row" in dispatched_ids, "Singleton must be dispatched despite group failure"
    assert result.failed == 1, f"Only grp-row should fail; got {result.failed}"
    assert result.processed == 1, f"singleton-row should succeed; got {result.processed}"
