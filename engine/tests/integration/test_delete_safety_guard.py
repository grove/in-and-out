"""Integration tests for T2 #31: delete safety guard.

When ``WritebackConfig.max_deletes_per_batch`` is set to `N`, any batch that
contains more than `N` ``_action='delete'`` rows must have ALL its delete rows
stripped and counted as ``skipped`` — preventing accidental bulk-deletes from
a bad data pipeline.  Non-delete rows in the same batch must still be
processed normally.

GOAL.md T2 #31: "Delete safety guard: verify record exists + state matches
before delete; abort deletes when batch exceeds ``max_deletes_per_batch``."
"""
from __future__ import annotations

import os
import re

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

_CONNECTOR = "delete_guard_test"
_DATATYPE = "subscriptions"
_BASE_URL = "https://api.delete-guard.example.com"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
os.environ["INOUT_CREDENTIAL_GUARD_KEY"] = "dummy"


def _make_connector(max_deletes: int | None = 2) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DeleteGuardSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="guard_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert", "update", "delete"],
                    max_deletes_per_batch=max_deletes,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{_DATATYPE}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        delete=OperationConfig(method="DELETE", path=f"/v1/{_DATATYPE}/${{external_id}}"),
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
                plan        TEXT,
                _action     TEXT NOT NULL DEFAULT 'delete'
            )
        """)
        await conn.commit()


async def _seed_rows(pool, rows: list[dict]) -> None:
    async with pool.connection() as conn:
        for row in rows:
            await conn.execute(
                f"INSERT INTO {_DELTA_TABLE} (external_id, plan, _action) "
                "VALUES (%(external_id)s, %(plan)s, %(_action)s)",
                row,
            )
        await conn.commit()


async def _clear_table(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"DELETE FROM {_DELTA_TABLE}")
        await conn.commit()


@pytest.mark.anyio
async def test_delete_guard_trips_and_strips_all_deletes(pool):
    """T2 #31: when delete_count > max_deletes_per_batch all deletes are stripped and skipped."""
    await _create_delta_table(pool)
    await _clear_table(pool)

    # 5 deletes — exceeds max_deletes_per_batch=2
    await _seed_rows(pool, [
        {"external_id": f"sub-{i}", "plan": "pro", "_action": "delete"}
        for i in range(5)
    ])

    connector = _make_connector(max_deletes=2)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        delete_route = mock.delete(re.compile(r"/v1/subscriptions/.*")).mock(
            return_value=httpx.Response(204)
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    # All 5 deletes must be skipped — none dispatched
    assert delete_route.called is False, (
        "HTTP DELETE must not be called when delete safety guard trips"
    )
    assert result.skipped == 5, (
        f"Expected 5 skipped (all deletes stripped), got {result.skipped}"
    )
    assert result.failed == 0


@pytest.mark.anyio
async def test_delete_guard_does_not_affect_non_delete_rows(pool):
    """T2 #31: non-delete rows in the same batch must still be processed when the guard trips."""
    await _create_delta_table(pool)
    await _clear_table(pool)

    # 3 deletes (exceeds limit=2) + 1 insert
    await _seed_rows(pool, [
        {"external_id": "sub-a", "plan": "basic", "_action": "delete"},
        {"external_id": "sub-b", "plan": "basic", "_action": "delete"},
        {"external_id": "sub-c", "plan": "basic", "_action": "delete"},
        {"external_id": "sub-new", "plan": "enterprise", "_action": "insert"},
    ])

    connector = _make_connector(max_deletes=2)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        delete_route = mock.delete(re.compile(r"/v1/subscriptions/.*")).mock(
            return_value=httpx.Response(204)
        )
        insert_route = mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": "sub-new"})
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    # Deletes stripped; insert should proceed
    assert delete_route.called is False, "DELETE must not be called after guard trips"
    assert insert_route.called, "POST (insert) must still be dispatched after guard strips deletes"
    assert result.skipped == 3, f"Expected 3 skipped (stripped deletes), got {result.skipped}"
    assert result.processed >= 1, f"Expected at least 1 processed (the insert), got {result.processed}"


@pytest.mark.anyio
async def test_delete_guard_allows_within_limit(pool):
    """T2 #31: deletes at or below the limit must be dispatched normally."""
    await _create_delta_table(pool)
    await _clear_table(pool)

    # Exactly 2 deletes — equals max_deletes_per_batch=2, must NOT trip the guard
    await _seed_rows(pool, [
        {"external_id": "sub-x", "plan": "pro", "_action": "delete"},
        {"external_id": "sub-y", "plan": "pro", "_action": "delete"},
    ])

    connector = _make_connector(max_deletes=2)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.delete(re.compile(r"/v1/subscriptions/.*")).mock(
            return_value=httpx.Response(204)
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    # Guard fires only when delete_count EXCEEDS limit, not when equal
    assert result.skipped == 0, (
        f"Guard must not trip when delete_count == max_deletes_per_batch (skipped={result.skipped})"
    )
    assert result.processed == 2, (
        f"Both deletes should be processed (got {result.processed})"
    )
