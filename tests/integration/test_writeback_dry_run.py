"""Integration tests for T2 #27: writeback dry-run / preview mode.

When ``WritebackConfig.dry_run=True`` the engine must execute all pre-write
logic (conflict detection, payload construction, deduplication) but must NOT
issue any real HTTP calls.  Every would-be write must be captured in
``WritebackResult.dry_run_log`` and counted as ``skipped``.

GOAL.md T2 #27: "dry_run=True → no HTTP calls made, results logged".
"""
from __future__ import annotations

import os

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

_CONNECTOR = "dryrun_test"
_DATATYPE = "invoices"
_BASE_URL = "https://api.dryrun-test.example.com"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
os.environ["INOUT_CREDENTIAL_DRYRUN_KEY"] = "dummy"


def _make_connector(dry_run: bool = True) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DryRunTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="dryrun_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert", "update", "delete"],
                    dry_run=dry_run,
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
                number      TEXT,
                amount      INTEGER,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        await conn.commit()


async def _seed_rows(pool, rows: list[dict]) -> None:
    async with pool.connection() as conn:
        for row in rows:
            await conn.execute(
                f"""
                INSERT INTO {_DELTA_TABLE} (external_id, number, amount, _action)
                VALUES (%(external_id)s, %(number)s, %(amount)s, %(_action)s)
                """,
                row,
            )
        await conn.commit()


@pytest.mark.anyio
async def test_dry_run_no_http_calls(pool):
    """T2 #27: dry_run=True must not dispatch any HTTP requests."""
    await _create_delta_table(pool)
    await _seed_rows(pool, [
        {"external_id": "inv-1", "number": "INV-001", "amount": 100, "_action": "insert"},
        {"external_id": "inv-2", "number": "INV-002", "amount": 200, "_action": "insert"},
    ])

    connector = _make_connector(dry_run=True)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    # respx raises on any unmocked request — if an HTTP call is made the test fails
    with respx.mock(assert_all_called=False) as mock:
        # Register routes — any match would count as a call; we assert none are made
        insert_route = mock.post(f"{_BASE_URL}/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": "inv-1"})
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    assert result.processed == 0, (
        f"dry_run=True must not increment processed (got {result.processed})"
    )
    assert result.skipped >= 2, (
        f"Expected at least 2 skipped entries in dry-run, got {result.skipped}"
    )
    assert insert_route.called is False, (
        "HTTP POST must not be called when dry_run=True"
    )


@pytest.mark.anyio
async def test_dry_run_log_contains_would_be_writes(pool):
    """T2 #27: dry_run_log must contain an entry for each would-be write with method, url, body."""
    await _create_delta_table(pool)
    await _seed_rows(pool, [
        {"external_id": "inv-10", "number": "INV-010", "amount": 500, "_action": "insert"},
    ])

    connector = _make_connector(dry_run=True)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(assert_all_called=False):
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    assert len(result.dry_run_log) >= 1, (
        "dry_run_log must have at least one entry per would-be write"
    )
    entry = result.dry_run_log[0]
    assert "action" in entry, "dry_run_log entry must include 'action'"
    assert "method" in entry, "dry_run_log entry must include 'method'"
    assert "url" in entry, "dry_run_log entry must include 'url'"
    assert "body" in entry, "dry_run_log entry must include 'body'"


@pytest.mark.anyio
async def test_dry_run_false_issues_http_calls(pool):
    """T2 #27 sanity: dry_run=False must still issue HTTP calls (control case)."""
    await _create_delta_table(pool)
    await _seed_rows(pool, [
        {"external_id": "inv-99", "number": "INV-099", "amount": 1, "_action": "insert"},
    ])

    connector = _make_connector(dry_run=False)
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL) as mock:
        insert_route = mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": "inv-99"})
        )
        engine = WritebackEngine(pool=pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, _DELTA_TABLE)

    assert result.processed >= 1, (
        f"dry_run=False must process records (got {result.processed})"
    )
    assert insert_route.called, "HTTP POST must be called when dry_run=False"
    assert len(result.dry_run_log) == 0, (
        f"dry_run_log must be empty when dry_run=False (got {result.dry_run_log})"
    )
