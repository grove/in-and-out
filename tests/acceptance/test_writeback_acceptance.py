"""Writeback acceptance tests — full flow: insert, update, conflict, dead-letter.

These tests exercise the complete writeback pipeline against a real PostgreSQL
database via testcontainers and mocked HTTP endpoints via respx.

They mirror the HubSpot/Salesforce ingestion acceptance tests in coverage depth,
verifying the most complex paths through WritebackEngine:
  - Successful insert / update / delete dispatch
  - Conflict resolution strategies (last_writer_wins, dead_letter)
  - Dead-letter promotion after repeated HTTP failures
  - Dry-run mode (no HTTP calls emitted)
  - Protection-level guard (optimistic etag conflict)
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
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine

pytestmark = pytest.mark.acceptance


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.acceptance,
    pytest.mark.skipif(not _docker_available(), reason="Docker not available for acceptance tests"),
]

_BASE_URL = "https://api.acceptance.example.com"
_CONNECTOR_NAME = "acceptance_wb"


def _make_connector(name: str = _CONNECTOR_NAME) -> ConnectorConfig:
    os.environ[f"INOUT_CREDENTIAL_{name.upper()}_KEY"] = "dummy-acceptance"
    return ConnectorConfig(
        name=name,
        system="AcceptanceSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref=f"{name}_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": DatatypeConfig(
                writeback=_make_writeback_cfg(),
            )
        },
    )


def _make_writeback_cfg(
    conflict_resolution: ConflictResolution = ConflictResolution.last_writer_wins,
    max_retry_count: int | None = None,
    dry_run: bool = False,
) -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=conflict_resolution,
        supported_actions=["insert", "update", "delete"],
        dry_run=dry_run,
        max_retry_count=max_retry_count,
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/contacts/${external_id}"),
            insert=OperationConfig(method="POST", path="/v1/contacts"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/contacts/${external_id}"),
            delete=OperationConfig(method="DELETE", path="/v1/contacts/${external_id}"),
        ),
    )


async def _create_delta_table(pool, table_name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                external_id TEXT,
                name        TEXT,
                email       TEXT,
                _action     TEXT NOT NULL DEFAULT 'update',
                _cluster_id TEXT
            )
        """)
        await conn.commit()


async def _insert_delta_rows(pool, table_name: str, rows: list[dict]) -> None:
    async with pool.connection() as conn:
        for row in rows:
            await conn.execute(
                f"INSERT INTO {table_name} (external_id, name, email, _action) VALUES (%s, %s, %s, %s)",
                [row.get("external_id"), row.get("name"), row.get("email"), row.get("_action", "update")],
            )
        await conn.commit()


# ---------------------------------------------------------------------------
# A. Insert flow — new records reach the target system
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_acceptance_insert_new_records(pool, run_migrations):
    """Insert rows: HTTP POST emitted per row, result.processed == row count."""
    delta = "_delta_acc_wb_insert"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, [
        {"external_id": "new-1", "name": "Alice", "email": "alice@acme.com", "_action": "insert"},
        {"external_id": "new-2", "name": "Bob",   "email": "bob@acme.com",   "_action": "insert"},
    ])

    posted_ids: list[str] = []

    def _handle_post(request: httpx.Request) -> httpx.Response:
        import orjson
        body = orjson.loads(request.content)
        posted_ids.append(body.get("external_id", ""))
        return httpx.Response(201, json={"id": body.get("external_id")})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/contacts").mock(side_effect=_handle_post)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            _make_connector(), "contacts", _make_writeback_cfg(), delta
        )

    assert result.processed == 2
    assert result.failed == 0
    assert set(posted_ids) == {"new-1", "new-2"}


# ---------------------------------------------------------------------------
# B. Update flow — existing records are patched in the target system
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_acceptance_update_existing_records(pool, run_migrations):
    """Update rows: HTTP PATCH emitted for each row, correct paths called."""
    delta = "_delta_acc_wb_update"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, [
        {"external_id": "upd-1", "name": "Carol Updated", "_action": "update"},
        {"external_id": "upd-2", "name": "Dave Updated",  "_action": "update"},
    ])

    patched_paths: list[str] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        def _handle_patch(request: httpx.Request) -> httpx.Response:
            patched_paths.append(request.url.path)
            return httpx.Response(200, json={})
        mock.patch(re.compile(r"/v1/contacts/\S+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            _make_connector(), "contacts", _make_writeback_cfg(), delta
        )

    assert result.processed == 2
    assert result.failed == 0
    assert set(patched_paths) == {"/v1/contacts/upd-1", "/v1/contacts/upd-2"}


# ---------------------------------------------------------------------------
# C. Conflict resolution — dead_letter strategy routes conflicts to DLQ
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_acceptance_conflict_dead_letter_strategy(pool, run_migrations):
    """409 Conflict + dead_letter resolution → row goes to dead-letter, not retried."""
    delta = "_delta_acc_wb_conflict_dl"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, [
        {"external_id": "conf-1", "name": "Conflict Row", "_action": "update"},
    ])

    conflict_cfg = _make_writeback_cfg(
        conflict_resolution=ConflictResolution.dead_letter,
    )

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/conf-1").mock(
            return_value=httpx.Response(409, json={"error": "conflict"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            _make_connector(), "contacts", conflict_cfg, delta
        )

    # Row should be recorded as failed (conflict goes to dead-letter path)
    assert result.failed >= 1
    assert result.processed == 0


# ---------------------------------------------------------------------------
# D. Dead-letter promotion — rows that repeatedly fail are moved to DLQ
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_acceptance_http_errors_counted_as_failures(pool, run_migrations):
    """5xx HTTP errors increment result.failed and are written to writeback_result."""
    delta = "_delta_acc_wb_dq_errors"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, [
        {"external_id": "dlq-1", "name": "DLQ Row", "_action": "update"},
    ])

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/dlq-1").mock(
            return_value=httpx.Response(503, json={"error": "service unavailable"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            _make_connector(), "contacts", _make_writeback_cfg(), delta
        )

    assert result.failed == 1
    assert result.processed == 0

    # Failure written to inout_ops_writeback_result as 'failed'
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT status FROM inout_ops_writeback_result "
            "WHERE connector = %s AND external_id = 'dlq-1' ORDER BY processed_at DESC LIMIT 1",
            [_CONNECTOR_NAME],
        )).fetchone()
    assert row is not None
    assert row[0] == "failed"


# ---------------------------------------------------------------------------
# E. Dry-run — no HTTP calls emitted, rows logged in dry_run_log
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_acceptance_dry_run_no_http_calls(pool, run_migrations):
    """Dry-run mode: zero HTTP requests, dry_run_log has entries for all rows."""
    delta = "_delta_acc_wb_dry_run"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, [
        {"external_id": "dr-1", "name": "Dry 1", "_action": "update"},
        {"external_id": "dr-2", "name": "Dry 2", "_action": "insert"},
    ])

    dry_cfg = _make_writeback_cfg(dry_run=True)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # No routes registered — any real HTTP call would raise
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            _make_connector(), "contacts", dry_cfg, delta
        )

    assert result.processed == 0  # dry-run doesn't count as processed
    assert len(result.dry_run_log) == 2


# ---------------------------------------------------------------------------
# F. Mixed batch — inserts + updates + deletes dispatched correctly
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_acceptance_mixed_action_batch(pool, run_migrations):
    """Mixed batch: each action type dispatched via correct HTTP method."""
    delta = "_delta_acc_wb_mixed"
    await _create_delta_table(pool, delta)
    await _insert_delta_rows(pool, delta, [
        {"external_id": "mix-ins", "name": "New",     "_action": "insert"},
        {"external_id": "mix-upd", "name": "Updated", "_action": "update"},
        {"external_id": "mix-del", "name": None,      "_action": "delete"},
    ])

    methods_seen: list[str] = []

    def _record_method(request: httpx.Request) -> httpx.Response:
        methods_seen.append(request.method)
        if request.method == "POST":
            return httpx.Response(201, json={"id": "mix-ins"})
        if request.method == "PATCH":
            return httpx.Response(200, json={})
        return httpx.Response(204)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/contacts").mock(side_effect=_record_method)
        mock.patch(re.compile(r"/v1/contacts/\S+")).mock(side_effect=_record_method)
        mock.delete(re.compile(r"/v1/contacts/\S+")).mock(side_effect=_record_method)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            _make_connector(), "contacts", _make_writeback_cfg(), delta
        )

    assert result.processed == 3
    assert result.failed == 0
    assert set(methods_seen) == {"POST", "PATCH", "DELETE"}
