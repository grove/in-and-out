"""Integration tests for WritebackEngine against a real PostgreSQL database."""
from __future__ import annotations

import os
import re

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig, UpdateOperationConfig,
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
    not _docker_available(),
    reason="Docker not available",
)


def _make_connector(base_url: str = "https://api.example.com") -> ConnectorConfig:
    return ConnectorConfig(
        name="test_wb",
        system="TestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": DatatypeConfig(
                writeback=_make_writeback_cfg(),
            )
        },
    )


def _make_writeback_cfg() -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.optimistic,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update", "delete"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/contacts/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/contacts/${external_id}"),
            insert=OperationConfig(method="POST", path="/v1/contacts"),
            delete=OperationConfig(method="DELETE", path="/v1/contacts/${external_id}"),
        ),
    )


async def _create_delta_table(pool, table_name: str) -> None:
    """Create a minimal delta table for writeback tests."""
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


@pytest.mark.anyio
async def test_writeback_dispatches_update_rows(pool):
    """Rows with _action='update' are dispatched via HTTP PATCH."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    delta_table = "_delta_test_wb_contacts_update"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_rows(pool, delta_table, [
        {"external_id": "c1", "name": "Alice", "email": "alice@example.com", "_action": "update"},
        {"external_id": "c2", "name": "Bob", "email": "bob@example.com", "_action": "update"},
    ])

    dispatched: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        dispatched.append(request.url.path)
        return httpx.Response(200, json={"id": request.url.path.split("/")[-1]})

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.patch(re.compile(r"/v1/contacts/\w+")).mock(side_effect=handle)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.processed == 2
    assert result.failed == 0
    assert set(dispatched) == {"/v1/contacts/c1", "/v1/contacts/c2"}


@pytest.mark.anyio
async def test_writeback_dispatches_insert_rows(pool):
    """Rows with _action='insert' are dispatched via HTTP POST."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    delta_table = "_delta_test_wb_contacts_insert"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_rows(pool, delta_table, [
        {"external_id": "c10", "name": "Carol", "email": "carol@example.com", "_action": "insert"},
    ])

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.post("/v1/contacts").mock(return_value=httpx.Response(201, json={"id": "c10"}))

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0


@pytest.mark.anyio
async def test_writeback_dispatches_delete_rows(pool):
    """Rows with _action='delete' are dispatched via HTTP DELETE."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    delta_table = "_delta_test_wb_contacts_delete"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_rows(pool, delta_table, [
        {"external_id": "c99", "name": None, "email": None, "_action": "delete"},
    ])

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.delete("/v1/contacts/c99").mock(return_value=httpx.Response(204))

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0


@pytest.mark.anyio
async def test_writeback_skips_noop_rows(pool):
    """Rows with _action='noop' are excluded from the fetch query."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    delta_table = "_delta_test_wb_contacts_noop"
    await _create_delta_table(pool, delta_table)
    # Insert both noop and update rows
    await _insert_delta_rows(pool, delta_table, [
        {"external_id": "n1", "name": "Noop", "email": None, "_action": "noop"},
        {"external_id": "u1", "name": "Updated", "email": "u@example.com", "_action": "update"},
    ])

    dispatched_paths: list[str] = []

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        def handle(request: httpx.Request) -> httpx.Response:
            dispatched_paths.append(request.url.path)
            return httpx.Response(200, json={})
        mock.patch(re.compile(r"/v1/contacts/\w+")).mock(side_effect=handle)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    # Only the update row should be dispatched
    assert result.processed == 1
    assert dispatched_paths == ["/v1/contacts/u1"]


@pytest.mark.anyio
async def test_writeback_missing_delta_table_skips(pool):
    """Gracefully skips when the delta table does not exist."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False):
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            connector, "contacts", writeback_cfg, "_delta_nonexistent_table_xyz"
        )

    assert result.skipped == 1
    assert result.processed == 0


@pytest.mark.anyio
async def test_writeback_feedback_written_to_result_table(pool, run_migrations):
    """Processed rows are logged to inout_ops_writeback_result."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    delta_table = "_delta_test_wb_contacts_feedback"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_rows(pool, delta_table, [
        {"external_id": "fb1", "name": "Feedback", "email": "fb@example.com", "_action": "update"},
    ])

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.patch("/v1/contacts/fb1").mock(return_value=httpx.Response(200, json={}))

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.processed == 1

    # Verify the feedback row was written
    async with pool.connection() as conn:
        row = await (await conn.execute(
            """SELECT connector, datatype, action, external_id, status
               FROM inout_ops_writeback_result
               WHERE connector = 'test_wb' AND external_id = 'fb1'
               ORDER BY processed_at DESC LIMIT 1"""
        )).fetchone()

    assert row is not None
    assert row[0] == "test_wb"
    assert row[2] == "update"
    assert row[3] == "fb1"
    assert row[4] == "ok"


@pytest.mark.anyio
async def test_writeback_http_failure_increments_failed_count(pool):
    """HTTP errors during dispatch are counted in result.failed."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    delta_table = "_delta_test_wb_contacts_http_fail"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_rows(pool, delta_table, [
        {"external_id": "err1", "name": "Err", "email": None, "_action": "update"},
    ])

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg()

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.patch("/v1/contacts/err1").mock(
            return_value=httpx.Response(500, json={"error": "server error"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0
