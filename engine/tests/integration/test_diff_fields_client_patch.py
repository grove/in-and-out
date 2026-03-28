"""Integration tests for diff-fields incremental PATCH (T2 #5)."""
from __future__ import annotations

import os

import httpx
import orjson
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig, UpdateOperationConfig,
)
from inandout.postgres.schema import ensure_source_table, source_table_name
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "diff_fields_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.diff-fields.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_DIFF_FIELDS_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_DIFF_FIELDS_TEST_KEY", None)


def _make_connector(diff_fields: bool) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DiffFieldsSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="diff_fields_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    diff_fields=diff_fields,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/contacts/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/contacts/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


async def _seed_last_written(pool, external_id: str, last_written: dict) -> None:
    """Insert a row in the source table with _last_written set."""
    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        # Upsert with _last_written (empty data/raw just for scaffolding)
        await conn.execute(
            f"""
            INSERT INTO {src_table}
                (external_id, data, raw, _raw_hash, _last_written)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (external_id) DO UPDATE
            SET _last_written = EXCLUDED._last_written
            """,
            [
                external_id,
                orjson.dumps(last_written).decode(),
                orjson.dumps({}).decode(),
                "testhash",
                orjson.dumps(last_written).decode(),
            ],
        )
        await conn.commit()


@pytest.mark.anyio
async def test_diff_fields_only_sends_changed_fields(pool, run_migrations):
    """With diff_fields=True, only modified fields are sent in the PATCH body.

    Setup: _last_written = {name: "Alice", status: "active"}
    Delta row: name="Alice" (same), status="inactive" (changed)
    Expected PATCH body: {status: "inactive"} only.
    """
    delta_table = f"inout_delta_{_CONNECTOR}_diff_partial"
    await _seed_last_written(pool, "contact-diff-1", {"name": "Alice", "status": "active"})

    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, status, _action) VALUES (%s, %s, %s, %s)",
            ["contact-diff-1", "Alice", "inactive", "update"],
        )
        await conn.commit()

    connector = _make_connector(diff_fields=True)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patched_bodies: list[dict] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patched_bodies.append(orjson.loads(request.content))
        return httpx.Response(200, json={"id": "contact-diff-1"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-diff-1").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(patched_bodies) == 1
    body = patched_bodies[0]
    assert "status" in body, "Changed field 'status' must appear in PATCH body"
    assert body["status"] == "inactive"
    assert "name" not in body, "Unchanged field 'name' must NOT appear in PATCH body with diff_fields=True"


@pytest.mark.anyio
async def test_diff_fields_skips_when_nothing_changed(pool, run_migrations):
    """With diff_fields=True, a row where all fields match _last_written is skipped."""
    delta_table = f"inout_delta_{_CONNECTOR}_diff_noop"
    await _seed_last_written(pool, "contact-diff-2", {"external_id": "contact-diff-2", "name": "Bob", "status": "active"})

    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        # Identical to _last_written — no actual change
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, status, _action) VALUES (%s, %s, %s, %s)",
            ["contact-diff-2", "Bob", "active", "update"],
        )
        await conn.commit()

    connector = _make_connector(diff_fields=True)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patched: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-diff-2").mock(
            side_effect=lambda req: (patched.append(req), httpx.Response(200, json={}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.skipped >= 1
    assert len(patched) == 0, "PATCH must NOT be sent when no fields changed"


@pytest.mark.anyio
async def test_diff_fields_false_sends_full_payload(pool, run_migrations):
    """With diff_fields=False (default), the full payload is always sent regardless of _last_written."""
    delta_table = f"inout_delta_{_CONNECTOR}_diff_full"
    await _seed_last_written(pool, "contact-diff-3", {"name": "Carol", "status": "active"})

    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        # Only status changed; but diff_fields=False → full payload sent
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, status, _action) VALUES (%s, %s, %s, %s)",
            ["contact-diff-3", "Carol", "inactive", "update"],
        )
        await conn.commit()

    connector = _make_connector(diff_fields=False)
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patched_bodies: list[dict] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patched_bodies.append(orjson.loads(request.content))
        return httpx.Response(200, json={"id": "contact-diff-3"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-diff-3").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(patched_bodies) == 1
    body = patched_bodies[0]
    # Both fields must appear — full payload
    assert "name" in body
    assert "status" in body
