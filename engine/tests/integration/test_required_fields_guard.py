"""Integration tests for required-fields guard (T2 #35)."""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig,
)
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "req_fields_test"
_DATATYPE = "leads"
_BASE_URL = "https://api.req-fields-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_REQ_FIELDS_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_REQ_FIELDS_TEST_KEY", None)


def _make_connector(required_fields: list[str]) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="ReqFieldsSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="req_fields_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    required_fields=required_fields,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/leads/${{external_id}}"),
                        insert=OperationConfig(method="POST", path="/v1/leads"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_required_fields_guard_blocks_row_missing_field(pool):
    """A row missing a required field is failed without sending an HTTP request."""
    delta_table = f"inout_delta_{_CONNECTOR}_guard_miss"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        # email is missing from this row
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["lead-1", "Alice", "insert"],
        )
        await conn.commit()

    connector = _make_connector(required_fields=["email"])
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/leads").mock(
            side_effect=lambda req: (posted.append(req), httpx.Response(201, json={"id": "lead-1"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0
    assert len(posted) == 0, "POST must NOT be sent when required field is missing"
    # The failed entry should contain the reason
    assert any("required_fields_missing" in e[2] for e in result._failed_entries)


@pytest.mark.anyio
async def test_required_fields_guard_passes_when_all_fields_present(pool):
    """A row with all required fields present is processed normally."""
    delta_table = f"inout_delta_{_CONNECTOR}_guard_ok"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                email       TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, email, _action) VALUES (%s, %s, %s, %s)",
            ["lead-2", "Bob", "bob@example.com", "insert"],
        )
        await conn.commit()

    connector = _make_connector(required_fields=["email"])
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    posted: list = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post("/v1/leads").mock(
            side_effect=lambda req: (posted.append(req), httpx.Response(201, json={"id": "lead-2"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(posted) == 1


@pytest.mark.anyio
async def test_required_fields_guard_checks_multiple_fields(pool):
    """Multiple required fields: all absent → fails; only one absent → still fails."""
    delta_table = f"inout_delta_{_CONNECTOR}_guard_multi"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert'
            )
        """)
        # Missing both email and phone
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["lead-3", "Carol", "insert"],
        )
        await conn.commit()

    connector = _make_connector(required_fields=["email", "phone"])
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False):
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0
    entry = result._failed_entries[0]
    assert "email" in entry[2] or "phone" in entry[2]
