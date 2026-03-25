"""Integration tests for ETag / If-Match conditional write protection (T2 #2, T2 #38).

When ProtectionLevel.optimistic is set, the writeback engine:
1. Fetches the current record via ops.lookup and captures the ETag from the response
2. Injects the ETag as an If-Match header on the subsequent PATCH request
3. Handles 412 Precondition Failed (concurrent write detected) by recording a
   conflict, skipping the write, and not propagating an exception

Covers:
- Lookup ETag is injected as If-Match header on PATCH (optimistic protection)
- 412 response causes conflict count increment and write skip
- base_version in delta row is used as If-Match without an extra GET round-trip
"""
from __future__ import annotations

import os

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

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.etag-test.example.com"
_CONNECTOR = "etag_test"
_DATATYPE = "accounts"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_ETAG_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_ETAG_TEST_KEY", None)


def _make_optimistic_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="ETagSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="etag_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    etag_header="ETag",
                    if_match_header="If-Match",
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/accounts/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/accounts/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_optimistic_etag_injected_as_if_match(pool, run_migrations):
    """T2 #2: optimistic protection fetches ETag from lookup and injects If-Match on PATCH."""
    delta_table = f"inout_delta_{_CONNECTOR}_etag_inject"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["account-etag-1", "Acme Corp", "update"],
        )
        await conn.commit()

    connector = _make_optimistic_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    captured_if_match: list[str] = []

    def _lookup_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"ETag": '"v_abc123"'},
            json={"id": "account-etag-1", "name": "Old Corp"},
        )

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        captured_if_match.append(request.headers.get("If-Match", ""))
        return httpx.Response(200, json={"id": "account-etag-1"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/accounts/account-etag-1").mock(side_effect=_lookup_handler)
        mock.patch("/v1/accounts/account-etag-1").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(captured_if_match) == 1, "PATCH should be called once"
    assert captured_if_match[0] == '"v_abc123"', (
        f"If-Match header should contain the ETag from lookup; got {captured_if_match[0]!r}"
    )


@pytest.mark.anyio
async def test_412_response_counts_as_conflict_and_skips(pool, run_migrations):
    """T2 #2: PATCH returning 412 is recorded as a conflict and the write is skipped (not failed)."""
    delta_table = f"inout_delta_{_CONNECTOR}_412"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["account-412-1", "Conflict Corp", "update"],
        )
        await conn.commit()

    connector = _make_optimistic_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Lookup returns a current ETag
        mock.get("/v1/accounts/account-412-1").mock(
            return_value=httpx.Response(200, headers={"ETag": '"v1"'}, json={"id": "account-412-1"})
        )
        # PATCH returns 412 — another writer has modified the record
        mock.patch("/v1/accounts/account-412-1").mock(
            return_value=httpx.Response(412, json={"error": "precondition_failed"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.conflicts == 1, f"Expected 1 conflict; got {result}"
    assert result.skipped >= 1, f"Expected at least 1 skipped write after 412; got {result}"
    assert result.failed == 0, "412 Precondition Failed should not count as a failure"
    assert result.processed == 0


@pytest.mark.anyio
async def test_base_version_in_row_bypasses_lookup_get(pool, run_migrations):
    """T2 #2: when delta row has base_version, no GET is made; base_version is used as If-Match."""
    delta_table = f"inout_delta_{_CONNECTOR}_base_ver"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id  TEXT,
                name         TEXT,
                base_version TEXT,
                _action      TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, base_version, _action) VALUES (%s, %s, %s, %s)",
            ["account-bv-1", "Versioned Corp", '"v_stored_99"', "update"],
        )
        await conn.commit()

    # Use a no-lookup-required connector (protection_level=none, no GET needed)
    # but the base_version field should still be injected as If-Match when present
    connector = ConnectorConfig(
        name=_CONNECTOR,
        system="ETagSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="etag_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    etag_header="ETag",
                    if_match_header="If-Match",
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/accounts/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/accounts/${{external_id}}"),
                    ),
                ),
            ),
        },
    )
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    get_call_count = [0]
    captured_if_match: list[str] = []

    def _lookup_handler(request: httpx.Request) -> httpx.Response:
        get_call_count[0] += 1
        return httpx.Response(200, headers={"ETag": '"v_from_get"'}, json={"id": "account-bv-1"})

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        captured_if_match.append(request.headers.get("If-Match", ""))
        return httpx.Response(200, json={"id": "account-bv-1"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/accounts/account-bv-1").mock(side_effect=_lookup_handler)
        mock.patch("/v1/accounts/account-bv-1").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    # base_version in row means If-Match should be injected
    assert len(captured_if_match) == 1
    assert captured_if_match[0] == '"v_stored_99"', (
        f"If-Match should use base_version from row; got {captured_if_match[0]!r}"
    )
    # The GET should NOT have been called (base_version from row bypasses lookup)
    assert get_call_count[0] == 0, (
        f"Lookup GET should not be called when base_version is in the row; was called {get_call_count[0]} times"
    )
