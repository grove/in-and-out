"""Integration tests for writeback politeness and 429 rate-limit handling (T2 #11, T1 #18).

Covers:
- Writeback transparently retries on 429 with Retry-After and eventually succeeds
- After all retry attempts exhausted by persistent 429s, the write is classified as failed
- Ingestion 429 retry: a 429-then-200 fetch cycle completes as a successful sync
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

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
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.rate-limit-test.example.com"
_CONNECTOR = "rate_limit_test"
_DATATYPE = "contacts"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_RATE_LIMIT_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_RATE_LIMIT_TEST_KEY", None)


def _make_writeback_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="RateLimitSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="rate_limit_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/contacts/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/contacts/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


def _make_ingestion_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="RateLimitSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="rate_limit_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/contacts",
                            record_selector="results",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_writeback_retries_on_429_and_succeeds(pool, run_migrations):
    """T2 #11: a PATCH that receives 429 with Retry-After is retried and on 200 is counted as processed."""
    delta_table = f"inout_delta_{_CONNECTOR}_rate_retry"
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
            ["contact-rl-1", "Alice", "update"],
        )
        await conn.commit()

    connector = _make_writeback_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    call_count = 0

    def patch_side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First attempt: 429 with short Retry-After
            return httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "rate limited"})
        # Second attempt: success
        return httpx.Response(200, json={})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch("/v1/contacts/contact-rl-1").mock(side_effect=patch_side_effect)

        with patch("anyio.sleep", new_callable=AsyncMock):
            engine = WritebackEngine(pool)
            result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1, f"Expected processed=1 after 429+retry, got {result}"
    assert result.failed == 0
    assert call_count == 2, f"Expected 2 PATCH attempts (429 then 200), got {call_count}"


@pytest.mark.anyio
async def test_writeback_fails_after_exhausting_429_retries(pool, run_migrations):
    """T2 #11: if all retry attempts return 429, the row is classified as failed."""
    delta_table = f"inout_delta_{_CONNECTOR}_rate_exhaust"
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
            ["contact-rl-2", "Bob", "update"],
        )
        await conn.commit()

    connector = _make_writeback_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Always return 429 — exhaust retries
        mock.patch("/v1/contacts/contact-rl-2").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "too many requests"})
        )

        with patch("anyio.sleep", new_callable=AsyncMock):
            # Use max_retries=1 to keep test fast
            from inandout.transport.http import HttpTransportAdapter
            original_init = HttpTransportAdapter.__init__

            def patched_init(self, connector, max_retries=1, **kwargs):
                original_init(self, connector, max_retries=1, **kwargs)

            with patch.object(HttpTransportAdapter, "__init__", patched_init):
                engine = WritebackEngine(pool)
                result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.failed == 1, f"Expected failed=1 when all retries exhausted, got {result}"
    assert result.processed == 0


@pytest.mark.anyio
async def test_ingestion_429_retried_sync_succeeds(pool):
    """T1 #18: ingestion that receives 429 on page 1 and retries successfully completes the sync."""
    connector = _make_ingestion_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "rate limited"})
        return httpx.Response(200, json={"results": [{"id": "c-1", "name": "Alice"}], "next_cursor": None})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(side_effect=side_effect)

        with patch("anyio.sleep", new_callable=AsyncMock):
            engine = IngestionEngine(pool)
            result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_fetched >= 1
