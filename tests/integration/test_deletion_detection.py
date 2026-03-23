"""Integration tests for full-sync deletion detection (tombstone pass)."""
from __future__ import annotations

import os

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine


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


def _make_connector(name: str = "test_deletion") -> ConnectorConfig:
    return ConnectorConfig(
        name=name,
        system="TestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/items",
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
                )
            )
        },
    )


@pytest.mark.anyio
async def test_tombstone_missing_records_after_full_sync(pool):
    """Records absent from a full sync are soft-deleted (_deleted_at set)."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_connector("test_tombstone")
    ingestion_cfg = connector.datatypes["items"].ingestion
    assert ingestion_cfg is not None

    # First sync: insert records 1, 2, 3
    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/items").mock(return_value=httpx.Response(
            200, json={"results": [{"id": "1"}, {"id": "2"}, {"id": "3"}], "next_cursor": None}
        ))
        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, "items", ingestion_cfg)

    assert result1.status == "completed"
    assert result1.records_inserted == 3

    # Second sync: only records 1 and 2 returned — record 3 is gone
    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/items").mock(return_value=httpx.Response(
            200, json={"results": [{"id": "1"}, {"id": "2"}], "next_cursor": None}
        ))
        result2 = await engine.run_sync(connector, "items", ingestion_cfg)

    assert result2.status == "completed"
    assert result2.records_deleted == 1

    # Verify record "3" is tombstoned
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT _deleted_at FROM inout_src_test_tombstone_items WHERE external_id = '3'"
        )).fetchone()
    assert row is not None
    assert row[0] is not None  # _deleted_at should be set


@pytest.mark.anyio
async def test_tombstone_circuit_breaker_trips_on_mass_deletion(pool):
    """If > 50% of records would be deleted, the tombstone pass is skipped."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_connector("test_circuit")
    ingestion_cfg = connector.datatypes["items"].ingestion
    assert ingestion_cfg is not None

    # First sync: insert 10 records
    records_10 = [{"id": str(i)} for i in range(10)]
    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/items").mock(return_value=httpx.Response(
            200, json={"results": records_10, "next_cursor": None}
        ))
        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, "items", ingestion_cfg)

    assert result1.records_inserted == 10

    # Second sync: only 3 records returned — 7 would be deleted (70% > 50% threshold)
    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/items").mock(return_value=httpx.Response(
            200, json={"results": [{"id": "0"}, {"id": "1"}, {"id": "2"}], "next_cursor": None}
        ))
        result2 = await engine.run_sync(connector, "items", ingestion_cfg)

    # Circuit breaker should have suppressed deletions
    assert result2.records_deleted == 0

    # All 10 records should still be visible (not tombstoned)
    async with pool.connection() as conn:
        count_row = await (await conn.execute(
            "SELECT COUNT(*) FROM inout_src_test_circuit_items WHERE _deleted_at IS NULL"
        )).fetchone()
    assert count_row is not None
    assert count_row[0] == 10
