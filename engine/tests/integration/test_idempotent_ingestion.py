"""Integration tests for T1 #47: idempotent upsert writes (crash-and-replay).

Re-processing the same source records after a simulated crash must produce
exactly the same final table state — no duplicates, no lost updates.

GOAL.md T1 #47: "A crash and restart that re-processes already-written records
must produce the same final table state — no duplicates, no lost updates. In
`append` history mode (#30), the combination of `external_id` and
`_sync_run_id` must be used as the deduplication key."
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
from inandout.config.ingestion import (
    IngestionConfig,
    HistoryMode,
    ListConfig,
    ScheduleConfig,
)
from inandout.config.pagination import (
    PaginationConfig,
    PaginationStrategy,
    CursorConfig,
)
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import source_table_name


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

_CONNECTOR = "idempotent_ingest"
_DATATYPE = "products"
_BASE_URL = "https://api.idempotent-test.example.com"
os.environ["INOUT_CREDENTIAL_IDEMPOTENT_KEY"] = "dummy"

_RECORDS = [
    {"id": "p1", "name": "Widget A", "price": 10},
    {"id": "p2", "name": "Widget B", "price": 20},
    {"id": "p3", "name": "Widget C", "price": 30},
]


def _make_connector(history_mode: HistoryMode = HistoryMode.overwrite) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="IdempotentTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="idempotent_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=history_mode,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/products",
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


def _mock_response(records: list | None = None) -> httpx.Response:
    return httpx.Response(
        200,
        json={"results": records if records is not None else _RECORDS, "next_cursor": None},
    )


@pytest.mark.anyio
async def test_replay_same_records_no_duplicates(pool):
    """T1 #47: running the same sync twice must not create duplicate rows in the source table."""
    connector = _make_connector()
    table = source_table_name(_CONNECTOR, _DATATYPE)

    # First sync — initial load
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/products").mock(return_value=_mock_response())
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result1.status == "completed"
    assert result1.records_inserted == 3

    # Second sync — same records (simulated crash-and-replay)
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/products").mock(return_value=_mock_response())
        engine2 = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result2.status == "completed"
    # Identical data → everything should be a no-op (0 inserts, 0 updates)
    assert result2.records_inserted == 0, (
        f"Re-processing identical records must not insert again (got {result2.records_inserted})"
    )
    assert result2.records_updated == 0, (
        f"Re-processing identical records must not update (got {result2.records_updated})"
    )

    # Source table must have exactly 3 rows — no duplicates
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        )).fetchone()
    assert row[0] == 3, f"Expected 3 rows after two identical syncs, got {row[0]}"


@pytest.mark.anyio
async def test_replay_with_updated_record_applies_update(pool):
    """T1 #47: replay with one updated record must apply only that update, not re-insert all."""
    # Use a distinct connector name to avoid table collision with the previous test
    connector_name = _CONNECTOR + "_update"
    connector = ConnectorConfig(
        name=connector_name,
        system="IdempotentTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="idempotent_key",
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
                            path="/v1/products",
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
    table = source_table_name(connector_name, _DATATYPE)

    # First sync
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/products").mock(return_value=_mock_response())
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result1.records_inserted == 3

    # Second sync — p2 has an updated price
    updated = [
        {"id": "p1", "name": "Widget A", "price": 10},          # unchanged
        {"id": "p2", "name": "Widget B", "price": 99},           # changed price
        {"id": "p3", "name": "Widget C", "price": 30},          # unchanged
    ]
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/products").mock(return_value=_mock_response(updated))
        engine2 = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result2.status == "completed"
    # Only the changed record should count as updated
    assert result2.records_inserted == 0
    assert result2.records_updated == 1, (
        f"Expected exactly 1 update for the changed record, got {result2.records_updated}"
    )

    # Source table still has exactly 3 rows
    async with pool.connection() as conn:
        count_row = await (await conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        )).fetchone()
        data_row = await (await conn.execute(
            f"SELECT data->>'price' FROM {table} WHERE external_id = 'p2'"
        )).fetchone()

    assert count_row[0] == 3
    assert data_row is not None
    assert data_row[0] == "99", f"Expected updated price=99, got {data_row[0]!r}"


@pytest.mark.anyio
async def test_history_mode_append_deduplicates_across_runs(pool):
    """T1 #47: in 'append' history mode replaying identical records within the same run must not create duplicate history entries."""
    # Use a distinct connector name to avoid cross-test table collisions
    connector_name = _CONNECTOR + "_hist"
    connector = ConnectorConfig(
        name=connector_name,
        system="IdempotentTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="idempotent_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.append,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/products",
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

    hist_table = source_table_name(connector_name, _DATATYPE) + "_history"

    # First sync — seeds 3 history rows (one per record)
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/products").mock(return_value=_mock_response())
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result1.status == "completed"
    assert result1.records_inserted == 3

    # Second sync — exact same data, different run_id → history dedup on (external_id, raw_hash)
    # prevents duplicate rows for unchanged records
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/products").mock(return_value=_mock_response())
        engine2 = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result2.status == "completed"
    assert result2.records_inserted == 0  # all identical → no new history rows

    # History table must have exactly 3 rows total (one per unique record)
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT COUNT(*) FROM {hist_table}"
        )).fetchone()

    assert row[0] == 3, (
        f"History table should have 3 unique entries after two identical syncs, got {row[0]}"
    )
