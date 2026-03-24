"""Integration tests for T1 #33: intra-sync deduplication.

During a single sync run the same record may appear multiple times — either
via duplicate entries in a paginated result set, or via overlap between a
webhook event and the polling full-sync.  The tool must deduplicate records
within a run by primary key, ensuring each object is written at most once per
sync cycle.

GOAL.md T1 #33: "The tool must deduplicate records within a run by primary key
before writing, ensuring each object is written at most once per sync cycle."
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

_CONNECTOR = "intra_dedup_test"
_DATATYPE = "tasks"
_BASE_URL = "https://api.intra-dedup.example.com"
os.environ["INOUT_CREDENTIAL_INTRA_DEDUP_KEY"] = "dummy"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="IntraDedupSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="intra_dedup_key",
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
                            path="/v1/tasks",
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
async def test_duplicate_records_in_page_written_once(pool):
    """T1 #33: same external_id appearing twice in one page → only one row in source table."""
    connector = _make_connector()
    table = source_table_name(_CONNECTOR, _DATATYPE)
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    # Page deliberately contains t1 twice (pagination bug / API bug)
    page = [
        {"id": "t1", "title": "Task A", "status": "open"},
        {"id": "t2", "title": "Task B", "status": "open"},
        {"id": "t1", "title": "Task A", "status": "open"},   # duplicate
    ]

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/tasks").mock(
            return_value=httpx.Response(
                200,
                json={"results": page, "next_cursor": None},
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    # Only 2 unique records exist — the duplicate must be silently dropped
    assert result.records_inserted == 2, (
        f"Expected 2 inserts (t1+t2), got {result.records_inserted}; "
        "the duplicate t1 must NOT be inserted again"
    )

    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        )).fetchone()
    assert row[0] == 2, f"Expected 2 rows in source table, got {row[0]}"


@pytest.mark.anyio
async def test_duplicate_across_pages_written_once(pool):
    """T1 #33: same external_id appearing on two different pages in one sync → one row."""
    # Use a distinct connector to avoid table collision
    connector_name = _CONNECTOR + "_pages"
    connector = ConnectorConfig(
        name=connector_name,
        system="IntraDedupSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="intra_dedup_key",
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
                            path="/v1/tasks",
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
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    call_count = 0

    def _page_response(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        cursor = request.url.params.get("cursor")
        if cursor == "page2":
            # Page 2: t3 and t1 again (drift/overlap)
            return httpx.Response(
                200,
                json={"results": [{"id": "t3", "title": "Task C", "status": "done"}, {"id": "t1", "title": "Task A v2", "status": "closed"}], "next_cursor": None},
            )
        # Page 1
        return httpx.Response(
            200,
            json={"results": [{"id": "t1", "title": "Task A", "status": "open"}, {"id": "t2", "title": "Task B", "status": "open"}], "next_cursor": "page2"},
        )

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/tasks").mock(side_effect=_page_response)
        engine = IngestionEngine(pool=pool, namespace="test")
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    # t1, t2 from page 1; t3 from page 2; t1 on page 2 is duplicate → skipped
    # records_inserted counts unique new records
    assert result.records_inserted == 3, (
        f"Expected 3 inserts (t1, t2, t3), got {result.records_inserted}"
    )

    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        )).fetchone()
    assert row[0] == 3, f"Expected 3 rows (t1 from page1 wins), got {row[0]}"


@pytest.mark.anyio
async def test_intra_sync_dedup_does_not_affect_next_run(pool):
    """T1 #33: the intra-sync dedup set must be cleared between runs (records appear again next sync)."""
    connector_name = _CONNECTOR + "_cross_run"
    connector = ConnectorConfig(
        name=connector_name,
        system="IntraDedupSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="intra_dedup_key",
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
                            path="/v1/tasks",
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
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    records = [{"id": "tx1", "title": "Persistent", "status": "active"}]

    # First sync
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/tasks").mock(
            return_value=httpx.Response(200, json={"results": records, "next_cursor": None})
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        r1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert r1.records_inserted == 1

    # Second sync — same records, different engine instance (fresh in_run_seen)
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/tasks").mock(
            return_value=httpx.Response(200, json={"results": records, "next_cursor": None})
        )
        engine2 = IngestionEngine(pool=pool, namespace="test")
        r2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert r2.status == "completed"
    # Second run: same record → no new insert (idempotent), no duplication
    # The intra-sync set from run 1 must NOT bleed into run 2
    assert r2.records_inserted == 0
    assert r2.records_updated == 0
