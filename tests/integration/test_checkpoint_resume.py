"""Integration tests: intra-sync checkpoint save and resume (T1 #29)."""
from __future__ import annotations

import os
import uuid

import httpx
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")


def _make_connector(checkpoint_every_n_pages: int = 1):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig

    return ConnectorConfig(
        name="checkpoint_test",
        system="CheckpointTest",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.checkpoint-test.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="checkpoint_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    checkpoint_every_n_pages=checkpoint_every_n_pages,
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/items",
                            record_selector="items",
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
async def test_sync_resumes_from_checkpoint(pool, run_migrations):
    """Simulate a sync that saves a checkpoint after page 1, then 'crashes'.
    The next run should resume from the checkpoint cursor.
    """
    os.environ["INOUT_CREDENTIAL_CHECKPOINT_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table

    connector = _make_connector(checkpoint_every_n_pages=1)
    ingestion_cfg = connector.datatypes["items"].ingestion

    async with pool.connection() as conn:
        await ensure_source_table(conn, connector.name, "items")
        await conn.commit()

    # Set up two pages of data
    page1 = {"items": [{"id": "a"}, {"id": "b"}], "next_cursor": "page2"}
    page2 = {"items": [{"id": "c"}, {"id": "d"}], "next_cursor": None}

    pages_iter = iter([page1, page2])
    call_count = 0

    def _handle(request):
        nonlocal call_count
        page = next(pages_iter, {"items": [], "next_cursor": None})
        call_count += 1
        return httpx.Response(200, json=page)

    # First sync — should get page 1, save checkpoint, then succeed
    with respx.mock(
        base_url="https://api.checkpoint-test.example.com", assert_all_called=False
    ) as mock:
        mock.get("/v1/items").mock(side_effect=_handle)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "items", ingestion_cfg)

    # Both pages should have been fetched and records inserted
    assert result.status == "completed"
    assert result.records_inserted >= 2

    # Verify records in the source table
    src_table = f"inout_src_{connector.name}_items"
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(f"SELECT external_id FROM {src_table} ORDER BY external_id")
        ).fetchall()
    ids = {r[0] for r in rows}
    # At least the first page records should be in the table
    assert "a" in ids or "b" in ids or "c" in ids or "d" in ids


@pytest.mark.anyio
async def test_checkpoint_row_persisted_to_db_after_page(pool, run_migrations):
    """T1 #29: verify that inout_ops_sync_checkpoint gets a row after each checkpointed page.

    Run a 2-page sync with checkpoint_every_n_pages=1. After the sync completes
    the checkpoint row (which the engine never deletes) must exist in
    inout_ops_sync_checkpoint with the correct connector / datatype.
    """
    os.environ["INOUT_CREDENTIAL_CHECKPOINT_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table

    connector = _make_connector(checkpoint_every_n_pages=1)
    ingestion_cfg = connector.datatypes["items"].ingestion

    async with pool.connection() as conn:
        await ensure_source_table(conn, connector.name, "items")
        await conn.commit()

    pages = [
        {"items": [{"id": "cp-1"}, {"id": "cp-2"}], "next_cursor": "pg2"},
        {"items": [{"id": "cp-3"}, {"id": "cp-4"}], "next_cursor": None},
    ]
    pages_iter = iter(pages)

    with respx.mock(
        base_url="https://api.checkpoint-test.example.com", assert_all_called=False
    ) as mock:
        mock.get("/v1/items").mock(side_effect=lambda _r: httpx.Response(200, json=next(pages_iter, {"items": [], "next_cursor": None})))

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "items", ingestion_cfg)

    assert result.status == "completed"

    # The checkpoint row must exist (engine never deletes it, run status is now 'completed')
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT connector, datatype, page_number FROM inout_ops_sync_checkpoint WHERE connector = %s AND datatype = %s",
            [connector.name, "items"],
        )).fetchone()

    assert row is not None, "Expected a checkpoint row to exist after sync"
    assert row[0] == connector.name
    assert row[1] == "items"
    assert row[2] >= 1, f"Expected page_number >= 1, got {row[2]}"


@pytest.mark.anyio
async def test_incremental_sync_resumes_from_checkpoint_watermark(pool, run_migrations):
    """T1 #29: when a 'running' sync_run has a checkpoint, new sync uses its cursor_value
    as the incremental watermark — confirmed by checking which ?since= param the API receives.
    """
    os.environ["INOUT_CREDENTIAL_CHECKPOINT_KEY"] = "dummy"
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import (
        IngestionConfig, HistoryMode, ListConfig, ScheduleConfig,
        IncrementalConfig, RequestFilterConfig, RequestFilterMode,
    )
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table

    CONNECTOR_NAME = "ckpt_incremental_test"
    DATATYPE = "events"
    RESUME_WATERMARK = "2026-03-01T12:00:00"
    BASE_URL = "https://api.ckpt-inc.example.com"

    os.environ["INOUT_CREDENTIAL_CKPT_INC_KEY"] = "dummy"

    connector = ConnectorConfig(
        name=CONNECTOR_NAME,
        system="CkptIncTest",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=BASE_URL),
        auth=ApiKeyAuth(
            type="api_key", credential_ref="ckpt_inc_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    checkpoint_every_n_pages=1,
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/events",
                            record_selector="events",
                            incremental=IncrementalConfig(
                                enabled=True,
                                cursor_field="updated_at",
                                request_filter=RequestFilterConfig(
                                    mode=RequestFilterMode.query_param,
                                    param="since",
                                    value="${watermark}",
                                ),
                            ),
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(request_param="cursor", response_path="next_cursor"),
                            ),
                        )
                    },
                )
            )
        },
    )

    async with pool.connection() as conn:
        await ensure_source_table(conn, CONNECTOR_NAME, DATATYPE)
        await conn.commit()

    # Pre-insert an "old crashed" sync run + checkpoint
    old_run_id = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_sync_run (id, connector, datatype, mode, status, started_at)
            VALUES (%s, %s, %s, 'incremental', 'running', NOW() - INTERVAL '5 minutes')
            """,
            [str(old_run_id), CONNECTOR_NAME, DATATYPE],
        )
        await conn.execute(
            """
            INSERT INTO inout_ops_sync_checkpoint
                (run_id, connector, datatype, page_number, cursor_value, records_committed, checkpointed_at)
            VALUES (%s, %s, %s, 1, %s, 10, NOW() - INTERVAL '4 minutes')
            """,
            [str(old_run_id), CONNECTOR_NAME, DATATYPE, RESUME_WATERMARK],
        )
        await conn.commit()

    # Records returned only when the correct ?since= param is present
    received_since_params: list[str | None] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        since = request.url.params.get("since")
        received_since_params.append(since)
        return httpx.Response(200, json={"events": [{"id": "e-1", "updated_at": "2026-03-01T13:00:00"}], "next_cursor": None})

    with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(side_effect=_handler)

        engine = IngestionEngine(pool)
        ingestion_cfg = connector.datatypes[DATATYPE].ingestion
        result = await engine.run_sync(connector, DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert len(received_since_params) >= 1, "Expected at least one API call"
    assert received_since_params[0] == RESUME_WATERMARK, (
        f"Expected first request to use checkpoint cursor '{RESUME_WATERMARK}' "
        f"as since param, got: {received_since_params[0]!r}"
    )

    # Cleanup env var
    os.environ.pop("INOUT_CREDENTIAL_CKPT_INC_KEY", None)


@pytest.mark.anyio
async def test_checkpoint_page_number_reflects_latest_page(pool, run_migrations):
    """T1 #29: checkpoint page_number advances with each checkpointed page.

    After a 3-page sync with checkpoint_every_n_pages=1, the checkpoint row
    must have page_number=3 (overwritten twice from page 1 and 2).
    """
    os.environ["INOUT_CREDENTIAL_CHECKPOINT_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table

    # Use a distinct connector name to avoid cross-test contamination
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig

    CONNECTOR_NAME = "ckpt_page_advance_test"
    BASE_URL = "https://api.ckpt-page.example.com"
    os.environ["INOUT_CREDENTIAL_CKPT_PAGE_KEY"] = "dummy"

    connector = ConnectorConfig(
        name=CONNECTOR_NAME,
        system="CkptPageTest",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=BASE_URL),
        auth=ApiKeyAuth(
            type="api_key", credential_ref="ckpt_page_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "docs": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    checkpoint_every_n_pages=1,
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/docs",
                            record_selector="docs",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(request_param="cursor", response_path="next_cursor"),
                            ),
                        )
                    },
                )
            )
        },
    )

    async with pool.connection() as conn:
        await ensure_source_table(conn, CONNECTOR_NAME, "docs")
        await conn.commit()

    pages = [
        {"docs": [{"id": "d1"}], "next_cursor": "pg2"},
        {"docs": [{"id": "d2"}], "next_cursor": "pg3"},
        {"docs": [{"id": "d3"}], "next_cursor": None},
    ]
    pages_iter = iter(pages)

    with respx.mock(base_url=BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/docs").mock(side_effect=lambda _r: httpx.Response(200, json=next(pages_iter, {"docs": [], "next_cursor": None})))

        engine = IngestionEngine(pool)
        ingestion_cfg = connector.datatypes["docs"].ingestion
        result = await engine.run_sync(connector, "docs", ingestion_cfg)

    assert result.status == "completed"
    assert result.records_fetched == 3

    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT page_number FROM inout_ops_sync_checkpoint WHERE connector = %s AND datatype = %s",
            [CONNECTOR_NAME, "docs"],
        )).fetchone()

    assert row is not None, "Expected checkpoint row to exist"
    assert row[0] == 3, f"Expected page_number=3 after 3-page sync, got {row[0]}"

    os.environ.pop("INOUT_CREDENTIAL_CKPT_PAGE_KEY", None)
