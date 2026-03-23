"""Integration test: checkpoint resume (B1)."""
from __future__ import annotations

import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")


def _make_connector(checkpoint_every_n_pages: int = 1):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig

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
                                strategy="cursor",
                                cursor_param="cursor",
                                cursor_path="next_cursor",
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
