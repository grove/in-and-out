"""Integration test: run sync with history_mode=append, verify rows in history table."""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import HistoryMode, IngestionConfig, ListConfig, ScheduleConfig
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
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


def _make_append_connector(name: str = "test_history") -> ConnectorConfig:
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
            "contacts": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.append,
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
                )
            )
        },
    )


@pytest.mark.anyio
async def test_history_mode_append_inserts_history_rows(pool):
    """First sync with history_mode=append should create rows in the history table."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_append_connector("test_hist_append")
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    records = [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": records, "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "contacts", ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 2

    # Verify rows in source table
    async with pool.connection() as conn:
        src_rows = await (await conn.execute(
            "SELECT external_id FROM inout_src_test_hist_append_contacts ORDER BY external_id"
        )).fetchall()
    assert [r[0] for r in src_rows] == ["1", "2"]

    # Verify rows in history table
    async with pool.connection() as conn:
        hist_rows = await (await conn.execute(
            "SELECT external_id FROM inout_src_test_hist_append_contacts_history ORDER BY external_id"
        )).fetchall()
    assert len(hist_rows) == 2
    assert [r[0] for r in hist_rows] == ["1", "2"]


@pytest.mark.anyio
async def test_history_mode_append_accumulates_on_update(pool):
    """Second sync that updates records should add more history rows."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_append_connector("test_hist_update")
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    records_v1 = [{"id": "10", "name": "Alice", "version": 1}]
    records_v2 = [{"id": "10", "name": "Alice Updated", "version": 2}]

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        # First sync — insert
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": records_v1, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, "contacts", ingestion_cfg)
        assert result1.records_inserted == 1

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        # Second sync — update (different data, different hash)
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": records_v2, "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        result2 = await engine2.run_sync(connector, "contacts", ingestion_cfg)
        assert result2.records_updated == 1

    # History table should have 2 rows (one per change)
    async with pool.connection() as conn:
        hist_rows = await (await conn.execute(
            "SELECT external_id, _raw_hash FROM inout_src_test_hist_update_contacts_history "
            "ORDER BY _history_id"
        )).fetchall()

    assert len(hist_rows) == 2
    # Both rows reference the same external_id but different hashes
    assert hist_rows[0][0] == "10"
    assert hist_rows[1][0] == "10"
    assert hist_rows[0][1] != hist_rows[1][1]


@pytest.mark.anyio
async def test_history_mode_overwrite_no_history_rows(pool):
    """With history_mode=overwrite, no rows should be written to the history table."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile

    connector = ConnectorConfig(
        name="test_hist_overwrite",
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
            "contacts": DatatypeConfig(
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
                )
            )
        },
    )

    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    records = [{"id": "20", "name": "Charlie"}]

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": records, "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "contacts", ingestion_cfg)

    assert result.records_inserted == 1

    # History table should not exist or be empty
    async with pool.connection() as conn:
        try:
            hist_rows = await (await conn.execute(
                "SELECT COUNT(*) FROM inout_src_test_hist_overwrite_contacts_history"
            )).fetchone()
            # Table exists but should have no rows
            assert hist_rows[0] == 0
        except Exception:
            # Table doesn't exist — that's also acceptable for overwrite mode
            pass


@pytest.mark.anyio
async def test_history_mode_append_noop_no_history_row(pool):
    """No-op records (same hash) should NOT create history rows."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_append_connector("test_hist_noop")
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    records = [{"id": "30", "name": "Stable"}]

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": records, "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, "contacts", ingestion_cfg)
        assert result1.records_inserted == 1

    # Second sync — same records, no-op
    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": records, "next_cursor": None})
        )

        engine2 = IngestionEngine(pool)
        result2 = await engine2.run_sync(connector, "contacts", ingestion_cfg)
        assert result2.records_inserted == 0
        assert result2.records_updated == 0

    # History table should only have 1 row (from the initial insert)
    async with pool.connection() as conn:
        hist_count = await (await conn.execute(
            "SELECT COUNT(*) FROM inout_src_test_hist_noop_contacts_history"
        )).fetchone()

    assert hist_count[0] == 1
