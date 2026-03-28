"""Integration tests for IngestionEngine against a real PostgreSQL database."""
from __future__ import annotations

import os

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.config.tool import DatabaseConfig
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.pool import create_pool


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


def _make_connector_config(base_url: str = "https://api.example.com") -> ConnectorConfig:
    """Build a minimal valid ConnectorConfig for integration testing."""
    return ConnectorConfig(
        name="test_integration",
        system="TestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
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


@pytest.mark.anyio
async def test_full_sync_inserts_records(pool):
    """Full sync fetches two pages and inserts 4 records into the source table."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = _make_connector_config()
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    page1 = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    page2 = [{"id": "3", "name": "Carol"}, {"id": "4", "name": "Dave"}]

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        params = dict(request.url.params)
        if call_count == 1:
            return httpx.Response(200, json={"results": page1, "next_cursor": "page2"})
        else:
            return httpx.Response(200, json={"results": page2, "next_cursor": None})

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(side_effect=side_effect)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "contacts", ingestion_cfg)

    assert result.status == "completed"
    assert result.records_fetched == 4
    assert result.records_inserted == 4
    assert result.records_updated == 0

    # Verify records in source table
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT external_id FROM inout_src_test_integration_contacts ORDER BY external_id"
        )).fetchall()
    assert [r[0] for r in rows] == ["1", "2", "3", "4"]

    # Verify sync_run record
    async with pool.connection() as conn:
        run_row = await (await conn.execute(
            "SELECT status, records_inserted FROM inout_ops_sync_run WHERE id = %s",
            [result.run_id],
        )).fetchone()
    assert run_row is not None
    assert run_row[0] == "completed"
    assert run_row[1] == 4


@pytest.mark.anyio
async def test_incremental_sync_uses_watermark(db_url, run_migrations):
    """Second run reads watermark and passes it as query param.

    Uses separate pools per run to avoid advisory lock contention on the same connection.
    """
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    # Build connector with incremental config
    from inandout.config.ingestion import IncrementalConfig, RequestFilterConfig, RequestFilterMode
    inc_connector = ConnectorConfig(
        name="test_incremental",
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
                            incremental=IncrementalConfig(
                                enabled=True,
                                cursor_field="updated_at",
                                cursor_type="timestamp",
                                request_filter=RequestFilterConfig(
                                    mode=RequestFilterMode.query_param,
                                    **{"param": "since", "value": "${watermark}"},
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )

    ingestion_cfg = inc_connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    observed_params: list[dict] = []

    def side_effect(request: httpx.Request) -> httpx.Response:
        observed_params.append(dict(request.url.params))
        return httpx.Response(200, json={
            "results": [{"id": "10", "updated_at": "2026-03-01T00:00:00Z"}],
            "next_cursor": None,
        })

    cfg = DatabaseConfig(dsn=db_url)

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(side_effect=side_effect)

        # First run — full sync, no watermark (fresh pool)
        pool1 = await create_pool(cfg)
        try:
            engine1 = IngestionEngine(pool1)
            result1 = await engine1.run_sync(inc_connector, "contacts", ingestion_cfg)
        finally:
            await pool1.close()

        assert result1.status == "completed"

        # Second run — should be incremental with watermark (fresh pool = fresh connections)
        pool2 = await create_pool(cfg)
        try:
            engine2 = IngestionEngine(pool2)
            result2 = await engine2.run_sync(inc_connector, "contacts", ingestion_cfg)
        finally:
            await pool2.close()

        assert result2.status == "completed"

    # First request should have no 'since' param
    assert "since" not in observed_params[0]
    # Second request should have 'since' param with the watermark value
    assert "since" in observed_params[1]
    assert observed_params[1]["since"] == "2026-03-01T00:00:00Z"


@pytest.mark.anyio
async def test_no_op_same_hash(pool):
    """Running sync twice with same data results in 0 inserts on second run."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = ConnectorConfig(
        name="test_noop",
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

    ingestion_cfg = connector.datatypes["items"].ingestion
    assert ingestion_cfg is not None

    records = [{"id": "100", "value": "unchanged"}]

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/items").mock(return_value=httpx.Response(
            200, json={"results": records, "next_cursor": None}
        ))

        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, "items", ingestion_cfg)
        assert result1.records_inserted == 1

        result2 = await engine.run_sync(connector, "items", ingestion_cfg)
        assert result2.records_inserted == 0
        assert result2.records_updated == 0


@pytest.mark.anyio
async def test_advisory_lock_prevents_concurrent_sync(pool):
    """Acquire row-level lock manually, run sync, verify it returns 'skipped'."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"

    connector = ConnectorConfig(
        name="test_locktest",
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

    ingestion_cfg = connector.datatypes["items"].ingestion
    assert ingestion_cfg is not None

    # Ensure the sync-lock row exists in the table
    async with pool.connection() as setup_conn:
        await setup_conn.execute(
            "INSERT INTO inout_ops_sync_lock (connector, datatype) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            [connector.name, "items"],
        )
        await setup_conn.commit()

    async with pool.connection() as lock_conn:
        # Hold the row-level lock so the engine's SKIP LOCKED finds nothing
        await lock_conn.execute(
            "SELECT connector FROM inout_ops_sync_lock WHERE connector = %s AND datatype = %s FOR UPDATE",
            [connector.name, "items"],
        )
        # Do NOT commit — keep the row-level lock held

        with respx.mock(base_url="https://api.example.com", assert_all_called=False):
            engine = IngestionEngine(pool)
            result = await engine.run_sync(connector, "items", ingestion_cfg)

        await lock_conn.rollback()  # release the row-level lock

    assert result.status == "skipped"
