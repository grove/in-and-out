"""Integration tests for T1 #38: pagination drift protection.

During a paginated full-sync, the external system may add or delete records
between page fetches, corrupting the result set.  The engine detects this
anomaly by comparing the total records fetched against the last known record
count.  When the count drops by more than ``drift_max_shrink_pct`` percent
(default 50 %), the sync is aborted as a drift event rather than silently
under-counting records.

GOAL.md T1 #38: "The tool must mitigate page-drift by detecting anomalies in
record counts across pages and triggering the circuit breaker."
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
from inandout.transport.circuit_breaker import reset_all


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

_CONNECTOR = "drift_test"
_DATATYPE = "events"
_BASE_URL = "https://api.drift-test.example.com"
os.environ["INOUT_CREDENTIAL_DRIFT_TEST_KEY"] = "dummy"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DriftTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="drift_test_key",
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
                            path="/v1/events",
                            record_selector="results",
                            # drift_protection=True (default); drift_max_shrink_pct=50 (default)
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


async def _seed_last_sync_count(pool, count: int) -> None:
    """Insert a completed sync run with the given records_fetched so drift_pre_conn picks it up."""
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_sync_run
                (connector, datatype, mode, status, records_fetched, records_inserted,
                 records_updated, records_deleted, records_errored, started_at, finished_at)
            VALUES (%s, %s, 'full', 'completed', %s, %s, 0, 0, 0, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '30 minutes')
            """,
            [_CONNECTOR, _DATATYPE, count, count],
        )
        await conn.commit()


@pytest.mark.anyio
async def test_normal_sync_not_flagged_as_drift(pool):
    """T1 #38: a sync returning the same count as last time must NOT be flagged as drift."""
    reset_all()
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    # Seed: last sync had 3 records
    await _seed_last_sync_count(pool, 3)

    records = [
        {"id": "e1", "name": "Event 1"},
        {"id": "e2", "name": "Event 2"},
        {"id": "e3", "name": "Event 3"},
    ]

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(
                200, json={"results": records, "next_cursor": None}
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Count stayed the same — no drift → sync completes normally
    assert result.status == "completed", (
        f"Expected 'completed' for stable record count, got '{result.status}'"
    )
    assert result.records_fetched == 3


@pytest.mark.anyio
async def test_massive_count_shrink_triggers_abort(pool):
    """T1 #38: when fetched count drops >50 % vs last known, sync is aborted as drift."""
    reset_all()

    connector_name = _CONNECTOR + "_shrink"
    connector = ConnectorConfig(
        name=connector_name,
        system="DriftTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="drift_test_key",
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
                            path="/v1/events",
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

    # Seed: last sync had 100 records
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_sync_run
                (connector, datatype, mode, status, records_fetched, records_inserted,
                 records_updated, records_deleted, records_errored, started_at, finished_at)
            VALUES (%s, %s, 'full', 'completed', 100, 100, 0, 0, 0, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '30 minutes')
            """,
            [connector_name, _DATATYPE],
        )
        await conn.commit()

    # This sync returns only 5 records — 95% drop, far beyond 50% threshold
    thin_records = [{"id": f"e{i}", "name": f"Event {i}"} for i in range(5)]

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(
                200, json={"results": thin_records, "next_cursor": None}
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Engine must abort (not completed) to prevent bad deletion detection
    assert result.status == "aborted", (
        f"Expected 'aborted' on >50% count drop (drift detected), got '{result.status}'. "
        "The engine should abort rather than tombstone 95% of records."
    )


@pytest.mark.anyio
async def test_first_sync_not_drift_checked(pool):
    """T1 #38: the very first sync (no prior completed run) must never be drift-aborted."""
    reset_all()

    connector_name = _CONNECTOR + "_first"
    connector = ConnectorConfig(
        name=connector_name,
        system="DriftTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="drift_test_key",
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
                            path="/v1/events",
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

    # No prior sync run seeded — last_known_count will be 0 → drift check skipped
    small_records = [{"id": "e0", "name": "Only Record"}]

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(
                200, json={"results": small_records, "next_cursor": None}
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed", (
        f"First-ever sync must always complete (no prior count to compare), got '{result.status}'"
    )
    assert result.records_inserted == 1
