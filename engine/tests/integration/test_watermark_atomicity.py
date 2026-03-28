"""Integration tests for T1 #40: watermark atomicity.

The watermark must be written within the **same database transaction** as the
data it tracks.  This means:

1. After a successful sync, the watermark in ``inout_ops_watermark`` must
   reflect the highest cursor value of the records actually committed to the
   source table — not a value from a future page or from an uncommitted
   transaction.

2. The watermark must never be ahead of the data (no phantom watermarks from
   failed writes).

GOAL.md T1 #40: "Watermark updates must occur atomically within the same
database transaction as the data write they correspond to."
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
    IncrementalConfig,
    IncrementalCursorType,
)
from inandout.config.pagination import (
    PaginationConfig,
    PaginationStrategy,
    CursorConfig,
)
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import source_table_name
from inandout.postgres.watermark import get_watermark


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

_CONNECTOR = "wm_atomicity_test"
_DATATYPE = "orders"
_BASE_URL = "https://api.wm-atomicity.example.com"
os.environ["INOUT_CREDENTIAL_WM_KEY"] = "dummy"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="WMAtomicSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="wm_key",
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
                            path="/v1/orders",
                            record_selector="results",
                            incremental=IncrementalConfig(
                                enabled=True,
                                cursor_field="updated_at",
                                cursor_type=IncrementalCursorType.timestamp,
                            ),
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
async def test_watermark_written_with_data(pool):
    """T1 #40: after a successful full sync the watermark matches the max cursor in the records."""
    connector = _make_connector()

    records = [
        {"id": "o1", "name": "Order 1", "updated_at": "2024-01-10T10:00:00Z"},
        {"id": "o2", "name": "Order 2", "updated_at": "2024-01-15T12:30:00Z"},
        {"id": "o3", "name": "Order 3", "updated_at": "2024-01-20T08:00:00Z"},  # max
    ]

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(
                200,
                json={"results": records, "next_cursor": None},
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 3

    # Watermark must be set and must equal the max cursor field value seen
    async with pool.connection() as conn:
        wm = await get_watermark(conn, _CONNECTOR, _DATATYPE)

    assert wm is not None, "Watermark must be set after a successful incremental sync"
    # The engine tracks the max cursor value; "2024-01-20T08:00:00Z" is the highest
    assert wm == "2024-01-20T08:00:00Z", (
        f"Expected watermark '2024-01-20T08:00:00Z' but got {wm!r}"
    )


@pytest.mark.anyio
async def test_watermark_not_set_on_empty_page(pool):
    """T1 #40: watermark is NOT advanced when the source returns no new records."""
    connector = _make_connector()

    # First sync seeds the watermark
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(
                200,
                json={"results": [{"id": "o10", "name": "Ten", "updated_at": "2024-03-01T00:00:00Z"}], "next_cursor": None},
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    async with pool.connection() as conn:
        first_wm = await get_watermark(conn, _CONNECTOR, _DATATYPE)

    assert first_wm is not None

    # Second sync returns zero records (nothing new since watermark)
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(
                200,
                json={"results": [], "next_cursor": None},
            )
        )
        engine2 = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result2.status == "completed"
    assert result2.records_inserted == 0

    # Watermark must remain unchanged — no phantom advance
    async with pool.connection() as conn:
        second_wm = await get_watermark(conn, _CONNECTOR, _DATATYPE)

    assert second_wm == first_wm, (
        f"Watermark must not advance on empty page; was {first_wm!r}, now {second_wm!r}"
    )


@pytest.mark.anyio
async def test_watermark_consistent_with_committed_data(pool):
    """T1 #40: watermark in DB corresponds to records actually in the source table."""
    connector = _make_connector()
    table = source_table_name(_CONNECTOR, _DATATYPE)

    records = [
        {"id": "o20", "name": "Alpha", "updated_at": "2024-05-01T00:00:00Z"},
        {"id": "o21", "name": "Beta",  "updated_at": "2024-05-05T00:00:00Z"},
    ]

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(
                200,
                json={"results": records, "next_cursor": None},
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"

    async with pool.connection() as conn:
        wm = await get_watermark(conn, _CONNECTOR, _DATATYPE)
        # Fetch the max cursor value actually stored in the source table
        row = await (await conn.execute(
            f"SELECT MAX(data->>'updated_at') FROM {table}"
        )).fetchone()
        max_in_table = row[0] if row else None

    # Watermark must equal the max cursor stored in the committed table
    assert wm is not None
    assert max_in_table is not None
    assert wm == max_in_table, (
        f"Watermark {wm!r} must match max cursor in table {max_in_table!r}"
    )
