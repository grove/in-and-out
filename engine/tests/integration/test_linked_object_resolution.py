"""Integration tests for T1 #16: Linked/Nested Object Resolution.

Parent records often embed lists of child object identifiers rather than full
child objects (e.g. an order containing ``line_item_ids``).  The ingestion
tool must extract those IDs, fire individual follow-up GET requests for each
child, and persist the children into their own dedicated source table.

GOAL.md T1 #16: "Parent records often embed lists of child object identifiers
rather than full child objects … The tool must extract those IDs, perform
individual lookups to resolve child entities, and persist them into their own
dedicated datatype tables."
"""
from __future__ import annotations

import json
import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
    LinkedObject,
)
from inandout.config.ingestion import (
    HistoryMode,
    IngestionConfig,
    ListConfig,
    ScheduleConfig,
)
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
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

_CONNECTOR = "linked_obj_test"
_PARENT_DT = "orders"
_CHILD_DT = "line_items"
_BASE_URL = "https://api.linked-test.example.com"
os.environ.setdefault("INOUT_CREDENTIAL_LINKED_OBJ_KEY", "dummy")


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="LinkedObjSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="linked_obj_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _PARENT_DT: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path=f"/v1/{_PARENT_DT}",
                            record_selector="orders",
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
                linked_objects=[
                    LinkedObject(
                        field="line_item_ids",        # field in parent record
                        datatype=_CHILD_DT,            # child table target
                        detail_path=f"/v1/{_CHILD_DT}/${{id}}",
                        concurrency=2,
                        primary_key="id",
                    )
                ],
            )
        },
    )


@pytest.mark.anyio
async def test_child_records_fetched_and_persisted(pool, run_migrations):
    """T1 #16: parent's embedded child IDs trigger follow-up GETs;
    child records land in the child source table."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_PARENT_DT].ingestion

    # Two parent orders, each with two line item IDs
    orders = [
        {"id": "ord-1", "total": 100, "line_item_ids": ["li-1", "li-2"]},
        {"id": "ord-2", "total": 200, "line_item_ids": ["li-3"]},
    ]
    line_items = {
        "li-1": {"id": "li-1", "name": "Widget A", "price": 40},
        "li-2": {"id": "li-2", "name": "Widget B", "price": 60},
        "li-3": {"id": "li-3", "name": "Gadget C", "price": 200},
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Orders list endpoint (single page)
        mock.get(f"/v1/{_PARENT_DT}").mock(
            return_value=httpx.Response(
                200, json={"orders": orders, "next_cursor": None}
            )
        )
        # Child detail endpoints
        for li_id, li_data in line_items.items():
            mock.get(f"/v1/{_CHILD_DT}/{li_id}").mock(
                return_value=httpx.Response(200, json=li_data)
            )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _PARENT_DT, ingestion_cfg)

    assert result.status == "completed", f"Sync failed: {result}"
    assert result.records_fetched == 2, (
        f"Expected 2 parent records fetched; got {result.records_fetched}"
    )

    # Parents must be in the orders table
    orders_table = source_table_name(_CONNECTOR, _PARENT_DT)
    async with pool.connection() as conn:
        parent_rows = await (
            await conn.execute(
                f"SELECT external_id FROM {orders_table} ORDER BY external_id"
            )
        ).fetchall()
    assert [r[0] for r in parent_rows] == ["ord-1", "ord-2"], (
        "Parent orders must be in the orders source table"
    )

    # Children must be in their own dedicated table
    child_table = source_table_name(_CONNECTOR, _CHILD_DT)
    async with pool.connection() as conn:
        child_rows = await (
            await conn.execute(
                f"SELECT external_id FROM {child_table} ORDER BY external_id"
            )
        ).fetchall()
    child_ids = [r[0] for r in child_rows]
    assert set(child_ids) == {"li-1", "li-2", "li-3"}, (
        f"All 3 child line items must be resolved and persisted; got {child_ids}"
    )


@pytest.mark.anyio
async def test_child_record_data_is_correct(pool, run_migrations):
    """T1 #16: persisted child records contain the full detail payload from the
    follow-up GET, not just the stub ID."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_PARENT_DT].ingestion

    orders = [{"id": "ord-data-1", "total": 50, "line_item_ids": ["li-data-1"]}]
    line_item = {"id": "li-data-1", "name": "Precision Part", "price": 50, "sku": "PP-001"}

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_PARENT_DT}").mock(
            return_value=httpx.Response(
                200, json={"orders": orders, "next_cursor": None}
            )
        )
        mock.get(f"/v1/{_CHILD_DT}/li-data-1").mock(
            return_value=httpx.Response(200, json=line_item)
        )

        engine = IngestionEngine(pool)
        await engine.run_sync(connector, _PARENT_DT, ingestion_cfg)

    child_table = source_table_name(_CONNECTOR, _CHILD_DT)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {child_table} WHERE external_id = 'li-data-1'"
            )
        ).fetchone()

    assert row is not None, "Child record must be persisted"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Precision Part", (
        f"Child data must reflect full detail response; got name={data.get('name')!r}"
    )
    assert data.get("sku") == "PP-001", (
        f"Child data must include all fields from detail GET; got sku={data.get('sku')!r}"
    )


@pytest.mark.anyio
async def test_parent_without_child_ids_does_not_error(pool, run_migrations):
    """T1 #16: parent records with no child IDs (empty list or missing field)
    are persisted normally without errors."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_PARENT_DT].ingestion

    # One order with no line items, one with null field
    orders = [
        {"id": "ord-empty-1", "total": 0, "line_item_ids": []},
        {"id": "ord-empty-2", "total": 0},  # field absent entirely
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_PARENT_DT}").mock(
            return_value=httpx.Response(
                200, json={"orders": orders, "next_cursor": None}
            )
        )
        # No child detail mocks — any unexpected call would fail the test

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _PARENT_DT, ingestion_cfg)

    assert result.status == "completed", f"Sync must complete; got {result}"
    assert result.records_fetched == 2

    orders_table = source_table_name(_CONNECTOR, _PARENT_DT)
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                f"SELECT external_id FROM {orders_table} WHERE external_id LIKE 'ord-empty-%' ORDER BY external_id"
            )
        ).fetchall()
    assert len(rows) == 2, "Both parent records with no children must be persisted"
