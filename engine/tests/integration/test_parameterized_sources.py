"""Integration tests for T1 #9: Parameterized Sources (id-list fetch strategy).

Some APIs return only a list of identifiers or partial/stub objects from their
list/search endpoints rather than full records.  The ingestion tool must
perform individual follow-up lookups — one per identifier — to retrieve the
complete object state before persisting it.

GOAL.md T1 #9: "Some APIs return only a list of identifiers or partial/stub
objects from their list/search endpoints rather than full records.  In these
cases the ingestion tool must perform individual follow-up lookups — one per
identifier — to retrieve the complete object state before persisting it."
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

_CONNECTOR = "id_list_test"
_DATATYPE = "products"
_BASE_URL = "https://api.id-list-test.example.com"
os.environ.setdefault("INOUT_CREDENTIAL_ID_LIST_KEY", "dummy")


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="IdListSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="id_list_key",
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
                            path=f"/v1/{_DATATYPE}",
                            record_selector="product_ids",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                            fetch_strategy="id_list",
                            id_field="id",
                            detail_path=f"/v1/{_DATATYPE}/${{external_id}}",
                            detail_concurrency=3,
                        )
                    },
                )
            )
        },
    )


@pytest.mark.anyio
async def test_stub_ids_expanded_to_full_records(pool, run_migrations):
    """T1 #9: list endpoint returns stub objects with IDs only; engine fires
    per-ID detail GETs and persists the full records."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    stubs = [{"id": "prod-1"}, {"id": "prod-2"}, {"id": "prod-3"}]
    full_records = {
        "prod-1": {"id": "prod-1", "name": "Alpha", "price": 10.0, "sku": "A001"},
        "prod-2": {"id": "prod-2", "name": "Beta",  "price": 20.0, "sku": "B002"},
        "prod-3": {"id": "prod-3", "name": "Gamma", "price": 30.0, "sku": "G003"},
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # List endpoint returns stub objects
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"product_ids": stubs, "next_cursor": None}
            )
        )
        # Detail endpoints
        for pid, data in full_records.items():
            mock.get(f"/v1/{_DATATYPE}/{pid}").mock(
                return_value=httpx.Response(200, json=data)
            )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed", f"Sync failed: {result}"

    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                f"SELECT external_id, data FROM {src_table} ORDER BY external_id"
            )
        ).fetchall()

    assert len(rows) == 3, f"All 3 full records must be persisted; got {len(rows)}"
    ids = [r[0] for r in rows]
    assert set(ids) == {"prod-1", "prod-2", "prod-3"}

    # Verify full detail data was stored, not just the stub
    data_by_id = {
        r[0]: (r[1] if isinstance(r[1], dict) else json.loads(r[1]))
        for r in rows
    }
    assert data_by_id["prod-1"].get("sku") == "A001", (
        "Record must contain full fields from detail GET, not just the stub ID"
    )
    assert data_by_id["prod-3"].get("name") == "Gamma"


@pytest.mark.anyio
async def test_detail_fetch_failure_does_not_abort_other_records(pool, run_migrations):
    """T1 #9: if one detail GET returns a non-200 status, the other records in
    the same page are still fetched and persisted."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    stubs = [{"id": "prod-ok-1"}, {"id": "prod-fail-1"}, {"id": "prod-ok-2"}]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"product_ids": stubs, "next_cursor": None}
            )
        )
        mock.get(f"/v1/{_DATATYPE}/prod-ok-1").mock(
            return_value=httpx.Response(200, json={"id": "prod-ok-1", "name": "OK-1"})
        )
        mock.get(f"/v1/{_DATATYPE}/prod-fail-1").mock(
            return_value=httpx.Response(404, json={"error": "not found"})
        )
        mock.get(f"/v1/{_DATATYPE}/prod-ok-2").mock(
            return_value=httpx.Response(200, json={"id": "prod-ok-2", "name": "OK-2"})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed", (
        "Sync must complete even when one detail fetch fails"
    )

    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                f"SELECT external_id FROM {src_table} WHERE external_id IN ('prod-ok-1', 'prod-ok-2', 'prod-fail-1') ORDER BY external_id"
            )
        ).fetchall()

    persisted_ids = {r[0] for r in rows}
    assert "prod-ok-1" in persisted_ids, "Successful detail fetches must be persisted"
    assert "prod-ok-2" in persisted_ids, "Successful detail fetches must be persisted"
    assert "prod-fail-1" not in persisted_ids, (
        "Failed detail fetch (404) must not produce a persisted record"
    )


@pytest.mark.anyio
async def test_id_list_across_multiple_pages(pool, run_migrations):
    """T1 #9: id-list strategy works correctly with multi-page list responses —
    stubs from all pages are expanded via detail GETs."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    page1_stubs = [{"id": "prod-p1-1"}, {"id": "prod-p1-2"}]
    page2_stubs = [{"id": "prod-p2-1"}]

    call_count = 0

    def list_side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json={"product_ids": page1_stubs, "next_cursor": "page2"})
        return httpx.Response(200, json={"product_ids": page2_stubs, "next_cursor": None})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(side_effect=list_side_effect)
        mock.get(f"/v1/{_DATATYPE}/prod-p1-1").mock(
            return_value=httpx.Response(200, json={"id": "prod-p1-1", "name": "Page1-Item1"})
        )
        mock.get(f"/v1/{_DATATYPE}/prod-p1-2").mock(
            return_value=httpx.Response(200, json={"id": "prod-p1-2", "name": "Page1-Item2"})
        )
        mock.get(f"/v1/{_DATATYPE}/prod-p2-1").mock(
            return_value=httpx.Response(200, json={"id": "prod-p2-1", "name": "Page2-Item1"})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"

    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                f"SELECT external_id FROM {src_table} WHERE external_id LIKE 'prod-p%' ORDER BY external_id"
            )
        ).fetchall()

    assert len(rows) == 3, f"All 3 records from both pages must be persisted; got {len(rows)}"
