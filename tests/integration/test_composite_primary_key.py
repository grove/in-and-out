"""Integration tests for composite primary keys (T1 #27).

Covers:
- List-based composite PK: ``primary_key=["company_id", "user_id"]`` →
  external_id stored as ``"company-a:user-1"``
- JMESPath expression PK: ``primary_key=PrimaryKeyExpression(expression="meta.id")``
  extracts a nested field value as the external_id
- Composite PK deduplication: re-ingesting the same records updates in place, not inserts
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig, PrimaryKeyExpression
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.composite-pk-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_COMPOSITE_PK_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_COMPOSITE_PK_KEY", None)


def _make_connector(connector_name: str, primary_key) -> ConnectorConfig:
    return ConnectorConfig(
        name=connector_name,
        system="CompositePkSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="composite_pk_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "memberships": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key=primary_key,
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="1h"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/memberships",
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
async def test_composite_pk_list_creates_compound_external_id(pool):
    """T1 #27: primary_key=["company_id", "user_id"] → external_id stored as "cid:uid"."""
    connector = _make_connector("cpk_list_test", primary_key=["company_id", "user_id"])
    ingestion_cfg = connector.datatypes["memberships"].ingestion
    assert ingestion_cfg is not None

    records = [
        {"company_id": "company-a", "user_id": "user-1", "role": "admin"},
        {"company_id": "company-a", "user_id": "user-2", "role": "member"},
        {"company_id": "company-b", "user_id": "user-1", "role": "owner"},
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/memberships").mock(
            return_value=httpx.Response(200, json={"items": records, "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "memberships", ingestion_cfg)

    assert result.status == "completed"
    assert result.records_fetched == 3
    assert result.records_inserted == 3

    async with pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT external_id FROM inout_src_cpk_list_test_memberships ORDER BY external_id"
        )).fetchall()

    external_ids = [r[0] for r in rows]
    assert external_ids == ["company-a:user-1", "company-a:user-2", "company-b:user-1"], (
        f"Composite external_ids must be 'company_id:user_id' but got {external_ids}"
    )


@pytest.mark.anyio
async def test_jmespath_pk_expression_extracts_nested_field(pool):
    """T1 #27: PrimaryKeyExpression with jmespath extracts nested field as external_id."""
    pk_expr = PrimaryKeyExpression(expression="metadata.id")
    connector = _make_connector("cpk_jmespath_test", primary_key=pk_expr)
    ingestion_cfg = connector.datatypes["memberships"].ingestion

    records = [
        {"metadata": {"id": "nested-001"}, "status": "active"},
        {"metadata": {"id": "nested-002"}, "status": "inactive"},
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/memberships").mock(
            return_value=httpx.Response(200, json={"items": records, "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, "memberships", ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 2

    async with pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT external_id FROM inout_src_cpk_jmespath_test_memberships ORDER BY external_id"
        )).fetchall()

    external_ids = [r[0] for r in rows]
    assert "nested-001" in external_ids
    assert "nested-002" in external_ids


@pytest.mark.anyio
async def test_composite_pk_deduplication_updates_in_place(pool):
    """T1 #27: re-ingesting the same composite-PK records upserts (updates), not inserts twice."""
    connector = _make_connector("cpk_dedup_test", primary_key=["company_id", "user_id"])
    ingestion_cfg = connector.datatypes["memberships"].ingestion

    records_v1 = [{"company_id": "corp-1", "user_id": "emp-1", "role": "engineer"}]
    records_v2 = [{"company_id": "corp-1", "user_id": "emp-1", "role": "lead"}]  # same PK, new role

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/memberships").mock(
            return_value=httpx.Response(200, json={"items": records_v1, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        await engine.run_sync(connector, "memberships", ingestion_cfg)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/memberships").mock(
            return_value=httpx.Response(200, json={"items": records_v2, "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        result = await engine2.run_sync(connector, "memberships", ingestion_cfg)

    assert result.status == "completed"

    # There must be exactly 1 row — the composite PK must be deduplicated
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT external_id, data FROM inout_src_cpk_dedup_test_memberships"
        )).fetchall()

    assert len(rows) == 1, f"Expected exactly 1 row (upsert dedup), got {len(rows)}"
    external_id, data = rows[0]
    assert external_id == "corp-1:emp-1"
    # Verify the data reflects the updated role
    import orjson
    record_data = data if isinstance(data, dict) else orjson.loads(data)
    assert record_data.get("role") == "lead", f"Expected role=lead after upsert update, got {record_data}"
