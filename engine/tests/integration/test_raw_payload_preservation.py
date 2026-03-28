"""Integration tests for T1 #17: raw payload preservation.

The ingestion engine must store an unmodified copy of each record in the
``raw`` JSONB column of the source table exactly as received from the API,
separate from any downstream transformations applied to ``data``.

GOAL.md T1 #17: "Store a clean, unmodified raw copy of each entity exactly
as received from the source system."
"""
from __future__ import annotations

import os

import httpx
import orjson
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.raw-preservation-test.example.com"
_CONNECTOR = "raw_pres_test"
_DATATYPE = "events"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_RAW_PRES_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_RAW_PRES_TEST_KEY", None)


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="RawPresSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="raw_pres_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="1h"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/events",
                            record_selector="events",
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
async def test_raw_column_stores_original_api_response_fields(pool):
    """T1 #17: the raw column stores all fields returned by the API, unmodified."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    # The API returns extra metadata fields alongside the business fields
    api_record = {
        "id": "evt-001",
        "title": "Kickoff Meeting",
        "status": "scheduled",
        "_internal_system_field": "sys_value_42",  # underscore-prefixed field
        "nested": {"key": "value", "count": 7},
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(200, json={"events": [api_record], "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 1

    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT external_id, raw FROM inout_src_{_CONNECTOR}_{_DATATYPE} WHERE external_id = %s",
            ["evt-001"],
        )).fetchone()

    assert row is not None, "Record must be stored in source table"
    external_id, raw = row
    assert external_id == "evt-001"

    # raw must be stored as JSONB and contain all fields from the API response
    raw_dict = raw if isinstance(raw, dict) else orjson.loads(raw)
    assert raw_dict.get("id") == "evt-001"
    assert raw_dict.get("title") == "Kickoff Meeting"
    assert raw_dict.get("status") == "scheduled"
    assert raw_dict.get("_internal_system_field") == "sys_value_42", (
        "T1 #17: raw must preserve ALL fields including underscore-prefixed ones"
    )
    assert raw_dict.get("nested") == {"key": "value", "count": 7}, (
        "T1 #17: raw must preserve nested objects exactly"
    )


@pytest.mark.anyio
async def test_raw_column_preserved_on_update(pool):
    """T1 #17: when a record is updated, the raw column is refreshed with the new API response."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    record_v1 = {"id": "evt-002", "title": "Sprint Planning", "status": "scheduled"}
    record_v2 = {"id": "evt-002", "title": "Sprint Planning", "status": "completed", "notes": "done"}

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(200, json={"events": [record_v1], "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(200, json={"events": [record_v2], "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        result = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.records_updated == 1

    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT raw FROM inout_src_{_CONNECTOR}_{_DATATYPE} WHERE external_id = %s",
            ["evt-002"],
        )).fetchone()

    raw_dict = row[0] if isinstance(row[0], dict) else orjson.loads(row[0])
    assert raw_dict.get("status") == "completed", "raw must reflect updated status"
    assert raw_dict.get("notes") == "done", "raw must include new field from updated record"


@pytest.mark.anyio
async def test_raw_hash_changes_when_record_changes(pool):
    """T1 #17: _raw_hash must change when the record payload changes, enabling drift detection."""
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    record_v1 = {"id": "evt-003", "title": "Retrospective", "participants": 5}
    record_v2 = {"id": "evt-003", "title": "Retrospective", "participants": 8}  # changed

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(200, json={"events": [record_v1], "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    async with pool.connection() as conn:
        row1 = await (await conn.execute(
            f"SELECT _raw_hash FROM inout_src_{_CONNECTOR}_{_DATATYPE} WHERE external_id = %s",
            ["evt-003"],
        )).fetchone()
    hash_v1 = row1[0]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/events").mock(
            return_value=httpx.Response(200, json={"events": [record_v2], "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    async with pool.connection() as conn:
        row2 = await (await conn.execute(
            f"SELECT _raw_hash FROM inout_src_{_CONNECTOR}_{_DATATYPE} WHERE external_id = %s",
            ["evt-003"],
        )).fetchone()
    hash_v2 = row2[0]

    assert hash_v1 != hash_v2, "_raw_hash must change when record payload changes (T1 #17 drift detection)"
