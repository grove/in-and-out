"""Integration tests for data quality rule enforcement during ingestion (T1 #26).

When a datatype has quality_rules configured, the ingestion engine validates
each record against those rules. Violating records are routed to the dead-letter
queue and excluded from the source table.

Covers:
- Records missing required fields are quarantined in dead-letter and skipped
- Records failing regex validation are quarantined
- Records with disallowed values are quarantined; compliant records pass through
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.config.quality import QualityRule

_NO_PAGINATION = PaginationConfig(
    strategy=PaginationStrategy.cursor,
    cursor=CursorConfig(request_param="cursor", response_path="next_cursor"),
)
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import dead_letter_table_name

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.quality-test.example.com"
_CONNECTOR = "quality_test"
_DATATYPE = "users"
_SOURCE_TABLE = f"inout_src_{_CONNECTOR}_{_DATATYPE}"
_DL_TABLE = dead_letter_table_name("ingestion", _CONNECTOR, _DATATYPE)


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_QUALITY_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_QUALITY_TEST_KEY", None)


def _make_quality_connector(quality_rules: QualityRule) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="QualitySystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="quality_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                quality_rules=quality_rules,
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/users",
                            record_selector="users",
                            pagination=_NO_PAGINATION,
                        )
                    },
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_missing_required_field_quarantined_to_dead_letter(pool, run_migrations):
    """T1 #26: records missing a required field are skipped and written to dead-letter."""
    connector = _make_quality_connector(
        quality_rules=QualityRule(required=["email"])
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    records = [
        {"id": "u-1", "name": "Alice", "email": "alice@example.com"},   # valid
        {"id": "u-2", "name": "Bob"},                                     # missing email → quarantine
        {"id": "u-3", "name": "Carol", "email": "carol@example.com"},   # valid
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/users").mock(
            return_value=httpx.Response(200, json={"users": records, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 2, f"Only 2 valid records should be inserted; got {result}"
    assert result.records_errored == 1, f"1 violating record should be errored; got {result}"

    # Valid records present in source table
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id FROM {_SOURCE_TABLE} ORDER BY external_id"
        )).fetchall()
    source_ids = [r[0] for r in rows]
    assert "u-1" in source_ids
    assert "u-3" in source_ids
    assert "u-2" not in source_ids, "Violating record must not be in source table"

    # Violating record in dead-letter table
    async with pool.connection() as conn:
        dl_rows = await (await conn.execute(
            f"SELECT external_id FROM {_DL_TABLE}"
        )).fetchall()
    dl_ids = [r[0] for r in dl_rows]
    assert "u-2" in dl_ids, f"Violating record must be in dead-letter; found: {dl_ids}"


@pytest.mark.anyio
async def test_regex_violation_quarantined(pool, run_migrations):
    """T1 #26: records failing a regex rule are quarantined; compliant records pass."""
    connector = _make_quality_connector(
        quality_rules=QualityRule(regex={"email": r"^[^@]+@[^@]+\.[^@]+$"})
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    records = [
        {"id": "r-1", "name": "Alice", "email": "alice@example.com"},   # valid email
        {"id": "r-2", "name": "Bob", "email": "not-an-email"},           # bad email → quarantine
        {"id": "r-3", "name": "Carol", "email": "carol@test.io"},        # valid email
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/users").mock(
            return_value=httpx.Response(200, json={"users": records, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 2
    assert result.records_errored == 1

    async with pool.connection() as conn:
        src_ids = [r[0] for r in await (await conn.execute(
            f"SELECT external_id FROM {_SOURCE_TABLE} ORDER BY external_id"
        )).fetchall()]
    assert "r-1" in src_ids
    assert "r-3" in src_ids
    assert "r-2" not in src_ids, "Regex-violating record must not be in source table"

    async with pool.connection() as conn:
        dl_ids = [r[0] for r in await (await conn.execute(
            f"SELECT external_id FROM {_DL_TABLE}"
        )).fetchall()]
    assert "r-2" in dl_ids


@pytest.mark.anyio
async def test_allowed_values_violation_quarantined(pool, run_migrations):
    """T1 #26: records with disallowed field values are quarantined; others pass."""
    connector = _make_quality_connector(
        quality_rules=QualityRule(allowed_values={"status": ["active", "inactive", "pending"]})
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    records = [
        {"id": "a-1", "name": "Alice", "status": "active"},        # valid
        {"id": "a-2", "name": "Bob", "status": "banned"},           # invalid status → quarantine
        {"id": "a-3", "name": "Carol", "status": "pending"},        # valid
        {"id": "a-4", "name": "Dave", "status": "deleted"},         # invalid → quarantine
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/users").mock(
            return_value=httpx.Response(200, json={"users": records, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_inserted == 2, f"Expected 2 valid records inserted; got {result}"
    assert result.records_errored == 2, f"Expected 2 quarantined records; got {result}"

    async with pool.connection() as conn:
        src_ids = [r[0] for r in await (await conn.execute(
            f"SELECT external_id FROM {_SOURCE_TABLE} ORDER BY external_id"
        )).fetchall()]
    assert "a-1" in src_ids
    assert "a-3" in src_ids
    assert "a-2" not in src_ids
    assert "a-4" not in src_ids

    async with pool.connection() as conn:
        dl_ids = [r[0] for r in await (await conn.execute(
            f"SELECT external_id FROM {_DL_TABLE}"
        )).fetchall()]
    assert "a-2" in dl_ids
    assert "a-4" in dl_ids
