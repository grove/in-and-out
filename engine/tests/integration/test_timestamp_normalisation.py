"""Integration tests for T1 #45: Timestamp & timezone normalisation.

External APIs return timestamps in many formats — Unix epoch (seconds or
milliseconds), ISO 8601 with timezone offsets, RFC 2822, and others.  The
ingestion tool must normalise all ``timestamp_fields`` to UTC ISO 8601 strings
(``YYYY-MM-DDTHH:MM:SSZ``) before persisting records.  A field with an
unrecognisable value must be stored as-is (the original value is preserved and
a warning is emitted).
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
    TimestampFieldConfig,
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

_CONNECTOR = "ts_norm_test"
_DATATYPE = "events"
_BASE_URL = "https://api.ts-norm-test.example.com"
os.environ.setdefault("INOUT_CREDENTIAL_TS_NORM_KEY", "dummy")


def _make_connector(timestamp_fields: list[TimestampFieldConfig]) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="TsNormTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="ts_norm_key",
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
                ),
                timestamp_fields=timestamp_fields,
            )
        },
    )


async def _run_sync_with_records(pool, records: list[dict], timestamp_fields: list[TimestampFieldConfig]) -> None:
    connector = _make_connector(timestamp_fields)
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"events": records, "next_cursor": None}
            )
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed", f"Sync failed: {result}"


async def _fetch_record(pool, external_id: str) -> dict:
    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {src_table} WHERE external_id = %s",
                [external_id],
            )
        ).fetchone()
    assert row is not None, f"Record {external_id!r} not found in source table"
    return row[0] if isinstance(row[0], dict) else json.loads(row[0])


@pytest.mark.anyio
async def test_unix_seconds_normalised_to_utc_iso(pool):
    """T1 #45: Unix epoch seconds (e.g. 1705320000) are normalised to UTC ISO 8601."""
    # 1705320000 = 2024-01-15T08:00:00Z
    records = [{"id": "evt-unix-sec", "title": "Epoch-sec event", "occurred_at": 1705320000}]
    timestamp_fields = [TimestampFieldConfig(field="occurred_at", format="unix_seconds")]

    await _run_sync_with_records(pool, records, timestamp_fields)

    data = await _fetch_record(pool, "evt-unix-sec")
    normalised = data.get("occurred_at")
    assert normalised == "2024-01-15T12:00:00Z", (
        f"Unix seconds must normalise to UTC ISO; got {normalised!r}"
    )


@pytest.mark.anyio
async def test_iso8601_with_offset_normalised_to_utc(pool):
    """T1 #45: ISO 8601 string with a non-UTC offset is converted to UTC Z."""
    # 2024-03-10T14:30:00+05:30 = 2024-03-10T09:00:00Z
    records = [{"id": "evt-iso-offset", "title": "Offset event", "occurred_at": "2024-03-10T14:30:00+05:30"}]
    timestamp_fields = [TimestampFieldConfig(field="occurred_at", format="iso8601")]

    await _run_sync_with_records(pool, records, timestamp_fields)

    data = await _fetch_record(pool, "evt-iso-offset")
    normalised = data.get("occurred_at")
    assert normalised == "2024-03-10T09:00:00Z", (
        f"ISO 8601 with offset must be converted to UTC; got {normalised!r}"
    )


@pytest.mark.anyio
async def test_unix_millis_normalised_to_utc_iso(pool):
    """T1 #45: Unix epoch milliseconds (numeric value >= 1e10) are normalised correctly."""
    # 1705320000000 ms = 2024-01-15T08:00:00Z
    records = [{"id": "evt-unix-ms", "title": "Millis event", "occurred_at": 1705320000000}]
    # Use 'auto' format so the engine auto-detects millis vs seconds
    timestamp_fields = [TimestampFieldConfig(field="occurred_at", format="auto")]

    await _run_sync_with_records(pool, records, timestamp_fields)

    data = await _fetch_record(pool, "evt-unix-ms")
    normalised = data.get("occurred_at")
    assert normalised == "2024-01-15T12:00:00Z", (
        f"Unix millis (auto-detected) must normalise to UTC ISO; got {normalised!r}"
    )
