"""Integration tests for API version header injection (T1 #39).

Covers:
- Connector-level api_version_header injects api_version into every ingestion GET request
- Datatype-level api_version override takes precedence over connector-level version
- Writeback requests also carry the api_version header when configured
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
from inandout.config.writeback import (
    WritebackConfig,
    ProtectionLevel,
    ConflictResolution,
    OperationsConfig,
    OperationConfig,
    UpdateOperationConfig,
)
from inandout.ingestion.engine import IngestionEngine
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.version-test.example.com"
_CONNECTOR = "version_test"
_DATATYPE = "records"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_VERSION_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_VERSION_TEST_KEY", None)


def _make_versioned_ingestion_connector(
    api_version: str = "v55.0",
    api_version_header: str = "Salesforce-Version",
    datatype_api_version: str | None = None,
) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="SalesforceSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version=api_version,
        api_version_header=api_version_header,
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="version_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                api_version=datatype_api_version,
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/services/data/records",
                            record_selector="records",
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
            ),
        },
    )


def _make_versioned_writeback_connector(api_version: str = "v2") -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="VersionedSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version=api_version,
        api_version_header="X-API-Version",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="version_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v2/records/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v2/records/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_api_version_injected_on_ingestion_request(pool, run_migrations):
    """T1 #39: api_version_header with api_version are injected as header on every GET during ingestion."""
    connector = _make_versioned_ingestion_connector(api_version="v55.0", api_version_header="Salesforce-Version")
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    captured_version_headers: list[str] = []

    def _get_handler(request: httpx.Request) -> httpx.Response:
        captured_version_headers.append(request.headers.get("Salesforce-Version", ""))
        return httpx.Response(
            200,
            json={"records": [{"id": "rec-1", "name": "Item 1"}], "next_cursor": None},
        )

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/services/data/records").mock(side_effect=_get_handler)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert result.records_fetched >= 1
    assert len(captured_version_headers) >= 1
    assert all(v == "v55.0" for v in captured_version_headers), (
        f"Expected Salesforce-Version: v55.0 on all requests; got {captured_version_headers}"
    )


@pytest.mark.anyio
async def test_datatype_api_version_overrides_connector_version(pool, run_migrations):
    """T1 #39: per-datatype api_version overrides the connector-level api_version."""
    connector = _make_versioned_ingestion_connector(
        api_version="v55.0",
        api_version_header="Salesforce-Version",
        datatype_api_version="v54.0",  # datatype overrides connector
    )
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    captured_headers: list[str] = []

    def _get_handler(request: httpx.Request) -> httpx.Response:
        captured_headers.append(request.headers.get("Salesforce-Version", ""))
        return httpx.Response(
            200,
            json={"records": [{"id": "rec-2", "name": "Item 2"}], "next_cursor": None},
        )

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/services/data/records").mock(side_effect=_get_handler)

        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed"
    assert len(captured_headers) >= 1
    # Datatype-level v54.0 should override connector-level v55.0
    assert all(v == "v54.0" for v in captured_headers), (
        f"Expected Salesforce-Version: v54.0 (datatype override); got {captured_headers}"
    )


@pytest.mark.anyio
async def test_api_version_injected_on_writeback_request(pool, run_migrations):
    """T1 #39: writeback PATCH requests also carry the api_version header."""
    delta_table = f"inout_delta_{_CONNECTOR}_version_wb"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["rec-wb-1", "Widget", "update"],
        )
        await conn.commit()

    connector = _make_versioned_writeback_connector(api_version="v2")
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    captured_version_headers: list[str] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        captured_version_headers.append(request.headers.get("X-API-Version", ""))
        return httpx.Response(200, json={"id": "rec-wb-1"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(f"{_BASE_URL}/v2/records/rec-wb-1").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert len(captured_version_headers) == 1
    assert captured_version_headers[0] == "v2", (
        f"Expected X-API-Version: v2 on PATCH; got {captured_version_headers[0]!r}"
    )
