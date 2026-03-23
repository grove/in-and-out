"""Integration tests: fan-in shared table (B3)."""
from __future__ import annotations

import os
import uuid

import httpx
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL_HS = "https://api.hubspot.fanin-test.example.com"
_BASE_URL_SF = "https://api.salesforce.fanin-test.example.com"


def _make_connector(name: str, base_url: str, shared_table: str = "contacts"):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig

    cred_ref = f"{name}_key"
    return ConnectorConfig(
        name=name,
        system=name.capitalize(),
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref=cred_ref,
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": DatatypeConfig(
                shared_table=shared_table,
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/contacts",
                            record_selector="contacts",
                            pagination=PaginationConfig(strategy="none"),
                        )
                    },
                ),
            )
        },
    )


@pytest.mark.anyio
async def test_two_connectors_write_to_shared_table(pool, run_migrations):
    """HubSpot and Salesforce both write contacts to inout_src_contacts."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_KEY"] = "dummy"
    os.environ["INOUT_CREDENTIAL_SALESFORCE_KEY"] = "dummy"

    from inandout.ingestion.engine import IngestionEngine

    hs_connector = _make_connector("hubspot", _BASE_URL_HS)
    sf_connector = _make_connector("salesforce", _BASE_URL_SF)

    hs_contacts = [{"id": "h-1", "name": "Alice"}]
    sf_contacts = [{"id": "s-1", "name": "Bob"}]

    engine = IngestionEngine(pool)

    with respx.mock(base_url=_BASE_URL_HS, assert_all_called=False) as mock_hs:
        mock_hs.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"contacts": hs_contacts, "next_cursor": None})
        )
        result_hs = await engine.run_sync(
            hs_connector, "contacts", hs_connector.datatypes["contacts"].ingestion,
            dtype_cfg=hs_connector.datatypes["contacts"],
        )

    assert result_hs.status == "completed"

    with respx.mock(base_url=_BASE_URL_SF, assert_all_called=False) as mock_sf:
        mock_sf.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"contacts": sf_contacts, "next_cursor": None})
        )
        result_sf = await engine.run_sync(
            sf_connector, "contacts", sf_connector.datatypes["contacts"].ingestion,
            dtype_cfg=sf_connector.datatypes["contacts"],
        )

    assert result_sf.status == "completed"

    # Both records should be in the shared table
    shared_table = "inout_src_contacts"
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                f"SELECT external_id, _connector FROM {shared_table} ORDER BY external_id"
            )
        ).fetchall()

    ids_by_connector = {(r[0], r[1]) for r in rows}
    assert ("h-1", "hubspot") in ids_by_connector, f"HubSpot record not found; rows={rows}"
    assert ("s-1", "salesforce") in ids_by_connector, f"Salesforce record not found; rows={rows}"


@pytest.mark.anyio
async def test_fan_in_upsert_same_external_id_same_connector(pool, run_migrations):
    """Same external_id from same connector → upsert, not duplicate."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_KEY"] = "dummy"

    from inandout.ingestion.engine import IngestionEngine

    connector = _make_connector("hubspot", _BASE_URL_HS)
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    dtype_cfg = connector.datatypes["contacts"]

    contacts = [{"id": "h-dup", "name": "Alice"}]
    engine = IngestionEngine(pool)

    for _ in range(2):
        with respx.mock(base_url=_BASE_URL_HS, assert_all_called=False) as mock:
            mock.get("/v1/contacts").mock(
                return_value=httpx.Response(200, json={"contacts": contacts})
            )
            await engine.run_sync(connector, "contacts", ingestion_cfg, dtype_cfg=dtype_cfg)

    shared_table = "inout_src_contacts"
    async with pool.connection() as conn:
        count_row = await (
            await conn.execute(
                f"SELECT COUNT(*) FROM {shared_table} WHERE external_id='h-dup' AND _connector='hubspot'"
            )
        ).fetchone()

    assert count_row is not None
    assert count_row[0] == 1, f"Expected 1 row for h-dup/hubspot, got {count_row[0]}"
