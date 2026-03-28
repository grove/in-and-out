"""Tests for the HubSpot simulator using GenericSimulator directly."""

from __future__ import annotations

import os

import httpx
import pytest

from inandout.stubs import (
    HUBSPOT_BASE_URL,
    GenericSimulator,
    make_hubspot_connector_config,
    make_hubspot_sim_config,
)
from inandout.transport.http import HttpTransportAdapter


@pytest.mark.anyio
async def test_simulator_lists_all_contacts_with_pagination():
    """GenericSimulator + HttpTransportAdapter fetches all contacts across pages."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy-token"

    connector = make_hubspot_connector_config()
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    all_records: list[dict] = []

    with GenericSimulator(connector, make_hubspot_sim_config(page_size=2)):
        async with HttpTransportAdapter(connector) as adapter:
            async for page in adapter.fetch_pages(ingestion_cfg.list):
                all_records.extend(page)

    assert len(all_records) == 3
    ids = {r["id"] for r in all_records}
    assert ids == {"1", "2", "3"}


@pytest.mark.anyio
async def test_simulator_returns_404_for_missing_contact():
    """GET /crm/v3/objects/contacts/999 returns 404."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy-token"

    connector = make_hubspot_connector_config()

    with GenericSimulator(connector, make_hubspot_sim_config()):
        async with httpx.AsyncClient(base_url=HUBSPOT_BASE_URL) as client:
            resp = await client.get("/crm/v3/objects/contacts/999")

    assert resp.status_code == 404
    body = resp.json()
    assert body["status"] == "error"


@pytest.mark.anyio
async def test_simulator_update_contact():
    """PATCH updates a contact and returns it."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy-token"

    connector = make_hubspot_connector_config()

    with GenericSimulator(connector, make_hubspot_sim_config()):
        async with httpx.AsyncClient(base_url=HUBSPOT_BASE_URL) as client:
            resp = await client.patch(
                "/crm/v3/objects/contacts/1",
                json={"properties": {"firstname": "AliceUpdated"}},
            )

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == "1"


# ---------------------------------------------------------------------------
# page_size / limit param tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_connector_config_has_page_size_and_param():
    """make_hubspot_connector_config includes page_size=100 and page_size_param='limit'."""
    connector = make_hubspot_connector_config()
    cursor = connector.datatypes["contacts"].ingestion.list.pagination.cursor
    assert cursor is not None
    assert cursor.page_size == 100
    assert cursor.page_size_param == "limit"


@pytest.mark.anyio
async def test_simulator_honours_limit_param_single_page():
    """With limit=100 injected by the transport and only 3 seed records, all records
    arrive in a single page (no pagination needed)."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy-token"

    connector = make_hubspot_connector_config()  # page_size=100, page_size_param="limit"
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    # No page_size override in sim → simulator reads ?limit=100 from the request
    from inandout.stubs.config import SimulatorConfig, SimulatorDatatypeConfig

    sim_cfg = SimulatorConfig(
        datatypes={
            "contacts": SimulatorDatatypeConfig(
                fixtures=[
                    {"id": "1", "properties": {"firstname": "Alice"}},
                    {"id": "2", "properties": {"firstname": "Bob"}},
                    {"id": "3", "properties": {"firstname": "Carol"}},
                ],
                page_size=None,  # no override — use limit from request
            )
        }
    )

    pages: list[list[dict]] = []
    with GenericSimulator(connector, sim_cfg):
        async with HttpTransportAdapter(connector) as adapter:
            async for page in adapter.fetch_pages(ingestion_cfg.list):
                pages.append(page)

    # limit=100 >> 3 records → everything in one page
    assert len(pages) == 1
    assert len(pages[0]) == 3


@pytest.mark.anyio
async def test_simulator_honours_limit_param_multiple_pages():
    """Connector configured with page_size=2; simulator must paginate accordingly
    even without an explicit sim page_size override."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy-token"

    # Override connector page_size to 2 so the transport sends ?limit=2
    connector = make_hubspot_connector_config(cursor_page_size=2, cursor_page_size_param="limit")
    ingestion_cfg = connector.datatypes["contacts"].ingestion
    assert ingestion_cfg is not None

    from inandout.stubs.config import SimulatorConfig, SimulatorDatatypeConfig

    sim_cfg = SimulatorConfig(
        datatypes={
            "contacts": SimulatorDatatypeConfig(
                fixtures=[
                    {"id": "1", "properties": {"firstname": "Alice"}},
                    {"id": "2", "properties": {"firstname": "Bob"}},
                    {"id": "3", "properties": {"firstname": "Carol"}},
                ],
                page_size=None,  # simulator reads limit from request
            )
        }
    )

    pages: list[list[dict]] = []
    with GenericSimulator(connector, sim_cfg):
        async with HttpTransportAdapter(connector) as adapter:
            async for page in adapter.fetch_pages(ingestion_cfg.list):
                pages.append(page)

    # 3 records, limit=2 → 2 pages
    assert len(pages) == 2
    assert len(pages[0]) == 2
    assert len(pages[1]) == 1
    assert sum(len(p) for p in pages) == 3
