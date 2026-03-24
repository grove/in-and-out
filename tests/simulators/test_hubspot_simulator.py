"""Tests for the HubSpot simulator using GenericSimulator directly."""
from __future__ import annotations

import os

import httpx
import pytest

from inandout.simulators import (
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
