"""HubSpot CRM config factories for the config-driven simulator.

Provides ``make_hubspot_connector_config`` and ``make_hubspot_sim_config``
that together with ``GenericSimulator`` give a fully functional HubSpot stub.
"""
from __future__ import annotations

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.ingestion import HistoryMode, IngestionConfig, ListConfig, ScheduleConfig
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
from inandout.simulators.config import (
    ExtraRoute,
    SimulatorConfig,
    SimulatorDatatypeConfig,
)
from inandout.simulators.generic import GenericSimulator

# ---------------------------------------------------------------------------
# Default fixture data
# ---------------------------------------------------------------------------

_DEFAULT_CONTACTS: list[dict] = [
    {
        "id": "1",
        "properties": {
            "firstname": "Alice",
            "email": "alice@example.com",
            "lastmodifieddate": "2026-01-01T00:00:00Z",
        },
    },
    {
        "id": "2",
        "properties": {
            "firstname": "Bob",
            "email": "bob@example.com",
            "lastmodifieddate": "2026-01-02T00:00:00Z",
        },
    },
    {
        "id": "3",
        "properties": {
            "firstname": "Carol",
            "email": "carol@example.com",
            "lastmodifieddate": "2026-01-03T00:00:00Z",
        },
    },
]


# ---------------------------------------------------------------------------
# Config factories
# ---------------------------------------------------------------------------


def make_hubspot_sim_config(
    contacts: list[dict] | None = None,
    page_size: int = 2,
) -> SimulatorConfig:
    """Build a ``SimulatorConfig`` for the HubSpot CRM simulator.

    Extra routes cover the per-contact GET and PATCH endpoints that are not
    declared in the minimal test ``ConnectorConfig`` (which has no writeback
    section).  They are expressed entirely in config — no Python routing code.
    """
    return SimulatorConfig(
        default_page_size=page_size,
        datatypes={
            "contacts": SimulatorDatatypeConfig(
                fixtures=contacts if contacts is not None else list(_DEFAULT_CONTACTS),
                page_size=page_size,
            ),
        },
        extra_routes=[
            # GET /crm/v3/objects/contacts/{id}
            ExtraRoute(
                method="GET",
                path=r"^/crm/v3/objects/contacts/\d+$",
                return_fixture_datatype="contacts",
                pk_field="id",
                not_found_body={"status": "error", "message": "Contact not found"},
            ),
            # PATCH /crm/v3/objects/contacts/{id}
            ExtraRoute(
                method="PATCH",
                path=r"^/crm/v3/objects/contacts/\d+$",
                return_fixture_datatype="contacts",
                pk_field="id",
                not_found_body={"status": "error", "message": "Contact not found"},
            ),
        ],
    )


def make_hubspot_connector_config(
    base_url: str = "https://api.hubapi.com",
) -> ConnectorConfig:
    """Build a minimal valid HubSpot connector config for use in tests."""
    return ConnectorConfig(
        name="hubspot",
        system="HubSpot CRM",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v3",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="hubspot_oauth",
            api_key=ApiKeyConfig(location="header", name="Authorization"),
        ),
        datatypes={
            "contacts": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/crm/v3/objects/contacts",
                            record_selector="results",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="after",
                                    response_path="paging.next.after",
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )


HUBSPOT_BASE_URL = "https://api.hubapi.com"
