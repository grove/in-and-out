"""Tripletex ERP config factories for the config-driven simulator.

Provides ``make_tripletex_connector_config`` and ``make_tripletex_sim_config``
that together with ``GenericSimulator`` give a fully functional Tripletex stub.

Tripletex uses offset-based pagination (``from`` / ``count``) and returns
records under a ``values`` envelope with ``fullResultSize`` for the total count.
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
from inandout.config.pagination import PaginationConfig, PaginationStrategy
from inandout.stubs.config import (
    ExtraRoute,
    SimulatorConfig,
    SimulatorDatatypeConfig,
)

TRIPLETEX_BASE_URL = "https://tripletex.no/v2"

# ---------------------------------------------------------------------------
# Default fixture data (mirrors tripletex.example.yaml seed_data)
# ---------------------------------------------------------------------------

_DEFAULT_CUSTOMERS: list[dict] = [
    {
        "id": 10001,
        "name": "Acme AS",
        "organizationNumber": "912345678",
        "email": "post@acme.no",
        "isCustomer": True,
        "isSupplier": False,
        "changesUntil": "2026-03-20T10:00:00.000+02:00",
    },
    {
        "id": 10002,
        "name": "Global AS",
        "organizationNumber": "923456789",
        "email": "post@global.no",
        "isCustomer": True,
        "isSupplier": False,
        "changesUntil": "2026-03-20T11:00:00.000+02:00",
    },
    {
        "id": 10003,
        "name": "Nordic Tech AS",
        "organizationNumber": "934567890",
        "email": "post@nordictech.no",
        "isCustomer": True,
        "isSupplier": True,
        "changesUntil": "2026-03-20T12:00:00.000+02:00",
    },
    {
        "id": 10004,
        "name": "Fjord Solutions AS",
        "organizationNumber": "945678901",
        "email": "post@fjord.no",
        "isCustomer": True,
        "isSupplier": False,
        "changesUntil": "2026-03-20T13:00:00.000+02:00",
    },
]

_DEFAULT_CONTACTS: list[dict] = [
    {
        "id": 20001,
        "firstName": "Kari",
        "lastName": "Nordmann",
        "email": "kari.nordmann@acme.no",
        "phoneNumberMobile": "+4795123456",
        "customer": {"id": 10001, "displayName": "Acme AS"},
        "changesUntil": "2026-03-20T10:00:00.000+02:00",
    },
    {
        "id": 20002,
        "firstName": "Ole",
        "lastName": "Hansen",
        "email": "ole.hansen@global.no",
        "phoneNumberMobile": "+4796234567",
        "customer": {"id": 10002, "displayName": "Global AS"},
        "changesUntil": "2026-03-20T10:00:00.000+02:00",
    },
    {
        "id": 20003,
        "firstName": "Anna",
        "lastName": "Berg",
        "email": "anna.berg@acme.no",
        "phoneNumberMobile": "+4797345678",
        "customer": {"id": 10001, "displayName": "Acme AS"},
        "changesUntil": "2026-03-20T10:00:00.000+02:00",
    },
    {
        "id": 20004,
        "firstName": "Lars",
        "lastName": "Olsen",
        "email": "lars.olsen@nordictech.no",
        "phoneNumberMobile": "+4798456789",
        "customer": {"id": 10003, "displayName": "Nordic Tech AS"},
        "changesUntil": "2026-03-20T10:00:00.000+02:00",
    },
    {
        "id": 20005,
        "firstName": "Ingrid",
        "lastName": "Dahl",
        "email": "ingrid.dahl@fjord.no",
        "phoneNumberMobile": "+4799567890",
        "customer": {"id": 10004, "displayName": "Fjord Solutions AS"},
        "changesUntil": "2026-03-20T10:00:00.000+02:00",
    },
]

# ---------------------------------------------------------------------------
# Config factories
# ---------------------------------------------------------------------------


def make_tripletex_sim_config(
    customers: list[dict] | None = None,
    contacts: list[dict] | None = None,
    page_size: int = 10,
) -> SimulatorConfig:
    """Build a ``SimulatorConfig`` for the Tripletex ERP simulator.

    Extra routes cover per-record GET, PUT, and POST endpoints for customers
    and contacts, matching the write operations declared in the connector YAML.
    """
    return SimulatorConfig(
        default_page_size=page_size,
        datatypes={
            "customers": SimulatorDatatypeConfig(
                fixtures=customers if customers is not None else list(_DEFAULT_CUSTOMERS),
                page_size=page_size,
                response_envelope={"fullResultSize": "${total_count}"},
            ),
            "contacts": SimulatorDatatypeConfig(
                fixtures=contacts if contacts is not None else list(_DEFAULT_CONTACTS),
                page_size=page_size,
                response_envelope={"fullResultSize": "${total_count}"},
            ),
        },
        extra_routes=[
            # GET /customer/{id}
            ExtraRoute(
                method="GET",
                path=r"^/customer/\d+$",
                return_fixture_datatype="customers",
                pk_field="id",
                not_found_body={"status": "error", "message": "Customer not found"},
            ),
            # PUT /customer/{id}
            ExtraRoute(
                method="PUT",
                path=r"^/customer/\d+$",
                return_fixture_datatype="customers",
                pk_field="id",
                not_found_body={"status": "error", "message": "Customer not found"},
            ),
            # POST /customer
            ExtraRoute(
                method="POST",
                path="/customer",
                return_fixture_datatype="customers",
                pk_field="id",
            ),
            # DELETE /customer/{id}
            ExtraRoute(
                method="DELETE",
                path=r"^/customer/\d+$",
                status_code=204,
                body_template={},
            ),
            # GET /contact/{id}
            ExtraRoute(
                method="GET",
                path=r"^/contact/\d+$",
                return_fixture_datatype="contacts",
                pk_field="id",
                not_found_body={"status": "error", "message": "Contact not found"},
            ),
            # PUT /contact/{id}
            ExtraRoute(
                method="PUT",
                path=r"^/contact/\d+$",
                return_fixture_datatype="contacts",
                pk_field="id",
                not_found_body={"status": "error", "message": "Contact not found"},
            ),
            # POST /contact
            ExtraRoute(
                method="POST",
                path="/contact",
                return_fixture_datatype="contacts",
                pk_field="id",
            ),
        ],
    )


def make_tripletex_connector_config(
    base_url: str = TRIPLETEX_BASE_URL,
) -> ConnectorConfig:
    """Build a minimal valid Tripletex connector config for use in tests.

    Includes ingestion for both ``customers`` and ``contacts`` with offset
    pagination matching the Tripletex v2 REST API shapes.
    """
    return ConnectorConfig(
        name="tripletex",
        system="Tripletex ERP",
        generation_profile=GenerationProfile.full_duplex,
        api_version="2",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="tripletex_session",
            api_key=ApiKeyConfig(location="header", name="Authorization"),
        ),
        datatypes={
            "customers": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="10m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/customer",
                            record_selector="values",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.offset,
                                offset={
                                    "param": "from",
                                    "limit_param": "count",
                                    "page_size": 10,
                                    "total_path": "fullResultSize",
                                },
                            ),
                        )
                    },
                )
            ),
            "contacts": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="10m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/contact",
                            record_selector="values",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.offset,
                                offset={
                                    "param": "from",
                                    "limit_param": "count",
                                    "page_size": 10,
                                    "total_path": "fullResultSize",
                                },
                            ),
                        )
                    },
                )
            ),
        },
    )
