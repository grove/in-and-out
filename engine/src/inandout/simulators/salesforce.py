"""Salesforce CRM config factories for the config-driven simulator.

Provides ``make_salesforce_connector_config`` and ``make_salesforce_sim_config``
that together with ``GenericSimulator`` give a fully functional Salesforce stub.

Salesforce-specific quirks are handled entirely in config:

1. **Shared path** — ``contacts`` and ``accounts`` share ``/query``; routed by
   ``route_discriminator`` matching the SOQL ``q`` parameter.
2. **Cursor-as-URL** — ``cursor_url_template`` generates ``nextRecordsUrl`` paths.
3. **Response envelope** — ``response_envelope`` injects ``totalSize`` / ``done``.
"""
from __future__ import annotations

from typing import Any

from inandout.config.auth import OAuth2Auth, OAuth2Config
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
    RouteDiscriminator,
    SimulatorAuthConfig,
    SimulatorConfig,
    SimulatorDatatypeConfig,
)
from inandout.simulators.generic import GenericSimulator

# ---------------------------------------------------------------------------
# Module-level constants (kept for backward compat — tests import them)
# ---------------------------------------------------------------------------

_BASE_URL = "https://myorg.salesforce.com"
_API_VERSION = "v59.0"
_QUERY_PATH = f"/services/data/{_API_VERSION}/query"
_TOKEN_PATH = "/services/oauth2/token"
_CONTACTS_SOBJECT = f"/services/data/{_API_VERSION}/sobjects/Contact"

_DEFAULT_CONTACTS: list[dict[str, Any]] = [
    {
        "Id": "003A000001aAAAA",
        "FirstName": "Alice",
        "LastName": "Smith",
        "Email": "alice@example.com",
        "LastModifiedDate": "2026-01-01T00:00:00.000+0000",
    },
    {
        "Id": "003A000001bBBBB",
        "FirstName": "Bob",
        "LastName": "Jones",
        "Email": "bob@example.com",
        "LastModifiedDate": "2026-01-02T00:00:00.000+0000",
    },
    {
        "Id": "003A000001cCCCC",
        "FirstName": "Carol",
        "LastName": "Williams",
        "Email": "carol@example.com",
        "LastModifiedDate": "2026-01-03T00:00:00.000+0000",
    },
]

_DEFAULT_ACCOUNTS: list[dict[str, Any]] = [
    {
        "Id": "001A000001xAAAA",
        "Name": "Acme Corp",
        "Industry": "Technology",
        "LastModifiedDate": "2026-01-01T00:00:00.000+0000",
    },
    {
        "Id": "001A000001yBBBB",
        "Name": "Globex",
        "Industry": "Manufacturing",
        "LastModifiedDate": "2026-01-02T00:00:00.000+0000",
    },
]


# ---------------------------------------------------------------------------
# Config factories
# ---------------------------------------------------------------------------


def make_salesforce_sim_config(
    contacts: list[dict] | None = None,
    accounts: list[dict] | None = None,
    page_size: int = 2,
    access_token: str = "sim_access_token",
) -> SimulatorConfig:
    """Build a ``SimulatorConfig`` for the Salesforce simulator.

    Salesforce-specific patterns are expressed entirely in config:

    * ``route_discriminator`` — routes the shared ``/query`` path to the right
      datatype by matching the SOQL ``q`` parameter.
    * ``cursor_url_template`` — generates ``nextRecordsUrl`` path segments for
      cursor-as-URL pagination.
    * ``response_envelope`` — injects ``totalSize`` and ``done`` into every page.
    * ``extra_routes`` — PATCH ``Contact`` endpoint (no writeback config in the
      test connector factory).
    """
    cursor_template = f"{_QUERY_PATH}/{{cursor_id}}"
    envelope = {"totalSize": "${total_count}", "done": "${done}"}

    return SimulatorConfig(
        default_page_size=page_size,
        auth=SimulatorAuthConfig(
            token_response={
                "access_token": access_token,
                "instance_url": _BASE_URL,
                "token_type": "Bearer",
                "expires_in": 7200,
            }
        ),
        datatypes={
            "contacts": SimulatorDatatypeConfig(
                fixtures=contacts if contacts is not None else list(_DEFAULT_CONTACTS),
                page_size=page_size,
                route_discriminator=RouteDiscriminator(
                    param="q",
                    pattern=r"FROM\s+Contact",
                ),
                cursor_url_template=cursor_template,
                response_envelope=envelope,
            ),
            "accounts": SimulatorDatatypeConfig(
                fixtures=accounts if accounts is not None else list(_DEFAULT_ACCOUNTS),
                page_size=page_size,
                route_discriminator=RouteDiscriminator(
                    param="q",
                    pattern=r"FROM\s+Account",
                ),
                cursor_url_template=cursor_template,
                response_envelope=envelope,
            ),
        },
        extra_routes=[
            # PATCH /services/data/v59.0/sobjects/Contact/{id}
            ExtraRoute(
                method="PATCH",
                path=rf"^/services/data/{_API_VERSION}/sobjects/Contact/[A-Za-z0-9]+$",
                status_code=204,
                return_fixture_datatype="contacts",
                pk_field="Id",
                not_found_status=404,
                not_found_body={"errorCode": "NOT_FOUND"},
            ),
        ],
    )


def make_salesforce_connector_config(
    base_url: str = _BASE_URL,
) -> ConnectorConfig:
    """Build a ConnectorConfig for the Salesforce simulator."""
    return ConnectorConfig(
        name="salesforce",
        system="Salesforce CRM",
        generation_profile=GenerationProfile.full_duplex,
        api_version=_API_VERSION,
        connection=ConnectionConfig(base_url=base_url),
        auth=OAuth2Auth(
            type="oauth2",
            credential_ref="salesforce_app",
            oauth2=OAuth2Config(
                grant_type="client_credentials",
                token_url=f"{base_url}{_TOKEN_PATH}",
                scopes=[],
            ),
        ),
        datatypes={
            "contacts": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="Id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="10m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path=_QUERY_PATH,
                            record_selector="records",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="q",
                                    response_path="nextRecordsUrl",
                                ),
                            ),
                        )
                    },
                )
            ),
            "accounts": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="Id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="10m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path=_QUERY_PATH,
                            record_selector="records",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="q",
                                    response_path="nextRecordsUrl",
                                ),
                            ),
                        )
                    },
                )
            ),
        },
    )


# ---------------------------------------------------------------------------
# Backward-compatible simulator class
# ---------------------------------------------------------------------------

