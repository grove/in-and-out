"""HubSpot CRM API simulator for testing."""
from __future__ import annotations

import re

import httpx
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import (
    IngestionConfig,
    HistoryMode,
    ListConfig,
    ScheduleConfig,
)
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig


class HubSpotSimulator:
    """Simulates the HubSpot CRM v3 Contacts API for testing."""

    BASE_URL = "https://api.hubapi.com"

    def __init__(self, contacts: list[dict] | None = None, page_size: int = 2):
        self._contacts = contacts or self._default_contacts()
        self._page_size = page_size
        self._mock: respx.MockRouter | None = None

    @staticmethod
    def _default_contacts() -> list[dict]:
        return [
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

    def __enter__(self) -> "HubSpotSimulator":
        self._mock = respx.mock(base_url=self.BASE_URL, assert_all_called=False)
        self._mock.__enter__()
        self._register_routes()
        return self

    def __exit__(self, *args: object) -> None:
        if self._mock:
            self._mock.__exit__(*args)

    def _register_routes(self) -> None:
        assert self._mock is not None
        # GET /crm/v3/objects/contacts?limit=N&after=cursor
        self._mock.get("/crm/v3/objects/contacts").mock(side_effect=self._handle_list_contacts)
        # GET /crm/v3/objects/contacts/{id}
        self._mock.get(re.compile(r"/crm/v3/objects/contacts/(\d+)")).mock(
            side_effect=self._handle_get_contact
        )
        # PATCH /crm/v3/objects/contacts/{id}
        self._mock.patch(re.compile(r"/crm/v3/objects/contacts/(\d+)")).mock(
            side_effect=self._handle_update_contact
        )

    def _handle_list_contacts(self, request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        after = params.get("after")
        limit = int(params.get("limit", self._page_size))

        if after:
            try:
                start = int(after)
            except ValueError:
                start = 0
        else:
            start = 0

        page = self._contacts[start : start + limit]
        next_after = start + limit if start + limit < len(self._contacts) else None

        response_body = {
            "results": page,
            "paging": {"next": {"after": str(next_after)}} if next_after else {},
        }
        return httpx.Response(200, json=response_body)

    def _handle_get_contact(self, request: httpx.Request) -> httpx.Response:
        contact_id = request.url.path.split("/")[-1]
        for c in self._contacts:
            if c["id"] == contact_id:
                return httpx.Response(200, json=c)
        return httpx.Response(404, json={"status": "error", "message": "Contact not found"})

    def _handle_update_contact(self, request: httpx.Request) -> httpx.Response:
        contact_id = request.url.path.split("/")[-1]
        for c in self._contacts:
            if c["id"] == contact_id:
                return httpx.Response(200, json=c)
        return httpx.Response(404, json={"status": "error", "message": "Contact not found"})


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
