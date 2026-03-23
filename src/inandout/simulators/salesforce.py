"""Salesforce REST API simulator for testing.

Simulates the Salesforce SOQL query API (REST v59.0):
  - POST /services/oauth2/token     → OAuth2 client_credentials token grant
  - GET  /services/data/v59/query   → SOQL query with nextRecordsUrl cursor
  - GET  /services/data/v59/query/<cursor> → subsequent pages
  - PATCH /services/data/v59/sobjects/Contact/<id> → update a Contact
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlencode

import httpx
import respx

from inandout.config.auth import OAuth2Auth, OAuth2Config
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.ingestion import (
    HistoryMode,
    IngestionConfig,
    ListConfig,
    ScheduleConfig,
)
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig


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


class SalesforceSimulator:
    """Simulates the Salesforce REST API for testing."""

    def __init__(
        self,
        contacts: list[dict] | None = None,
        accounts: list[dict] | None = None,
        page_size: int = 2,
        access_token: str = "sim_access_token",
    ):
        self._contacts = contacts if contacts is not None else list(_DEFAULT_CONTACTS)
        self._accounts = accounts if accounts is not None else list(_DEFAULT_ACCOUNTS)
        self._page_size = page_size
        self._access_token = access_token
        self._mock: respx.MockRouter | None = None
        # cursor → (records_list, offset)
        self._cursors: dict[str, tuple[list, int]] = {}

    def __enter__(self) -> "SalesforceSimulator":
        self._mock = respx.mock(base_url=_BASE_URL, assert_all_called=False)
        self._mock.__enter__()
        self._register_routes()
        return self

    def __exit__(self, *args: object) -> None:
        if self._mock:
            self._mock.__exit__(*args)

    def _register_routes(self) -> None:
        assert self._mock is not None
        # OAuth2 token endpoint
        self._mock.post(_TOKEN_PATH).mock(side_effect=self._handle_token)
        # SOQL query — first page
        self._mock.get(_QUERY_PATH).mock(side_effect=self._handle_query)
        # SOQL query — subsequent pages via nextRecordsUrl cursor
        self._mock.get(re.compile(r"/services/data/v59\.0/query/[A-Za-z0-9]+")).mock(
            side_effect=self._handle_query_next
        )
        # PATCH Contact
        self._mock.patch(
            re.compile(rf"/services/data/{_API_VERSION}/sobjects/Contact/[A-Za-z0-9]+")
        ).mock(side_effect=self._handle_patch_contact)

    # ------------------------------------------------------------------
    # Token
    # ------------------------------------------------------------------

    def _handle_token(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "access_token": self._access_token,
                "instance_url": _BASE_URL,
                "token_type": "Bearer",
                "expires_in": 7200,
            },
        )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def _handle_query(self, request: httpx.Request) -> httpx.Response:
        q = request.url.params.get("q", "").upper()
        if "FROM CONTACT" in q:
            records = self._contacts
        elif "FROM ACCOUNT" in q:
            records = self._accounts
        else:
            return httpx.Response(400, json={"errorCode": "MALFORMED_QUERY", "message": "Unsupported object"})

        return self._paginate(records, 0)

    def _handle_query_next(self, request: httpx.Request) -> httpx.Response:
        cursor = request.url.path.split("/")[-1]
        entry = self._cursors.get(cursor)
        if entry is None:
            return httpx.Response(404, json={"errorCode": "NOT_FOUND"})
        records, offset = entry
        return self._paginate(records, offset)

    def _paginate(self, records: list, offset: int) -> httpx.Response:
        page = records[offset: offset + self._page_size]
        next_offset = offset + self._page_size
        has_more = next_offset < len(records)
        next_records_url: str | None = None
        if has_more:
            cursor_id = f"cursor{next_offset:04d}"
            self._cursors[cursor_id] = (records, next_offset)
            next_records_url = f"/services/data/{_API_VERSION}/query/{cursor_id}"

        body: dict[str, Any] = {
            "totalSize": len(records),
            "done": not has_more,
            "records": page,
        }
        if next_records_url:
            body["nextRecordsUrl"] = next_records_url

        return httpx.Response(200, json=body)

    # ------------------------------------------------------------------
    # PATCH
    # ------------------------------------------------------------------

    def _handle_patch_contact(self, request: httpx.Request) -> httpx.Response:
        contact_id = request.url.path.split("/")[-1]
        for c in self._contacts:
            if c["Id"] == contact_id:
                return httpx.Response(204)
        return httpx.Response(404, json={"errorCode": "NOT_FOUND"})


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
