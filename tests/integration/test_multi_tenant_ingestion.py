"""Integration tests for multi-tenancy / per-account ingestion scoping (T1 #20).

When a connector defines multiple accounts, the daemon spawns one ingestion
loop per account — each using the account's credential_ref and optional base_url
override.  Records from all accounts land in the same (connector, datatype)
source table, uniquely identified by their external_id.

These tests simulate the per-account connector construction that the daemon
performs, then run IngestionEngine.run_sync for each account to verify:
- Each account's API is called at its own base_url
- Records from all accounts are stored in the shared source table
- Account-specific data does not overwrite another account's records
"""
from __future__ import annotations

import copy
import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    AccountConfig,
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine

_NO_PAGINATION = PaginationConfig(
    strategy=PaginationStrategy.cursor,
    cursor=CursorConfig(request_param="cursor", response_path="next_cursor"),
)

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL_TENANT_A = "https://tenant-a.crm.example.com"
_BASE_URL_TENANT_B = "https://tenant-b.crm.example.com"
_CONNECTOR = "multi_tenant_test"
_DATATYPE = "contacts"
_SOURCE_TABLE = f"inout_src_{_CONNECTOR}_{_DATATYPE}"


@pytest.fixture(autouse=True)
def _set_credentials():
    os.environ["INOUT_CREDENTIAL_TENANT_A_CRED"] = "token-a"
    os.environ["INOUT_CREDENTIAL_TENANT_B_CRED"] = "token-b"
    os.environ["INOUT_CREDENTIAL_MULTI_TENANT_KEY"] = "shared-key"
    yield
    for k in ("INOUT_CREDENTIAL_TENANT_A_CRED", "INOUT_CREDENTIAL_TENANT_B_CRED", "INOUT_CREDENTIAL_MULTI_TENANT_KEY"):
        os.environ.pop(k, None)


def _make_multi_tenant_connector() -> ConnectorConfig:
    """Connector with two accounts, each with a distinct base_url."""
    return ConnectorConfig(
        name=_CONNECTOR,
        system="MultiTenantCRM",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL_TENANT_A),  # default base
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="multi_tenant_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        accounts=[
            AccountConfig(
                account_id="tenant-a",
                credential_ref="tenant_a_cred",
                base_url=_BASE_URL_TENANT_A,
            ),
            AccountConfig(
                account_id="tenant-b",
                credential_ref="tenant_b_cred",
                base_url=_BASE_URL_TENANT_B,
            ),
        ],
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/contacts",
                            record_selector="contacts",
                            pagination=_NO_PAGINATION,
                        )
                    },
                ),
            ),
        },
    )


def _build_account_connector(connector: ConnectorConfig, account: AccountConfig) -> ConnectorConfig:
    """Simulate what the daemon does: deepcopy + apply account base_url override."""
    account_connector = copy.deepcopy(connector)
    if account.base_url is not None:
        object.__setattr__(account_connector.connection, "base_url", account.base_url)
    return account_connector


@pytest.mark.anyio
async def test_per_account_ingestion_fetches_from_each_base_url(pool, run_migrations):
    """T1 #20: each account's connector fetches from its own base_url."""
    connector = _make_multi_tenant_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    tenant_a_records = [
        {"id": "a-contact-1", "name": "Alice"},
        {"id": "a-contact-2", "name": "Bob"},
    ]
    tenant_b_records = [
        {"id": "b-contact-1", "name": "Carlos"},
        {"id": "b-contact-2", "name": "Diana"},
    ]

    # Tenant A
    account_a = connector.accounts[0]
    connector_a = _build_account_connector(connector, account_a)

    with respx.mock(base_url=_BASE_URL_TENANT_A, assert_all_called=False) as mock_a:
        mock_a.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"contacts": tenant_a_records, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result_a = await engine.run_sync(connector_a, _DATATYPE, ingestion_cfg)

    assert result_a.status == "completed"
    assert result_a.records_inserted == 2

    # Tenant B
    account_b = connector.accounts[1]
    connector_b = _build_account_connector(connector, account_b)

    with respx.mock(base_url=_BASE_URL_TENANT_B, assert_all_called=False) as mock_b:
        mock_b.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"contacts": tenant_b_records, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result_b = await engine.run_sync(connector_b, _DATATYPE, ingestion_cfg)

    assert result_b.status == "completed"
    assert result_b.records_inserted == 2

    # Both accounts' records are in the shared source table
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id FROM {_SOURCE_TABLE} ORDER BY external_id"
        )).fetchall()
    all_ids = {r[0] for r in rows}
    assert "a-contact-1" in all_ids
    assert "a-contact-2" in all_ids
    assert "b-contact-1" in all_ids
    assert "b-contact-2" in all_ids


@pytest.mark.anyio
async def test_account_records_do_not_overwrite_each_other(pool, run_migrations):
    """T1 #20: records from different accounts with distinct IDs coexist in the source table."""
    connector = _make_multi_tenant_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    # Both accounts return records with different external IDs
    tenant_a_records = [{"id": "shared-scope-a1", "name": "Alice", "account": "A"}]
    tenant_b_records = [{"id": "shared-scope-b1", "name": "Bob", "account": "B"}]

    for account, records in zip(connector.accounts, [tenant_a_records, tenant_b_records]):
        acc_connector = _build_account_connector(connector, account)
        base = account.base_url or connector.connection.base_url

        with respx.mock(base_url=base, assert_all_called=False) as mock:
            mock.get("/v1/contacts").mock(
                return_value=httpx.Response(200, json={"contacts": records, "next_cursor": None})
            )
            engine = IngestionEngine(pool)
            await engine.run_sync(acc_connector, _DATATYPE, ingestion_cfg)

    # Both records exist independently
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, data->>'account' as acct FROM {_SOURCE_TABLE} ORDER BY external_id"
        )).fetchall()
    found = {r[0]: r[1] for r in rows}
    assert found.get("shared-scope-a1") == "A"
    assert found.get("shared-scope-b1") == "B"


@pytest.mark.anyio
async def test_per_account_ingestion_multiple_runs_update_records(pool, run_migrations):
    """T1 #20: re-running ingestion for an account updates existing records."""
    connector = _make_multi_tenant_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    account_a = connector.accounts[0]
    connector_a = _build_account_connector(connector, account_a)

    # First run: insert a contact
    with respx.mock(base_url=_BASE_URL_TENANT_A, assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"contacts": [
                {"id": "update-test-1", "name": "Original Name", "status": "active"}
            ], "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector_a, _DATATYPE, ingestion_cfg)

    assert result1.records_inserted == 1

    # Second run: same id, updated name
    with respx.mock(base_url=_BASE_URL_TENANT_A, assert_all_called=False) as mock:
        mock.get("/v1/contacts").mock(
            return_value=httpx.Response(200, json={"contacts": [
                {"id": "update-test-1", "name": "Updated Name", "status": "active"}
            ], "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result2 = await engine.run_sync(connector_a, _DATATYPE, ingestion_cfg)

    assert result2.records_updated == 1

    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT data->>'name' FROM {_SOURCE_TABLE} WHERE external_id = %s",
            ["update-test-1"],
        )).fetchone()
    assert row is not None
    assert row[0] == "Updated Name", f"Record should be updated; got {row[0]!r}"
