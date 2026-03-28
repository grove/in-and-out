"""Tests for the Salesforce simulator using GenericSimulator directly."""
from __future__ import annotations

import os

import pytest
import httpx

from inandout.simulators import (
    GenericSimulator,
    make_salesforce_connector_config,
    make_salesforce_sim_config,
)
from inandout.simulators.salesforce import (
    _BASE_URL,
    _TOKEN_PATH,
    _QUERY_PATH,
    _API_VERSION,
)
from inandout.transport.auth import OAuth2ClientCredentialsAuth


@pytest.fixture(autouse=True)
def clear_oauth2_cache():
    OAuth2ClientCredentialsAuth._cache.clear()
    OAuth2ClientCredentialsAuth._locks.clear()
    yield
    OAuth2ClientCredentialsAuth._cache.clear()
    OAuth2ClientCredentialsAuth._locks.clear()


# ---------------------------------------------------------------------------
# Token endpoint
# ---------------------------------------------------------------------------

def test_token_endpoint_returns_access_token(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_SALESFORCE_APP", "my_client_id:my_client_secret")
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config()) as sim:
        with httpx.Client() as client:
            resp = client.post(f"{_BASE_URL}{_TOKEN_PATH}", data={
                "grant_type": "client_credentials",
                "client_id": "my_client_id",
                "client_secret": "my_client_secret",
            })
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert body["token_type"] == "Bearer"


# ---------------------------------------------------------------------------
# Contacts pagination
# ---------------------------------------------------------------------------

def test_list_contacts_first_page():
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config(page_size=2)) as sim:
        with httpx.Client() as client:
            resp = client.get(
                f"{_BASE_URL}{_QUERY_PATH}",
                params={"q": "SELECT Id FROM Contact"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["results" if "results" in body else "records"]) == 2
    assert "nextRecordsUrl" in body


def test_list_contacts_all_pages():
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config(page_size=2)) as sim:
        with httpx.Client() as client:
            # First page
            resp1 = client.get(
                f"{_BASE_URL}{_QUERY_PATH}",
                params={"q": "SELECT Id FROM Contact"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
            body1 = resp1.json()
            assert len(body1["records"]) == 2
            assert "nextRecordsUrl" in body1

            # Second page (via nextRecordsUrl)
            resp2 = client.get(
                f"{_BASE_URL}{body1['nextRecordsUrl']}",
                headers={"Authorization": "Bearer sim_access_token"},
            )
            body2 = resp2.json()
            assert len(body2["records"]) == 1  # 3 contacts total, page_size=2
            assert body2["done"] is True
            assert "nextRecordsUrl" not in body2


def test_list_contacts_exact_page_boundary():
    """When total records == page_size, no nextRecordsUrl should appear."""
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config(contacts=[{"Id": "1"}, {"Id": "2"}], page_size=2)) as sim:
        with httpx.Client() as client:
            resp = client.get(
                f"{_BASE_URL}{_QUERY_PATH}",
                params={"q": "SELECT Id FROM Contact"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
    body = resp.json()
    assert body["done"] is True
    assert "nextRecordsUrl" not in body


# ---------------------------------------------------------------------------
# Accounts pagination
# ---------------------------------------------------------------------------

def test_list_accounts_first_page():
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config(page_size=2)) as sim:
        with httpx.Client() as client:
            resp = client.get(
                f"{_BASE_URL}{_QUERY_PATH}",
                params={"q": "SELECT Id FROM Account"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["records"]) == 2  # page_size=2, 2 accounts total
    assert body["done"] is True  # exactly page_size, but still done if no more


def test_list_unknown_object_returns_400():
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config()) as sim:
        with httpx.Client() as client:
            resp = client.get(
                f"{_BASE_URL}{_QUERY_PATH}",
                params={"q": "SELECT Id FROM NoSuchObject"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# PATCH Contact
# ---------------------------------------------------------------------------

def test_patch_contact_success():
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config()) as sim:
        with httpx.Client() as client:
            resp = client.patch(
                f"{_BASE_URL}/services/data/{_API_VERSION}/sobjects/Contact/003A000001aAAAA",
                json={"FirstName": "Alicia"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
    assert resp.status_code == 204


def test_patch_contact_not_found():
    connector = make_salesforce_connector_config()
    with GenericSimulator(connector, make_salesforce_sim_config()) as sim:
        with httpx.Client() as client:
            resp = client.patch(
                f"{_BASE_URL}/services/data/{_API_VERSION}/sobjects/Contact/NONEXISTENT",
                json={"FirstName": "Ghost"},
                headers={"Authorization": "Bearer sim_access_token"},
            )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# make_salesforce_connector_config
# ---------------------------------------------------------------------------

def test_make_connector_config_structure():
    cfg = make_salesforce_connector_config()
    assert cfg.name == "salesforce"
    assert "contacts" in cfg.datatypes
    assert "accounts" in cfg.datatypes
    assert cfg.datatypes["contacts"].ingestion is not None
    assert cfg.datatypes["accounts"].ingestion is not None
    assert cfg.auth.type == "oauth2"


def test_connector_config_oauth2_grant_type():
    cfg = make_salesforce_connector_config()
    assert cfg.auth.oauth2.grant_type == "client_credentials"
