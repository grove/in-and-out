"""Unit tests for the connector registry / marketplace."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import respx
import httpx

from inandout.registry import (
    ConnectorIndex,
    ConnectorIndexEntry,
    fetch_index,
    install_connector,
    search_connectors,
)


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

SAMPLE_INDEX_JSON = {
    "connectors": [
        {
            "name": "hubspot",
            "version": "1.0.0",
            "description": "HubSpot CRM connector",
            "yaml_url": "https://example.com/connectors/hubspot.yaml",
            "hooks_url": None,
        },
        {
            "name": "salesforce",
            "version": "2.1.0",
            "description": "Salesforce CRM connector with full duplex support",
            "yaml_url": "https://example.com/connectors/salesforce.yaml",
            "hooks_url": "https://example.com/connectors/salesforce_hooks.py",
        },
        {
            "name": "stripe",
            "version": "1.5.0",
            "description": "Stripe payments connector",
            "yaml_url": "https://example.com/connectors/stripe.yaml",
            "hooks_url": None,
        },
    ]
}


# ---------------------------------------------------------------------------
# fetch_index parses a mock JSON response into ConnectorIndex
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_fetch_index_parses_json():
    respx.get("https://test.example.com/index.json").mock(
        return_value=httpx.Response(200, json=SAMPLE_INDEX_JSON)
    )

    index = await fetch_index("https://test.example.com/index.json")

    assert isinstance(index, ConnectorIndex)
    assert len(index.connectors) == 3
    assert index.connectors[0].name == "hubspot"
    assert index.connectors[1].name == "salesforce"
    assert index.connectors[1].hooks_url == "https://example.com/connectors/salesforce_hooks.py"
    assert index.connectors[2].version == "1.5.0"


@pytest.mark.anyio
@respx.mock
async def test_fetch_index_raises_on_http_error():
    respx.get("https://test.example.com/index.json").mock(
        return_value=httpx.Response(404)
    )

    with pytest.raises(httpx.HTTPStatusError):
        await fetch_index("https://test.example.com/index.json")


# ---------------------------------------------------------------------------
# install_connector downloads YAML to dest_dir
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_install_connector_downloads_yaml(tmp_path: Path):
    yaml_content = b"schema_version: 1\nconnector:\n  name: hubspot\n"
    entry = ConnectorIndexEntry(
        name="hubspot",
        version="1.0.0",
        description="HubSpot CRM connector",
        yaml_url="https://example.com/connectors/hubspot.yaml",
        hooks_url=None,
    )

    respx.get("https://example.com/connectors/hubspot.yaml").mock(
        return_value=httpx.Response(200, content=yaml_content)
    )

    yaml_path = await install_connector(entry, tmp_path)

    assert yaml_path == tmp_path / "hubspot.yaml"
    assert yaml_path.exists()
    assert yaml_path.read_bytes() == yaml_content


@pytest.mark.anyio
@respx.mock
async def test_install_connector_downloads_hooks_when_present(tmp_path: Path):
    yaml_content = b"schema_version: 1\n"
    hooks_content = b"# hubspot hooks\ndef transform(record): return record\n"
    entry = ConnectorIndexEntry(
        name="salesforce",
        version="2.1.0",
        description="Salesforce connector",
        yaml_url="https://example.com/connectors/salesforce.yaml",
        hooks_url="https://example.com/connectors/salesforce_hooks.py",
    )

    respx.get("https://example.com/connectors/salesforce.yaml").mock(
        return_value=httpx.Response(200, content=yaml_content)
    )
    respx.get("https://example.com/connectors/salesforce_hooks.py").mock(
        return_value=httpx.Response(200, content=hooks_content)
    )

    yaml_path = await install_connector(entry, tmp_path)

    assert yaml_path == tmp_path / "salesforce.yaml"
    assert yaml_path.exists()
    hooks_path = tmp_path / "salesforce_hooks.py"
    assert hooks_path.exists()
    assert hooks_path.read_bytes() == hooks_content


@pytest.mark.anyio
@respx.mock
async def test_install_connector_creates_dest_dir(tmp_path: Path):
    dest = tmp_path / "new_subdir" / "connectors"
    assert not dest.exists()

    entry = ConnectorIndexEntry(
        name="stripe",
        version="1.0.0",
        description="Stripe",
        yaml_url="https://example.com/stripe.yaml",
        hooks_url=None,
    )
    respx.get("https://example.com/stripe.yaml").mock(
        return_value=httpx.Response(200, content=b"data")
    )

    yaml_path = await install_connector(entry, dest)

    assert dest.exists()
    assert yaml_path.exists()


# ---------------------------------------------------------------------------
# search filters entries by substring match on name/description
# ---------------------------------------------------------------------------

def _make_index() -> ConnectorIndex:
    return ConnectorIndex(
        connectors=[
            ConnectorIndexEntry(
                name="hubspot",
                version="1.0.0",
                description="HubSpot CRM connector",
                yaml_url="https://x.com/h.yaml",
            ),
            ConnectorIndexEntry(
                name="salesforce",
                version="2.0.0",
                description="Salesforce CRM connector",
                yaml_url="https://x.com/s.yaml",
            ),
            ConnectorIndexEntry(
                name="stripe",
                version="1.0.0",
                description="Stripe payments connector",
                yaml_url="https://x.com/st.yaml",
            ),
        ]
    )


def test_search_by_name():
    index = _make_index()
    results = search_connectors(index, "stripe")
    assert len(results) == 1
    assert results[0].name == "stripe"


def test_search_by_description():
    index = _make_index()
    results = search_connectors(index, "CRM")
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"hubspot", "salesforce"}


def test_search_case_insensitive():
    index = _make_index()
    results = search_connectors(index, "PAYMENTS")
    assert len(results) == 1
    assert results[0].name == "stripe"


def test_search_no_results():
    index = _make_index()
    results = search_connectors(index, "zendesk")
    assert results == []


def test_search_empty_query_matches_all():
    index = _make_index()
    results = search_connectors(index, "")
    assert len(results) == 3
