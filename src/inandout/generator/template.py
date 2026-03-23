"""Connector YAML template renderer using Python string templates."""
from __future__ import annotations

import re
from string import Template


# ---------------------------------------------------------------------------
# Auth section templates
# ---------------------------------------------------------------------------

_AUTH_TEMPLATES: dict[str, str] = {
    "api_key": """\
  auth:
    type: api_key
    credential_ref: ${name}_key
    header: X-API-Key
    # TODO: update header name to match the API's actual auth header""",

    "oauth2": """\
  auth:
    type: oauth2_client_credentials
    credential_ref: ${name}_oauth
    token_url: https://auth.example.com/oauth/token  # TODO: set real token URL
    client_id: $${runtime.client_id}
    # TODO: configure OAuth2 scopes""",

    "basic": """\
  auth:
    type: basic
    credential_ref: ${name}_credentials
    # TODO: configure username/password credential""",

    "none": """\
  auth:
    type: none""",
}

# ---------------------------------------------------------------------------
# Pagination stub templates
# ---------------------------------------------------------------------------

_PAGINATION_CURSOR = """\
        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: next_cursor
          # TODO: verify cursor parameter and response field names"""

_PAGINATION_OFFSET = """\
        pagination:
          type: page_offset
          page_param: page
          page_size_param: per_page
          page_size: 100
          # TODO: verify page parameter names"""

_PAGINATION_NONE = """\
        pagination:
          type: none"""


def _make_datatype_name(path: str) -> str:
    """Convert an API path to a safe datatype name."""
    # Remove leading slash and path params
    name = path.lstrip("/")
    name = re.sub(r"\{[^}]+\}", "", name)  # strip {param}
    name = re.sub(r"[^a-zA-Z0-9_/]", "_", name)
    name = name.replace("/", "_").strip("_")
    name = re.sub(r"_+", "_", name)
    return name.lower() or "records"


def render_connector_yaml(
    name: str,
    base_url: str,
    auth: str,
    endpoints: list[dict],
    version: str = "1.0.0",
) -> str:
    """Render a connector YAML string.

    Args:
        name: Connector name (used in table names and credential refs).
        base_url: Base URL of the API.
        auth: Auth type — one of "api_key", "oauth2", "basic", "none".
        endpoints: List of endpoint dicts from extract_list_endpoints().
        version: Connector version string.

    Returns:
        A YAML string ready to be written to disk.
    """
    safe_name = re.sub(r"[^a-z0-9_-]", "_", name.lower()).strip("_") or "connector"
    auth_block = _AUTH_TEMPLATES.get(auth, _AUTH_TEMPLATES["none"])
    # Substitute ${name} in auth block
    auth_block = Template(auth_block).safe_substitute(name=safe_name)

    if not endpoints:
        # Minimal stub with a single placeholder datatype
        datatypes_block = _render_stub_datatype(safe_name)
    else:
        dtype_lines: list[str] = []
        for ep in endpoints:
            dt_name = _make_datatype_name(ep["path"])
            if not dt_name:
                dt_name = "records"
            pag_type = ep.get("pagination", "none")
            if pag_type == "cursor":
                pag_block = _PAGINATION_CURSOR
            elif pag_type == "offset":
                pag_block = _PAGINATION_OFFSET
            else:
                pag_block = _PAGINATION_NONE

            description = ep.get("description", "")
            desc_line = f"    description: {description!r}" if description else "    # TODO: add description"
            dtype_lines.append(f"""\
  {dt_name}:
{desc_line}
    ingestion:
      primary_key: id  # TODO: set actual primary key field
      history_mode: overwrite
      schedule:
        interval: 5m
      list:
        method: GET
        path: {ep['path']}
        record_selector: $.items  # TODO: verify record selector path
{pag_block}
      incremental:
        enabled: true
        cursor_field: updated_at  # TODO: verify cursor field name
        cursor_type: timestamp
""")
        datatypes_block = "\n".join(dtype_lines)

    yaml_str = f"""\
schema_version: 1
connector:
  name: {safe_name}
  system: {name}
  generation_profile: ingestion_polling_readonly
  description: "Auto-generated connector for {name}"  # TODO: update description
  api_version: v1  # TODO: set actual API version
  version: {version}
  connection:
    base_url: {base_url}
    timeout:
      connect: 10s
      read: 30s
{auth_block}
  datatypes:
{datatypes_block}
"""
    return yaml_str


def render_connector_test(name: str, base_url: str, datatypes: list[str]) -> str:
    """Render a Python test file for a generated connector.

    Args:
        name: Connector name.
        base_url: Base URL of the API.
        datatypes: List of datatype names discovered from the spec.

    Returns:
        A Python source string to be written as ``test_{name}_connector.py``.
    """
    safe_name = re.sub(r"[^a-z0-9_-]", "_", name.lower()).strip("_") or "connector"
    name_upper = safe_name.upper().replace("-", "_")

    dtypes_list = repr(datatypes) if datatypes else "[]"

    return f'''\
"""Tests for {safe_name} connector — generated by `inandout connector new`."""
from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest
import respx

CONNECTOR_PATH = Path(__file__).parent / "{safe_name}.yaml"


def test_yaml_valid():
    """Verify that the generated YAML passes Pydantic schema validation."""
    from inandout.config.loader import load_connector
    cfg = load_connector(CONNECTOR_PATH)
    assert cfg.connector.name == "{safe_name}"


def test_credentials_resolvable():
    """Verify all credential_ref fields have corresponding env vars."""
    os.environ["INOUT_CREDENTIAL_{name_upper}_KEY"] = "test-key"
    from inandout.config.loader import load_connector
    from inandout.testing.cases import test_credentials_resolvable
    cfg = load_connector(CONNECTOR_PATH)
    result = test_credentials_resolvable(cfg)
    assert result.passed, result.message


@pytest.mark.anyio
async def test_mock_fetch_one_page():
    """Smoke test: one page of results returned from a mock API."""
    with respx.mock(base_url="{base_url}", assert_all_called=False) as mock:
        # TODO: update path and response to match your API
        mock.get("/").mock(return_value=httpx.Response(
            200, json={{"results": [{{"id": "test-1"}}], "next_cursor": None}}
        ))
        # TODO: import and invoke your connector\'s ingestion runner here
        # from inandout.testing.runner import run_connector_tests
        # suite = await run_connector_tests(CONNECTOR_PATH)
        # fetch_result = next(r for r in suite.results if r.test_name == "test_mock_fetch_one_page")
        # assert fetch_result.passed, fetch_result.message
        pass  # Remove this and uncomment the lines above after filling in the TODO
'''


def _render_stub_datatype(name: str) -> str:
    """Render a minimal stub datatype when no endpoints were discovered."""
    return f"""\
  records:
    # TODO: rename this datatype and add more as needed
    ingestion:
      primary_key: id  # TODO: set actual primary key field
      history_mode: overwrite
      schedule:
        interval: 5m
      list:
        method: GET
        path: /api/records  # TODO: set actual endpoint path
        record_selector: $.items  # TODO: verify record selector path
{_PAGINATION_OFFSET}
      incremental:
        enabled: true
        cursor_field: updated_at  # TODO: verify cursor field
        cursor_type: timestamp
"""
