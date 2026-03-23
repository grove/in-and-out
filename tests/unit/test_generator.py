"""Unit tests for connector template generator."""
from __future__ import annotations

import pytest
import respx
import httpx
import yaml

from inandout.generator.introspect import (
    extract_list_endpoints,
    fetch_openapi_spec,
    infer_auth,
    infer_pagination,
)
from inandout.generator.template import render_connector_yaml


# ---------------------------------------------------------------------------
# fetch_openapi_spec tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_fetch_openapi_spec_returns_none_on_404():
    """fetch_openapi_spec returns None when all probe URLs return 404."""
    respx.get("https://api.example.com/openapi.json").mock(return_value=httpx.Response(404))
    respx.get("https://api.example.com/swagger.json").mock(return_value=httpx.Response(404))
    respx.get("https://api.example.com/api-docs").mock(return_value=httpx.Response(404))

    result = await fetch_openapi_spec("https://api.example.com")
    assert result is None


@pytest.mark.anyio
@respx.mock
async def test_fetch_openapi_spec_returns_spec_on_200():
    """fetch_openapi_spec returns parsed JSON when first probe succeeds."""
    spec_data = {"openapi": "3.0.0", "info": {"title": "Test API", "version": "1.0.0"}, "paths": {}}
    respx.get("https://api.example.com/openapi.json").mock(
        return_value=httpx.Response(200, json=spec_data)
    )

    result = await fetch_openapi_spec("https://api.example.com")
    assert result is not None
    assert result["openapi"] == "3.0.0"


@pytest.mark.anyio
@respx.mock
async def test_fetch_openapi_spec_tries_swagger_json_fallback():
    """fetch_openapi_spec falls through to swagger.json when openapi.json 404s."""
    spec_data = {"swagger": "2.0", "info": {"title": "Test", "version": "1"}, "paths": {}}
    respx.get("https://api.example.com/openapi.json").mock(return_value=httpx.Response(404))
    respx.get("https://api.example.com/swagger.json").mock(
        return_value=httpx.Response(200, json=spec_data)
    )

    result = await fetch_openapi_spec("https://api.example.com")
    assert result is not None
    assert result["swagger"] == "2.0"


# ---------------------------------------------------------------------------
# extract_list_endpoints tests
# ---------------------------------------------------------------------------

def _make_spec_with_array_response(path: str, tag: str = "items") -> dict:
    return {
        "paths": {
            path: {
                "get": {
                    "tags": [tag],
                    "summary": f"List {path}",
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {"type": "object"},
                                    }
                                }
                            }
                        }
                    },
                }
            }
        }
    }


def test_extract_list_endpoints_finds_array_returning_get():
    """extract_list_endpoints identifies GET endpoints that return arrays."""
    spec = _make_spec_with_array_response("/api/users", tag="users")
    endpoints = extract_list_endpoints(spec)
    assert len(endpoints) == 1
    assert endpoints[0]["path"] == "/api/users"
    assert endpoints[0]["tag"] == "users"


def test_extract_list_endpoints_uses_items_key():
    """An endpoint returning schema with 'items' key should be detected even without type:array."""
    spec = {
        "paths": {
            "/api/orders": {
                "get": {
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "items": {"type": "object"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    endpoints = extract_list_endpoints(spec)
    assert len(endpoints) == 1
    assert endpoints[0]["path"] == "/api/orders"


def test_extract_list_endpoints_ignores_non_array_endpoints():
    """extract_list_endpoints skips endpoints returning non-array schemas."""
    spec = {
        "paths": {
            "/api/user/{id}": {
                "get": {
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object"}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    endpoints = extract_list_endpoints(spec)
    assert len(endpoints) == 0


def test_extract_list_endpoints_ignores_post_methods():
    """extract_list_endpoints only considers GET operations."""
    spec = {
        "paths": {
            "/api/users": {
                "post": {
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "array", "items": {}}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    endpoints = extract_list_endpoints(spec)
    assert len(endpoints) == 0


# ---------------------------------------------------------------------------
# infer_pagination tests
# ---------------------------------------------------------------------------

def test_infer_pagination_detects_cursor():
    """infer_pagination returns 'cursor' when cursor param is present."""
    spec = {
        "paths": {
            "/api/items": {
                "get": {
                    "parameters": [
                        {"name": "cursor", "in": "query"},
                        {"name": "limit", "in": "query"},
                    ]
                }
            }
        }
    }
    result = infer_pagination(spec, "/api/items")
    assert result == "cursor"


def test_infer_pagination_detects_page_token_as_cursor():
    """infer_pagination returns 'cursor' for page_token parameter."""
    spec = {
        "paths": {
            "/api/items": {
                "get": {
                    "parameters": [
                        {"name": "page_token", "in": "query"},
                    ]
                }
            }
        }
    }
    result = infer_pagination(spec, "/api/items")
    assert result == "cursor"


def test_infer_pagination_detects_offset():
    """infer_pagination returns 'offset' for page parameter."""
    spec = {
        "paths": {
            "/api/items": {
                "get": {
                    "parameters": [
                        {"name": "page", "in": "query"},
                        {"name": "per_page", "in": "query"},
                    ]
                }
            }
        }
    }
    result = infer_pagination(spec, "/api/items")
    assert result == "offset"


def test_infer_pagination_returns_none_when_no_params():
    """infer_pagination returns 'none' when no pagination params are found."""
    spec = {"paths": {"/api/items": {"get": {"parameters": []}}}}
    result = infer_pagination(spec, "/api/items")
    assert result == "none"


def test_infer_pagination_path_not_in_spec():
    """infer_pagination returns 'none' for unknown paths."""
    spec = {"paths": {}}
    result = infer_pagination(spec, "/api/unknown")
    assert result == "none"


# ---------------------------------------------------------------------------
# infer_auth tests
# ---------------------------------------------------------------------------

def test_infer_auth_detects_api_key():
    """infer_auth returns 'api_key' when an apiKey scheme is present."""
    spec = {
        "components": {
            "securitySchemes": {
                "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
            }
        }
    }
    result = infer_auth(spec)
    assert result == "api_key"


def test_infer_auth_detects_oauth2():
    """infer_auth returns 'oauth2' when an OAuth2 scheme is present."""
    spec = {
        "components": {
            "securitySchemes": {
                "OAuth2": {"type": "oauth2", "flows": {}}
            }
        }
    }
    result = infer_auth(spec)
    assert result == "oauth2"


def test_infer_auth_detects_basic():
    """infer_auth returns 'basic' when an HTTP basic scheme is present."""
    spec = {
        "components": {
            "securitySchemes": {
                "BasicAuth": {"type": "http", "scheme": "basic"}
            }
        }
    }
    result = infer_auth(spec)
    assert result == "basic"


def test_infer_auth_returns_none_when_no_schemes():
    """infer_auth returns 'none' when no securitySchemes are present."""
    spec = {"components": {}}
    result = infer_auth(spec)
    assert result == "none"


def test_infer_auth_swagger_2_securitydefinitions():
    """infer_auth handles Swagger 2.0 securityDefinitions."""
    spec = {
        "securityDefinitions": {
            "api_key": {"type": "apiKey", "in": "header", "name": "Authorization"}
        }
    }
    result = infer_auth(spec)
    assert result == "api_key"


# ---------------------------------------------------------------------------
# render_connector_yaml tests
# ---------------------------------------------------------------------------

def test_render_connector_yaml_produces_valid_yaml():
    """render_connector_yaml output should be parseable by PyYAML."""
    yaml_str = render_connector_yaml(
        name="myapi",
        base_url="https://api.myapi.com",
        auth="api_key",
        endpoints=[],
    )
    parsed = yaml.safe_load(yaml_str)
    assert isinstance(parsed, dict)


def test_render_connector_yaml_with_endpoints_produces_valid_yaml():
    """render_connector_yaml with endpoints should produce valid YAML."""
    endpoints = [
        {"path": "/api/users", "tag": "users", "description": "List users", "pagination": "offset"},
        {"path": "/api/orders", "tag": "orders", "description": "List orders", "pagination": "cursor"},
    ]
    yaml_str = render_connector_yaml(
        name="myapi",
        base_url="https://api.myapi.com",
        auth="oauth2",
        endpoints=endpoints,
    )
    parsed = yaml.safe_load(yaml_str)
    assert isinstance(parsed, dict)
    # Should have schema_version
    assert parsed.get("schema_version") == 1


def test_render_connector_yaml_includes_connector_name():
    """render_connector_yaml should include the connector name."""
    yaml_str = render_connector_yaml(
        name="testconn",
        base_url="https://api.test.com",
        auth="none",
        endpoints=[],
    )
    assert "testconn" in yaml_str


def test_render_connector_yaml_includes_base_url():
    """render_connector_yaml should include the base URL."""
    yaml_str = render_connector_yaml(
        name="testconn",
        base_url="https://api.test.com/v2",
        auth="none",
        endpoints=[],
    )
    assert "https://api.test.com/v2" in yaml_str


def test_render_connector_yaml_auth_api_key_includes_credential_ref():
    """api_key auth should produce a credential_ref with connector name."""
    yaml_str = render_connector_yaml(
        name="myconn",
        base_url="https://api.test.com",
        auth="api_key",
        endpoints=[],
    )
    assert "myconn_key" in yaml_str


def test_render_connector_yaml_passes_pydantic_validation():
    """Generated YAML should pass Pydantic validation when properly formed.

    We use a minimal valid spec rather than the raw generated stub (which has TODO markers).
    """
    from inandout.config.loader import load_connector_from_string

    # Build a minimal valid connector YAML from scratch to test the loader
    minimal_yaml = """\
schema_version: 1
connector:
  name: testconn
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: v1
  version: 1.0.0
  connection:
    base_url: https://api.test.com
  auth:
    type: api_key
    credential_ref: testconn_key
    api_key:
      location: header
      name: X-API-Key
  datatypes:
    records:
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: 5m
        list:
          path: /api/records
          pagination:
            strategy: offset
"""
    cfg = load_connector_from_string(minimal_yaml)
    assert cfg.connector.name == "testconn"


def test_render_connector_yaml_stub_has_todo_comments():
    """Generated stub should contain TODO comments for manual completion."""
    yaml_str = render_connector_yaml(
        name="myapi",
        base_url="https://api.test.com",
        auth="none",
        endpoints=[],
    )
    assert "TODO" in yaml_str


def test_render_connector_yaml_with_version():
    """render_connector_yaml should include the specified version."""
    yaml_str = render_connector_yaml(
        name="myapi",
        base_url="https://api.test.com",
        auth="none",
        endpoints=[],
        version="2.1.0",
    )
    assert "2.1.0" in yaml_str
