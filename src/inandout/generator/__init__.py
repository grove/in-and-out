"""Connector template generator package."""
from __future__ import annotations

from inandout.generator.introspect import (
    extract_list_endpoints,
    fetch_openapi_spec,
    infer_auth,
    infer_pagination,
)
from inandout.generator.template import render_connector_yaml

__all__ = [
    "fetch_openapi_spec",
    "extract_list_endpoints",
    "infer_pagination",
    "infer_auth",
    "render_connector_yaml",
]
