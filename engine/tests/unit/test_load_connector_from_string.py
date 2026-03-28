"""Unit tests for load_connector_from_string in config/loader.py."""
from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError

from inandout.config.loader import load_connector_from_string

# Minimal valid connector YAML matching actual ConnectorFileConfig schema
_MINIMAL_YAML = """
schema_version: 1
connector:
  name: test_connector
  system: Test System
  generation_profile: ingestion_polling_readonly
  api_version: v1
  connection:
    base_url: https://api.example.com
  auth:
    type: api_key
    credential_ref: test_key
    api_key:
      location: header
      name: X-Api-Key
  datatypes:
    items:
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: "60s"
        list:
          method: GET
          path: /v1/items
          record_selector: results
          pagination:
            strategy: cursor
            cursor:
              response_path: next_cursor
              request_param: after
            termination:
              - missing_next_link
"""


def test_valid_yaml_returns_config():
    cfg = load_connector_from_string(_MINIMAL_YAML)
    assert cfg is not None


def test_connector_name_parsed():
    cfg = load_connector_from_string(_MINIMAL_YAML)
    assert cfg.connector.name == "test_connector"


def test_datatypes_parsed():
    cfg = load_connector_from_string(_MINIMAL_YAML)
    assert "items" in cfg.connector.datatypes


def test_schema_version_parsed():
    cfg = load_connector_from_string(_MINIMAL_YAML)
    assert cfg.schema_version == 1


def test_invalid_yaml_raises_yaml_error():
    bad_yaml = "key: [\nunclosed bracket"
    with pytest.raises(yaml.YAMLError):
        load_connector_from_string(bad_yaml)


def test_missing_required_field_raises_validation_error():
    # Missing 'connector' top-level key
    bad = "schema_version: 1\nsome_other_key: value\n"
    with pytest.raises((ValidationError, Exception)):
        load_connector_from_string(bad)


def test_empty_yaml_raises():
    with pytest.raises(Exception):
        load_connector_from_string("")


def test_none_yaml_raises():
    # yaml.safe_load("null") returns None
    with pytest.raises(Exception):
        load_connector_from_string("null")


def test_generation_profile_parsed():
    cfg = load_connector_from_string(_MINIMAL_YAML)
    assert cfg.connector.generation_profile == "ingestion_polling_readonly"
