"""Unit tests for ConnectorConfig minimal validation."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import (
    AccountConfig,
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.ingestion import HistoryMode, IngestionConfig, ScheduleConfig


def _minimal_list() -> dict:
    return {"path": "/items", "pagination": {"strategy": "link_header"}}


def _minimal_ingestion() -> IngestionConfig:
    return IngestionConfig(
        primary_key="id",
        history_mode=HistoryMode.overwrite,
        schedule=ScheduleConfig(interval="30s"),
        **{"list": _minimal_list()},
    )


def _minimal_auth() -> ApiKeyAuth:
    return ApiKeyAuth(
        type="api_key",
        credential_ref="MY_KEY",
        api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
    )


def _minimal_connector(**overrides) -> dict:
    base = {
        "name": "my_connector",
        "system": "MySystem",
        "generation_profile": "ingestion_polling_readonly",
        "api_version": "v1",
        "connection": ConnectionConfig(base_url="https://api.example.com"),
        "auth": _minimal_auth(),
        "datatypes": {
            "contacts": DatatypeConfig(ingestion=_minimal_ingestion()),
        },
    }
    base.update(overrides)
    return base


# --- Name pattern validation ---

def test_valid_name():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.name == "my_connector"


def test_name_with_hyphen_valid():
    cfg = ConnectorConfig(**_minimal_connector(name="my-connector"))
    assert cfg.name == "my-connector"


def test_name_starts_with_digit_raises():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(name="1invalid"))


def test_name_uppercase_raises():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(name="MyConnector"))


def test_name_empty_raises():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(name=""))


# --- system min_length ---

def test_system_empty_raises():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(system=""))


# --- api_version min_length ---

def test_api_version_empty_raises():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(api_version=""))


# --- datatypes min_length ---

def test_datatypes_empty_raises():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(datatypes={}))


# --- Default field values ---

def test_version_default():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.version == "1.0.0"


def test_description_default_none():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.description is None


def test_rate_limit_default_none():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.rate_limit is None


def test_webhooks_default_none():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.webhooks is None


def test_depends_on_default_empty():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.depends_on == []


def test_accounts_default_empty():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.accounts == []


def test_generation_profile_stored():
    cfg = ConnectorConfig(**_minimal_connector())
    assert cfg.generation_profile == GenerationProfile.ingestion_polling_readonly


# --- extra field forbidden ---

def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        ConnectorConfig(**_minimal_connector(unknown_field="bad"))
