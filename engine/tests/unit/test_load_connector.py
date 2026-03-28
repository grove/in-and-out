"""Unit tests for load_connector (file-based) in config/loader.py."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from inandout.config.loader import load_connector

_FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "connectors"
_VALID_DIR = _FIXTURES_DIR / "valid"
_INVALID_DIR = _FIXTURES_DIR / "invalid"


def test_load_minimal_ingestion_polling():
    cfg = load_connector(_VALID_DIR / "minimal_ingestion_polling.yaml")
    assert cfg.connector.name == "demo_polling"


def test_load_minimal_full_duplex():
    cfg = load_connector(_VALID_DIR / "minimal_full_duplex.yaml")
    assert cfg is not None
    assert cfg.connector.generation_profile == "full_duplex"


def test_load_minimal_writeback_patch():
    cfg = load_connector(_VALID_DIR / "minimal_writeback_patch.yaml")
    assert cfg is not None


def test_load_minimal_ingestion_webhook():
    cfg = load_connector(_VALID_DIR / "minimal_ingestion_webhook_incremental.yaml")
    assert cfg is not None


def test_missing_file_raises_file_not_found_error():
    with pytest.raises(FileNotFoundError):
        load_connector("/this/path/does/not/exist.yaml")


def test_invalid_schema_version_raises():
    with pytest.raises(Exception):
        load_connector(_INVALID_DIR / "missing_schema_version.yaml")


def test_returns_connector_file_config_type():
    from inandout.config.connector import ConnectorFileConfig
    cfg = load_connector(_VALID_DIR / "minimal_ingestion_polling.yaml")
    assert isinstance(cfg, ConnectorFileConfig)


def test_connector_has_datatypes():
    cfg = load_connector(_VALID_DIR / "minimal_ingestion_polling.yaml")
    assert len(cfg.connector.datatypes) >= 1


def test_accepts_path_object():
    cfg = load_connector(Path(_VALID_DIR / "minimal_ingestion_polling.yaml"))
    assert cfg is not None


def test_accepts_string_path():
    cfg = load_connector(str(_VALID_DIR / "minimal_ingestion_polling.yaml"))
    assert cfg is not None
