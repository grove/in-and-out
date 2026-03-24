"""Unit tests for load_ingestion_tool_config / load_writeback_tool_config
with env var interpolation."""
from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from inandout.config.loader import load_ingestion_tool_config, load_writeback_tool_config


_INGESTION_YAML = textwrap.dedent("""\
database:
  dsn: "${TEST_DATABASE_URL}"
connectors_dir: /connectors
""")

_WRITEBACK_YAML = textwrap.dedent("""\
database:
  dsn: "${TEST_DATABASE_URL}"
connectors_dir: /connectors
""")


@pytest.fixture
def ingestion_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "ingestion.yaml"
    p.write_text(_INGESTION_YAML)
    return p


@pytest.fixture
def writeback_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "writeback.yaml"
    p.write_text(_WRITEBACK_YAML)
    return p


def test_load_ingestion_tool_config_interpolates_env(ingestion_yaml, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://localhost/testdb")
    cfg = load_ingestion_tool_config(ingestion_yaml)
    assert cfg.database.dsn == "postgresql://localhost/testdb"


def test_load_writeback_tool_config_interpolates_env(writeback_yaml, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://localhost/testdb")
    cfg = load_writeback_tool_config(writeback_yaml)
    assert cfg.database.dsn == "postgresql://localhost/testdb"


def test_load_ingestion_missing_env_var_raises(ingestion_yaml, monkeypatch):
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    with pytest.raises(EnvironmentError, match="TEST_DATABASE_URL"):
        load_ingestion_tool_config(ingestion_yaml)


def test_load_writeback_missing_env_var_raises(writeback_yaml, monkeypatch):
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    with pytest.raises(EnvironmentError, match="TEST_DATABASE_URL"):
        load_writeback_tool_config(writeback_yaml)


def test_load_ingestion_accepts_path_object(ingestion_yaml, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://localhost/testdb")
    cfg = load_ingestion_tool_config(Path(ingestion_yaml))
    assert cfg is not None


def test_load_writeback_accepts_string_path(writeback_yaml, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://localhost/testdb")
    cfg = load_writeback_tool_config(str(writeback_yaml))
    assert cfg is not None


def test_load_ingestion_missing_file_raises(monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "x")
    with pytest.raises((FileNotFoundError, OSError)):
        load_ingestion_tool_config("/no/such/file.yaml")
