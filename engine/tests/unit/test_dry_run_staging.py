"""Unit tests for dry-run staging environment support (Step 81)."""
from __future__ import annotations

import pytest
import respx
import httpx
from typer.testing import CliRunner

from inandout.cli.main import app

runner = CliRunner()

STAGING_CONNECTOR_YAML = """\
schema_version: 1
connector:
  name: testconn
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: "v1"
  connection:
    base_url: https://api.production.example
    staging_base_url: https://api.staging.example
  auth:
    type: api_key
    credential_ref: testconn_key
    api_key:
      location: header
      name: X-API-Key
  datatypes:
    contacts:
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: 5m
        list:
          method: GET
          path: /contacts
          record_selector: results
          pagination:
            strategy: offset
            offset:
              page_size: 100
              offset_param: offset
              limit_param: limit
"""

NO_STAGING_CONNECTOR_YAML = """\
schema_version: 1
connector:
  name: testconn
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: "v1"
  connection:
    base_url: https://api.production.example
  auth:
    type: api_key
    credential_ref: testconn_key
    api_key:
      location: header
      name: X-API-Key
  datatypes:
    contacts:
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: 5m
        list:
          method: GET
          path: /contacts
          record_selector: results
          pagination:
            strategy: offset
            offset:
              page_size: 100
              offset_param: offset
              limit_param: limit
"""


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

@respx.mock
def test_dry_run_staging_uses_staging_url(tmp_path, monkeypatch):
    """--env staging should target staging_base_url."""
    monkeypatch.setenv("INOUT_CREDENTIAL_TESTCONN_KEY", "dummy-secret")

    connector_file = tmp_path / "testconn.yaml"
    connector_file.write_text(STAGING_CONNECTOR_YAML)

    # Mock ONLY the staging URL — production should NOT be called
    respx.get("https://api.staging.example/contacts").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": "s1", "name": "StagingUser"}],
            },
        )
    )

    result = runner.invoke(
        app,
        [
            "ingest", "dry-run",
            "--connector", str(connector_file),
            "--datatype", "contacts",
            "--env", "staging",
        ],
    )

    assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput:\n{result.output}"
    assert "staging" in result.output.lower() or "Would insert" in result.output


@respx.mock
def test_dry_run_production_uses_base_url(tmp_path, monkeypatch):
    """--env production (default) should target base_url."""
    monkeypatch.setenv("INOUT_CREDENTIAL_TESTCONN_KEY", "dummy-secret")

    connector_file = tmp_path / "testconn.yaml"
    connector_file.write_text(STAGING_CONNECTOR_YAML)

    # Mock ONLY the production URL
    respx.get("https://api.production.example/contacts").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": "p1", "name": "ProdUser"}],
            },
        )
    )

    result = runner.invoke(
        app,
        [
            "ingest", "dry-run",
            "--connector", str(connector_file),
            "--datatype", "contacts",
            "--env", "production",
        ],
    )

    assert result.exit_code == 0, f"Exit code: {result.exit_code}\nOutput:\n{result.output}"
    assert "Would insert" in result.output


def test_dry_run_missing_staging_url_raises_error(tmp_path, monkeypatch):
    """--env staging without staging_base_url should fail with a helpful error."""
    monkeypatch.setenv("INOUT_CREDENTIAL_TESTCONN_KEY", "dummy-secret")

    connector_file = tmp_path / "testconn.yaml"
    connector_file.write_text(NO_STAGING_CONNECTOR_YAML)

    result = runner.invoke(
        app,
        [
            "ingest", "dry-run",
            "--connector", str(connector_file),
            "--datatype", "contacts",
            "--env", "staging",
        ],
    )

    assert result.exit_code == 1
    # The error message should mention staging_base_url
    assert "staging" in result.output.lower() or "staging" in (result.stderr or "").lower() or \
        "staging" in str(result.exception).lower()


# ---------------------------------------------------------------------------
# DryRunResult dataclass tests
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.anyio
async def test_dry_run_result_counts_applied_mappings(tmp_path, monkeypatch):
    """DryRunResult.applied_mappings should count records with field mappings applied."""
    monkeypatch.setenv("INOUT_CREDENTIAL_TESTCONN_KEY", "dummy-secret")

    yaml_with_mapping = """\
schema_version: 1
connector:
  name: testconn
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: "v1"
  connection:
    base_url: https://api.production.example
    staging_base_url: https://api.staging.example
  auth:
    type: api_key
    credential_ref: testconn_key
    api_key:
      location: header
      name: X-API-Key
  datatypes:
    contacts:
      field_mappings:
        - source: full_name
          target: name
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: 5m
        list:
          method: GET
          path: /contacts
          record_selector: results
          pagination:
            strategy: offset
            offset:
              page_size: 100
              offset_param: offset
              limit_param: limit
"""
    connector_file = tmp_path / "testconn.yaml"
    connector_file.write_text(yaml_with_mapping)

    from inandout.config.loader import load_connector
    from inandout.ingestion.dry_run import dry_run_connector

    cfg = load_connector(connector_file)

    respx.get("https://api.staging.example/contacts").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": "1", "full_name": "Alice"}],
            },
        )
    )

    result = await dry_run_connector(cfg.connector, "contacts", env="staging")
    assert result.applied_mappings >= 1
    assert result.env == "staging"


@respx.mock
@pytest.mark.anyio
async def test_dry_run_result_missing_staging_url():
    """dry_run_connector should raise ValueError when staging_base_url is missing."""
    import yaml as _yaml
    from inandout.config.connector import (
        ConnectorConfig, ConnectionConfig, DatatypeConfig,
    )
    from inandout.config.ingestion import IngestionConfig, ListConfig
    from inandout.config.pagination import PaginationConfig
    from inandout.config.auth import AuthConfig
    from inandout.ingestion.dry_run import dry_run_connector

    # Build minimal config manually
    import yaml as _yaml
    import tempfile
    from pathlib import Path
    from inandout.config.loader import load_connector

    yaml_content = """\
schema_version: 1
connector:
  name: nostagin
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: "v1"
  connection:
    base_url: https://api.production.example
  auth:
    type: api_key
    credential_ref: nostagin_key
    api_key:
      location: header
      name: X-API-Key
  datatypes:
    items:
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: 5m
        list:
          method: GET
          path: /items
          record_selector: results
          pagination:
            strategy: offset
            offset:
              page_size: 100
              offset_param: offset
              limit_param: limit
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        tmp = Path(f.name)

    cfg = load_connector(tmp)
    tmp.unlink()

    with pytest.raises(ValueError, match="staging_base_url"):
        await dry_run_connector(cfg.connector, "items", env="staging")
