"""Unit tests for the dry-run CLI command."""
from __future__ import annotations

import pytest
import respx
import httpx
from typer.testing import CliRunner

from inandout.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Minimal connector YAML for tests
# ---------------------------------------------------------------------------

MINIMAL_CONNECTOR_YAML = """\
schema_version: 1
connector:
  name: testconn
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: "v1"
  connection:
    base_url: https://api.test.example
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
# validate-connector command
# ---------------------------------------------------------------------------

def test_validate_connector_valid(tmp_path):
    connector_file = tmp_path / "testconn.yaml"
    connector_file.write_text(MINIMAL_CONNECTOR_YAML)

    result = runner.invoke(app, ["ingest", "validate-connector", "--connector", str(connector_file)])
    assert result.exit_code == 0
    assert "OK" in result.stdout


def test_validate_connector_file_not_found(tmp_path):
    result = runner.invoke(
        app, ["ingest", "validate-connector", "--connector", str(tmp_path / "missing.yaml")]
    )
    assert result.exit_code == 1


def test_validate_connector_invalid_yaml(tmp_path):
    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text("not: valid: yaml: [\n")
    result = runner.invoke(app, ["ingest", "validate-connector", "--connector", str(bad_file)])
    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# dry-run command — mock HTTP call with respx
# ---------------------------------------------------------------------------

@respx.mock
def test_dry_run_returns_record_previews_without_db(tmp_path, monkeypatch):
    """dry-run should fetch records and display them without writing to DB."""
    monkeypatch.setenv("INOUT_CREDENTIAL_TESTCONN_KEY", "dummy-secret")

    connector_file = tmp_path / "testconn.yaml"
    connector_file.write_text(MINIMAL_CONNECTOR_YAML)

    # Mock the HTTP response
    respx.get("https://api.test.example/contacts").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {"id": "1", "name": "Alice", "email": "alice@example.com"},
                    {"id": "2", "name": "Bob", "email": "bob@example.com"},
                ]
            },
        )
    )

    result = runner.invoke(
        app,
        [
            "ingest", "dry-run",
            "--connector", str(connector_file),
            "--datatype", "contacts",
            "--limit", "10",
        ],
    )

    assert result.exit_code == 0, f"Exit code was {result.exit_code}. Output:\n{result.output}"
    assert "Would insert" in result.output
    assert "testconn" in result.output or "contacts" in result.output


@respx.mock
def test_dry_run_connector_not_found(tmp_path):
    result = runner.invoke(
        app,
        ["ingest", "dry-run", "--connector", str(tmp_path / "nonexistent.yaml")],
    )
    assert result.exit_code == 1
