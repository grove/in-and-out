"""Unit tests for CLI commands (validate, version, help)."""
from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from inandout.cli.main import app

runner = CliRunner()


def test_version_command():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    # Should print a version string
    assert result.stdout.strip() != ""


def test_app_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "ingest" in result.stdout
    assert "writeback" in result.stdout
    assert "db" in result.stdout


def test_ingest_help():
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout


def test_writeback_help():
    result = runner.invoke(app, ["writeback", "--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout


def test_db_help():
    result = runner.invoke(app, ["db", "--help"])
    assert result.exit_code == 0
    assert "upgrade" in result.stdout
    assert "status" in result.stdout


def test_ingest_run_missing_config(tmp_path):
    result = runner.invoke(app, ["ingest", "run", "--config", str(tmp_path / "nonexistent.yaml")])
    assert result.exit_code == 1


def test_writeback_run_missing_config(tmp_path):
    result = runner.invoke(app, ["writeback", "run", "--config", str(tmp_path / "nonexistent.yaml")])
    assert result.exit_code == 1


def test_validate_missing_connectors_dir(tmp_path):
    result = runner.invoke(app, ["ingest", "validate", "--connectors-dir", str(tmp_path / "nope")])
    assert result.exit_code == 1


def test_validate_empty_directory(tmp_path):
    result = runner.invoke(app, ["ingest", "validate", "--connectors-dir", str(tmp_path)])
    assert result.exit_code == 0


def test_validate_valid_connector(tmp_path):
    """Copy the hubspot example YAML into a temp dir and validate it."""
    example = Path("connectors/hubspot.example.yaml")
    if not example.exists():
        pytest.skip("hubspot.example.yaml not found")

    (tmp_path / "hubspot.yaml").write_text(example.read_text())
    result = runner.invoke(app, ["ingest", "validate", "--connectors-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "OK" in result.stdout


def test_validate_invalid_yaml(tmp_path):
    (tmp_path / "bad.yaml").write_text("this: is: not: valid: yaml: [\n")
    result = runner.invoke(app, ["ingest", "validate", "--connectors-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "FAIL" in result.stdout
