"""Tests: writeback dry-run CLI command (T2 #27)."""
from __future__ import annotations

import pytest
from pathlib import Path
from typer.testing import CliRunner

from inandout.cli.main import app

runner = CliRunner()

# Reuse the validated fixture connectors
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "connectors" / "valid"
WRITEBACK_FIXTURE = FIXTURES_DIR / "minimal_writeback_patch.yaml"
INGESTION_FIXTURE = FIXTURES_DIR / "minimal_ingestion_polling.yaml"


def test_writeback_dry_run_help():
    """writeback dry-run --help exits 0."""
    result = runner.invoke(app, ["writeback", "dry-run", "--help"])
    assert result.exit_code == 0
    assert "dry" in result.output.lower()


def test_writeback_dry_run_missing_connector_arg():
    """writeback dry-run requires --connector argument."""
    result = runner.invoke(app, ["writeback", "dry-run"])
    assert result.exit_code != 0


def test_writeback_dry_run_nonexistent_file(tmp_path):
    """writeback dry-run exits 1 when connector file is not found."""
    missing = str(tmp_path / "missing.yaml")
    result = runner.invoke(app, ["writeback", "dry-run", "--connector", missing])
    assert result.exit_code == 1


def test_writeback_dry_run_invalid_yaml(tmp_path):
    """writeback dry-run exits 1 on schema-invalid connector YAML."""
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("schema_version: 1\nconnector: null\n")
    result = runner.invoke(app, ["writeback", "dry-run", "--connector", str(bad_yaml)])
    assert result.exit_code == 1


@pytest.mark.skipif(not INGESTION_FIXTURE.exists(), reason="Fixture not available")
def test_writeback_dry_run_no_writeback_datatypes():
    """writeback dry-run exits 0 with info message when no writeback datatypes."""
    result = runner.invoke(app, ["writeback", "dry-run", "--connector", str(INGESTION_FIXTURE)])
    assert result.exit_code == 0
    assert "no writeback" in result.output.lower()


@pytest.mark.skipif(not WRITEBACK_FIXTURE.exists(), reason="Fixture not available")
def test_writeback_dry_run_unknown_datatype():
    """writeback dry-run exits 1 when --datatype is not in connector."""
    result = runner.invoke(
        app,
        ["writeback", "dry-run", "--connector", str(WRITEBACK_FIXTURE), "--datatype", "nonexistent"],
    )
    assert result.exit_code == 1
    assert "nonexistent" in result.output


@pytest.mark.skipif(not WRITEBACK_FIXTURE.exists(), reason="Fixture not available")
def test_writeback_dry_run_empty_delta_table():
    """writeback dry-run exits 0 with table output when delta returns no rows."""
    result = runner.invoke(app, ["writeback", "dry-run", "--connector", str(WRITEBACK_FIXTURE)])
    assert result.exit_code == 0
    output = result.output.lower()
    assert "nothing" in output or "no delta" in output or "0 action" in output or "dry-run" in output
