"""Unit tests for T1 #43: ingest validate-connector connectivity probe."""
from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from typer.testing import CliRunner

from inandout.cli.main import app


runner = CliRunner()

# Use the project's own valid fixture so the schema always parses correctly.
VALID_FIXTURE = Path(__file__).parents[2] / "fixtures" / "connectors" / "valid" / "minimal_ingestion_polling.yaml"


# ---------------------------------------------------------------------------
# --skip-connectivity skips probe
# ---------------------------------------------------------------------------

def test_validate_connector_skip_connectivity():
    """With --skip-connectivity the table shows schema result without connectivity row."""
    result = runner.invoke(app, [
        "ingest", "validate-connector",
        "--connector", str(VALID_FIXTURE),
        "--skip-connectivity",
    ])
    assert result.exit_code == 0
    assert "schema" in result.output
    assert "connectivity" not in result.output


# ---------------------------------------------------------------------------
# --check-connectivity with successful probe
# ---------------------------------------------------------------------------

def test_validate_connector_connectivity_success():
    """With --check-connectivity and a 200 response, table shows connectivity OK."""
    mock_response = MagicMock()
    mock_response.status_code = 200

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = runner.invoke(app, [
            "ingest", "validate-connector",
            "--connector", str(VALID_FIXTURE),
            "--check-connectivity",
        ])

    assert result.exit_code == 0
    assert "connectivity" in result.output
    assert "FAIL" not in result.output


# ---------------------------------------------------------------------------
# --check-connectivity with server error
# ---------------------------------------------------------------------------

def test_validate_connector_connectivity_server_error():
    """With --check-connectivity and a 500 response, table shows WARN."""
    mock_response = MagicMock()
    mock_response.status_code = 500

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = runner.invoke(app, [
            "ingest", "validate-connector",
            "--connector", str(VALID_FIXTURE),
            "--check-connectivity",
        ])

    assert result.exit_code == 0
    assert "connectivity" in result.output
    assert "WARN" in result.output


# ---------------------------------------------------------------------------
# --check-connectivity with network error
# ---------------------------------------------------------------------------

def test_validate_connector_connectivity_network_error():
    """With --check-connectivity and a connection error, shows WARN with error text."""
    import httpx

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = runner.invoke(app, [
            "ingest", "validate-connector",
            "--connector", str(VALID_FIXTURE),
            "--check-connectivity",
        ])

    assert result.exit_code == 0
    assert "connectivity" in result.output
    assert "WARN" in result.output


# ---------------------------------------------------------------------------
# Missing file still fails fast
# ---------------------------------------------------------------------------

def test_validate_connector_missing_file(tmp_path):
    """Missing connector file returns exit code 1 immediately."""
    result = runner.invoke(app, [
        "ingest", "validate-connector",
        "--connector", str(tmp_path / "nonexistent.yaml"),
        "--skip-connectivity",
    ])
    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Invalid YAML fails with schema error
# ---------------------------------------------------------------------------

def test_validate_connector_invalid_yaml(tmp_path):
    """Completely invalid YAML returns exit code 1 with FAIL status."""
    p = tmp_path / "bad.yaml"
    p.write_text("not_connector_key: oops\n")
    result = runner.invoke(app, [
        "ingest", "validate-connector",
        "--connector", str(p),
        "--skip-connectivity",
    ])
    assert result.exit_code == 1
    assert "FAIL" in result.output
