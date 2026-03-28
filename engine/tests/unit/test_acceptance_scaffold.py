"""Unit tests for Step 55 — Acceptance test suite scaffold."""
from __future__ import annotations

import os

import pytest


# ---------------------------------------------------------------------------
# test_acceptance_conftest_skips_without_env
# ---------------------------------------------------------------------------

def test_hubspot_available_false_when_no_env(monkeypatch):
    """hubspot_available() returns False when env var missing."""
    monkeypatch.delenv("INOUT_ACCEPTANCE_HUBSPOT_API_KEY", raising=False)
    from tests.acceptance.conftest import hubspot_available
    assert hubspot_available() is False


def test_hubspot_available_true_when_env_set(monkeypatch):
    """hubspot_available() returns True when env var present."""
    monkeypatch.setenv("INOUT_ACCEPTANCE_HUBSPOT_API_KEY", "pat-xxx")
    from tests.acceptance.conftest import hubspot_available
    assert hubspot_available() is True


def test_salesforce_available_false_when_no_env(monkeypatch):
    """salesforce_available() returns False when any SF env var missing."""
    for var in ["INOUT_ACCEPTANCE_SF_CLIENT_ID", "INOUT_ACCEPTANCE_SF_CLIENT_SECRET",
                "INOUT_ACCEPTANCE_SF_INSTANCE_URL"]:
        monkeypatch.delenv(var, raising=False)
    from tests.acceptance.conftest import salesforce_available
    assert salesforce_available() is False


def test_salesforce_available_true_when_all_set(monkeypatch):
    """salesforce_available() returns True when all SF env vars present."""
    monkeypatch.setenv("INOUT_ACCEPTANCE_SF_CLIENT_ID", "client-id")
    monkeypatch.setenv("INOUT_ACCEPTANCE_SF_CLIENT_SECRET", "secret")
    monkeypatch.setenv("INOUT_ACCEPTANCE_SF_INSTANCE_URL", "https://myorg.salesforce.com")
    from tests.acceptance.conftest import salesforce_available
    assert salesforce_available() is True


def test_salesforce_available_false_partial_env(monkeypatch):
    """salesforce_available() returns False when only some SF env vars present."""
    monkeypatch.setenv("INOUT_ACCEPTANCE_SF_CLIENT_ID", "client-id")
    monkeypatch.delenv("INOUT_ACCEPTANCE_SF_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("INOUT_ACCEPTANCE_SF_INSTANCE_URL", raising=False)
    from tests.acceptance.conftest import salesforce_available
    assert salesforce_available() is False


# ---------------------------------------------------------------------------
# test_acceptance_markers_registered
# ---------------------------------------------------------------------------

def test_acceptance_marker_registered_in_pyproject():
    """acceptance marker is registered in pyproject.toml pytest config."""
    import tomllib
    from pathlib import Path

    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    assert pyproject_path.exists(), f"pyproject.toml not found at {pyproject_path}"

    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)

    markers = config.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("markers", [])
    acceptance_markers = [m for m in markers if m.startswith("acceptance")]
    assert acceptance_markers, f"acceptance marker not found in pyproject.toml markers: {markers}"
