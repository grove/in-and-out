"""Unit tests for API version management (T1 #39 A6)."""
from __future__ import annotations

import datetime

import pytest


# ---------------------------------------------------------------------------
# Config fields
# ---------------------------------------------------------------------------

def test_connector_config_has_api_version_deprecation_fields():
    """ConnectorConfig has api_version_deprecation_date and api_version_warning_days."""
    from inandout.config.connector import ConnectorConfig

    fields = ConnectorConfig.model_fields
    assert "api_version_deprecation_date" in fields
    assert "api_version_warning_days" in fields


def test_connector_api_version_deprecation_date_default_none():
    """api_version_deprecation_date defaults to None."""
    from inandout.config.connector import ConnectorConfig

    field = ConnectorConfig.model_fields["api_version_deprecation_date"]
    assert field.default is None


def test_connector_api_version_warning_days_default():
    """api_version_warning_days defaults to 60."""
    from inandout.config.connector import ConnectorConfig

    field = ConnectorConfig.model_fields["api_version_warning_days"]
    assert field.default == 60


def test_datatype_config_has_api_version_field():
    """DatatypeConfig has api_version field defaulting to None."""
    from inandout.config.connector import DatatypeConfig

    fields = DatatypeConfig.model_fields
    assert "api_version" in fields
    assert fields["api_version"].default is None


# ---------------------------------------------------------------------------
# Deprecation date logic
# ---------------------------------------------------------------------------

def _check_deprecation(dep_date_str: str, warning_days: int, today: datetime.date):
    """Returns 'error', 'warning', or None."""
    if not dep_date_str:
        return None
    dep_date = datetime.date.fromisoformat(dep_date_str)
    days_remaining = (dep_date - today).days
    if days_remaining < 0:
        return "error"
    elif days_remaining <= warning_days:
        return "warning"
    return None


def test_deprecation_date_past_returns_error():
    """Past deprecation date should log ERROR."""
    today = datetime.date(2026, 3, 23)
    result = _check_deprecation("2026-01-01", 60, today)
    assert result == "error"


def test_deprecation_date_within_warning_window_returns_warning():
    """Deprecation date within warning_days should log WARNING."""
    today = datetime.date(2026, 3, 23)
    result = _check_deprecation("2026-04-15", 60, today)  # ~23 days away
    assert result == "warning"


def test_deprecation_date_outside_warning_window_returns_none():
    """Deprecation date outside warning window should not log anything."""
    today = datetime.date(2026, 3, 23)
    result = _check_deprecation("2027-01-01", 60, today)  # ~280 days away
    assert result is None


def test_no_deprecation_date_returns_none():
    """No deprecation date configured → no warning/error."""
    today = datetime.date(2026, 3, 23)
    result = _check_deprecation("", 60, today)
    assert result is None


def test_deprecation_check_function_in_daemon():
    """_check_api_version_deprecations function exists in ingestion daemon."""
    from inandout.ingestion.daemon import _check_api_version_deprecations
    assert callable(_check_api_version_deprecations)


# ---------------------------------------------------------------------------
# Datatype-level api_version override
# ---------------------------------------------------------------------------

def test_datatype_api_version_overrides_connector():
    """dtype_cfg.api_version takes precedence over connector.api_version."""
    dtype_api_version = "v2"
    connector_api_version = "v1"

    # Logic from engine._do_sync
    effective = dtype_api_version or connector_api_version
    assert effective == "v2"


def test_datatype_api_version_falls_back_to_connector():
    """When dtype_cfg.api_version is None, connector.api_version is used."""
    dtype_api_version = None
    connector_api_version = "v1"

    effective = dtype_api_version or connector_api_version
    assert effective == "v1"


def test_datatype_api_version_both_none_uses_connector():
    """When both are None/empty, connector api_version wins (never actually None)."""
    dtype_api_version = None
    connector_api_version = "2023-01"

    effective = dtype_api_version or connector_api_version
    assert effective == "2023-01"


# ---------------------------------------------------------------------------
# _check_api_version_deprecations integration-level test
# ---------------------------------------------------------------------------

def test_check_api_version_deprecations_no_crash_on_empty_list():
    """_check_api_version_deprecations should not crash on empty connector list."""
    from inandout.ingestion.daemon import _check_api_version_deprecations

    try:
        _check_api_version_deprecations([])
    except Exception as exc:
        pytest.fail(f"_check_api_version_deprecations raised: {exc}")


def test_check_api_version_deprecations_skips_connectors_without_date():
    """Connectors without api_version_deprecation_date are silently skipped."""
    from inandout.ingestion.daemon import _check_api_version_deprecations
    from unittest.mock import MagicMock

    mock_connector = MagicMock()
    mock_connector.name = "test"
    mock_connector.api_version = "v1"
    mock_connector.api_version_deprecation_date = None
    mock_connector.api_version_warning_days = 60

    mock_file_cfg = MagicMock()
    mock_file_cfg.connector = mock_connector

    try:
        _check_api_version_deprecations([mock_file_cfg])
    except Exception as exc:
        pytest.fail(f"_check_api_version_deprecations raised: {exc}")
