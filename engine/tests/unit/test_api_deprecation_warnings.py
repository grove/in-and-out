"""Unit tests for API deprecation warnings (T1 #39)."""
from __future__ import annotations

import datetime as _dt
from unittest.mock import MagicMock

import pytest

from inandout.ingestion.daemon import _check_api_deprecations


def _make_connector_config(
    name: str = "test",
    api_version: str = "v1",
    api_version_deprecation_date: str | None = None,
    api_deprecation_deadline: str | None = None,
    api_version_warning_days: int = 60,
):
    """Build a minimal connector config for testing."""
    cfg = MagicMock()
    cfg.connector = MagicMock()
    cfg.connector.name = name
    cfg.connector.api_version = api_version
    cfg.connector.api_version_deprecation_date = api_version_deprecation_date
    cfg.connector.api_deprecation_deadline = api_deprecation_deadline
    cfg.connector.api_version_warning_days = api_version_warning_days
    return cfg


def test_no_deprecation_no_warning():
    """No warnings when no deprecation dates are set."""
    cfg = _make_connector_config()
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.warning.assert_not_called()
    log.error.assert_not_called()


def test_deprecation_date_in_future_no_warning():
    """No warning when deprecation date is far in the future."""
    tomorrow = (_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=100)).date()
    cfg = _make_connector_config(
        api_version_deprecation_date=tomorrow.isoformat(),
        api_version_warning_days=60,
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.warning.assert_not_called()
    log.error.assert_not_called()


def test_deprecation_date_approaching_warns():
    """Warning when deprecation date is within warning window."""
    soon = (_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=30)).date()
    cfg = _make_connector_config(
        api_version_deprecation_date=soon.isoformat(),
        api_version_warning_days=60,
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.warning.assert_called_once()
    call_args = log.warning.call_args
    assert call_args[0][0] == "api_version_deprecation_approaching"
    assert call_args[1]["connector"] == "test"
    assert "days_remaining" in call_args[1]


def test_deprecation_date_passed_errors():
    """Error when deprecation date has passed."""
    past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=10)).date()
    cfg = _make_connector_config(
        api_version_deprecation_date=past.isoformat(),
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.error.assert_called_once()
    call_args = log.error.call_args
    assert call_args[0][0] == "api_version_deprecated"
    assert call_args[1]["connector"] == "test"


def test_invalid_deprecation_date_format():
    """Warning when deprecation date format is invalid."""
    cfg = _make_connector_config(
        api_version_deprecation_date="not-a-date",
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.warning.assert_called_once()
    call_args = log.warning.call_args
    assert call_args[0][0] == "api_deprecation_date_invalid"


def test_legacy_api_deprecation_deadline_approaching():
    """Warning when legacy api_deprecation_deadline is within 60 days."""
    soon = (_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=30)).date()
    cfg = _make_connector_config(
        api_deprecation_deadline=soon.isoformat(),
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.warning.assert_called_once()
    call_args = log.warning.call_args
    assert call_args[0][0] == "api_deprecation_approaching"


def test_legacy_api_deprecation_deadline_passed():
    """Error when legacy api_deprecation_deadline has passed."""
    past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=5)).date()
    cfg = _make_connector_config(
        api_deprecation_deadline=past.isoformat(),
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.error.assert_called_once()
    call_args = log.error.call_args
    assert call_args[0][0] == "api_deprecated"


def test_multiple_connectors_independent():
    """Warnings for one connector don't affect others."""
    past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=10)).date()
    future = (_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=100)).date()
    
    cfg1 = _make_connector_config(
        name="deprecated",
        api_version_deprecation_date=past.isoformat(),
    )
    cfg2 = _make_connector_config(
        name="current",
        api_version_deprecation_date=future.isoformat(),
    )
    
    log = MagicMock()
    _check_api_deprecations([cfg1, cfg2], log)
    
    # Should have one error (for deprecated) and no warnings (future is >60 days away)
    assert log.error.call_count == 1
    assert log.error.call_args[1]["connector"] == "deprecated"


def test_custom_warning_days():
    """Custom api_version_warning_days is respected."""
    soon = (_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=40)).date()
    cfg = _make_connector_config(
        api_version_deprecation_date=soon.isoformat(),
        api_version_warning_days=45,  # Should warn (40 days < 45 days threshold)
    )
    log = MagicMock()
    
    _check_api_deprecations([cfg], log)
    
    log.warning.assert_called_once()
