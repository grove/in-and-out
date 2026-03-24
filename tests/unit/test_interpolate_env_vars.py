"""Unit tests for _interpolate_env_vars in config/loader.py."""
from __future__ import annotations

import os

import pytest

from inandout.config.loader import _interpolate_env_vars


def test_no_vars_unchanged():
    text = "hello world"
    assert _interpolate_env_vars(text) == "hello world"


def test_replaces_single_var(monkeypatch):
    monkeypatch.setenv("MY_VAR", "hello")
    result = _interpolate_env_vars("value is ${MY_VAR}")
    assert result == "value is hello"


def test_replaces_multiple_vars(monkeypatch):
    monkeypatch.setenv("HOST", "localhost")
    monkeypatch.setenv("PORT", "5432")
    result = _interpolate_env_vars("${HOST}:${PORT}")
    assert result == "localhost:5432"


def test_replaces_same_var_twice(monkeypatch):
    monkeypatch.setenv("THING", "x")
    result = _interpolate_env_vars("${THING} and ${THING}")
    assert result == "x and x"


def test_raises_environment_error_for_missing_var(monkeypatch):
    monkeypatch.delenv("MISSING_VAR_XYZ", raising=False)
    with pytest.raises(EnvironmentError, match="MISSING_VAR_XYZ"):
        _interpolate_env_vars("value=${MISSING_VAR_XYZ}")


def test_error_message_contains_var_name(monkeypatch):
    monkeypatch.delenv("ABSENT_VAR", raising=False)
    with pytest.raises(EnvironmentError) as exc_info:
        _interpolate_env_vars("dsn=${ABSENT_VAR}")
    assert "ABSENT_VAR" in str(exc_info.value)


def test_non_uppercase_pattern_left_unchanged(monkeypatch):
    """Patterns like ${runtime.value} (not all-uppercase) should pass through."""
    monkeypatch.delenv("runtime.value", raising=False)
    result = _interpolate_env_vars("${runtime.value}")
    assert result == "${runtime.value}"


def test_lowercase_pattern_left_unchanged():
    result = _interpolate_env_vars("${lowercase_var}")
    assert result == "${lowercase_var}"


def test_mixed_case_pattern_left_unchanged():
    result = _interpolate_env_vars("${MyVar}")
    assert result == "${MyVar}"


def test_empty_string_unchanged():
    assert _interpolate_env_vars("") == ""


def test_var_value_containing_special_chars(monkeypatch):
    monkeypatch.setenv("DSN", "postgresql://user:pass@host/db")
    result = _interpolate_env_vars("dsn: ${DSN}")
    assert result == "dsn: postgresql://user:pass@host/db"


def test_multiple_missing_vars_reported(monkeypatch):
    monkeypatch.delenv("VAR_A", raising=False)
    monkeypatch.delenv("VAR_B", raising=False)
    with pytest.raises(EnvironmentError) as exc_info:
        _interpolate_env_vars("${VAR_A} and ${VAR_B}")
    msg = str(exc_info.value)
    assert "VAR_A" in msg
    assert "VAR_B" in msg


def test_digits_in_var_name_allowed(monkeypatch):
    monkeypatch.setenv("VAR123", "value123")
    result = _interpolate_env_vars("${VAR123}")
    assert result == "value123"


def test_var_name_with_leading_digit_not_matched():
    """Pattern requires first char to be uppercase letter [A-Z], not digit."""
    result = _interpolate_env_vars("${1VAR}")
    assert result == "${1VAR}"
