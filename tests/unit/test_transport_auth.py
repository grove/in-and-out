"""Unit tests for resolve_credential in transport/auth.py."""
from __future__ import annotations

import os

import pytest

from inandout.transport.auth import resolve_credential


def test_resolves_env_var(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_KEY", "secret-value")
    assert resolve_credential("MY_KEY") == "secret-value"


def test_missing_env_var_raises_environment_error(monkeypatch):
    monkeypatch.delenv("INOUT_CREDENTIAL_MISSING_KEY", raising=False)
    with pytest.raises(EnvironmentError, match="MISSING_KEY"):
        resolve_credential("MISSING_KEY")


def test_error_message_contains_env_var_name(monkeypatch):
    monkeypatch.delenv("INOUT_CREDENTIAL_MY_TOKEN", raising=False)
    with pytest.raises(EnvironmentError) as exc_info:
        resolve_credential("MY_TOKEN")
    assert "INOUT_CREDENTIAL_MY_TOKEN" in str(exc_info.value)


def test_lowercased_ref_upcased_for_lookup(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_KEY", "lowercase-ref-value")
    # credential_ref in lowercase maps to upper env var
    assert resolve_credential("my_key") == "lowercase-ref-value"


def test_hyphen_in_ref_replaced_with_underscore(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_KEY", "hyphen-value")
    assert resolve_credential("my-key") == "hyphen-value"


def test_mixed_case_and_hyphen(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_API_TOKEN", "mixed-value")
    assert resolve_credential("api-token") == "mixed-value"


def test_value_returned_as_string(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_X", "42")
    result = resolve_credential("X")
    assert isinstance(result, str)
    assert result == "42"


def test_empty_string_env_var_returned(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_EMPTY", "")
    # An empty string is a valid (if unusual) credential value
    assert resolve_credential("EMPTY") == ""
