"""Unit tests for _is_allowed_interpolation in config/connector.py."""
from __future__ import annotations

import pytest

from inandout.config.connector import _is_allowed_interpolation


# --- Environment variable pattern (all uppercase, digits, underscore) ---

def test_env_var_uppercase_allowed():
    assert _is_allowed_interpolation("MY_ENV_VAR") is True


def test_env_var_with_numbers_allowed():
    assert _is_allowed_interpolation("API_KEY_2") is True


def test_env_var_single_letter_allowed():
    assert _is_allowed_interpolation("X") is True


def test_mixed_case_not_env_var():
    # Lowercase letter makes it NOT match the env var pattern
    assert _is_allowed_interpolation("myVar") is False


def test_starts_with_digit_not_env_var():
    assert _is_allowed_interpolation("1BAD") is False


# --- Allowed prefixes ---

def test_runtime_prefix_allowed():
    assert _is_allowed_interpolation("runtime.some_param") is True


def test_credential_prefix_allowed():
    assert _is_allowed_interpolation("credential.my_cred") is True


def test_auth_prefix_allowed():
    assert _is_allowed_interpolation("auth.token") is True


def test_record_prefix_allowed():
    assert _is_allowed_interpolation("record.field_name") is True


def test_data_prefix_allowed():
    assert _is_allowed_interpolation("data.id") is True


def test_pre_flight_prefix_allowed():
    assert _is_allowed_interpolation("pre_flight.etag") is True


def test_subscription_prefix_allowed():
    assert _is_allowed_interpolation("subscription.id") is True


# --- Exact allowed tokens ---

def test_connection_base_url_allowed():
    assert _is_allowed_interpolation("connection.base_url") is True


def test_watermark_allowed():
    assert _is_allowed_interpolation("watermark") is True


def test_external_id_allowed():
    assert _is_allowed_interpolation("external_id") is True


def test_cluster_id_allowed():
    assert _is_allowed_interpolation("cluster_id") is True


def test_job_id_allowed():
    assert _is_allowed_interpolation("job.id") is True


def test_child_id_allowed():
    assert _is_allowed_interpolation("child.id") is True


# --- Unknown namespaces ---

def test_unknown_namespace_rejected():
    assert _is_allowed_interpolation("unknown.namespace") is False


def test_internal_namespace_rejected():
    assert _is_allowed_interpolation("internal.secret") is False


def test_plain_lowercase_word_rejected():
    assert _is_allowed_interpolation("myvar") is False


def test_dot_only_rejected():
    assert _is_allowed_interpolation(".") is False
