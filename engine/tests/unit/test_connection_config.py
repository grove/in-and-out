"""Unit tests for ConnectionConfig, TimeoutConfig, and RateLimitConfig."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import (
    ConnectionConfig,
    RateLimitConfig,
    TimeoutConfig,
)


# --- TimeoutConfig ---

def test_timeout_config_all_none_by_default():
    cfg = TimeoutConfig()
    assert cfg.connect is None
    assert cfg.read is None
    assert cfg.write is None


def test_timeout_config_all_set():
    cfg = TimeoutConfig(connect="5s", read="30s", write="30s")
    assert cfg.connect == "5s"
    assert cfg.read == "30s"
    assert cfg.write == "30s"


def test_timeout_config_extra_field_forbidden():
    with pytest.raises(ValidationError):
        TimeoutConfig(connect="5s", unknown="bad")


def test_timeout_config_partial():
    cfg = TimeoutConfig(read="60s")
    assert cfg.read == "60s"
    assert cfg.connect is None


# --- ConnectionConfig ---

def test_connection_config_minimal():
    cfg = ConnectionConfig(base_url="https://api.example.com")
    assert cfg.base_url == "https://api.example.com"


def test_connection_config_staging_url_default_none():
    cfg = ConnectionConfig(base_url="https://api.example.com")
    assert cfg.staging_base_url is None


def test_connection_config_timeout_default_none():
    cfg = ConnectionConfig(base_url="https://api.example.com")
    assert cfg.timeout is None


def test_connection_config_retry_budget_default_none():
    cfg = ConnectionConfig(base_url="https://api.example.com")
    assert cfg.retry_budget is None


def test_connection_config_pre_request_default_none():
    cfg = ConnectionConfig(base_url="https://api.example.com")
    assert cfg.pre_request is None


def test_connection_config_staging_url_set():
    cfg = ConnectionConfig(
        base_url="https://api.example.com",
        staging_base_url="https://sandbox.example.com",
    )
    assert cfg.staging_base_url == "https://sandbox.example.com"


def test_connection_config_with_timeout():
    cfg = ConnectionConfig(
        base_url="https://api.example.com",
        timeout=TimeoutConfig(connect="10s", read="60s"),
    )
    assert cfg.timeout.connect == "10s"


def test_connection_config_missing_base_url_raises():
    with pytest.raises(ValidationError):
        ConnectionConfig()


def test_connection_config_extra_field_forbidden():
    with pytest.raises(ValidationError):
        ConnectionConfig(base_url="https://api.example.com", unknown="bad")


# --- RateLimitConfig ---

def test_rate_limit_defaults_none():
    cfg = RateLimitConfig()
    assert cfg.requests_per_second is None
    assert cfg.burst is None


def test_rate_limit_requests_per_second_set():
    cfg = RateLimitConfig(requests_per_second=10.0)
    assert cfg.requests_per_second == 10.0


def test_rate_limit_burst_set():
    cfg = RateLimitConfig(requests_per_second=10.0, burst=20)
    assert cfg.burst == 20


def test_rate_limit_requests_per_second_must_be_positive():
    with pytest.raises(ValidationError):
        RateLimitConfig(requests_per_second=0)


def test_rate_limit_burst_must_be_at_least_one():
    with pytest.raises(ValidationError):
        RateLimitConfig(burst=0)


def test_rate_limit_extra_field_forbidden():
    with pytest.raises(ValidationError):
        RateLimitConfig(requests_per_second=5.0, unknown="bad")
