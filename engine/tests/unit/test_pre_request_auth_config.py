"""Unit tests for PreRequestAuthConfig."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.auth import PreRequestAuthConfig


def test_minimal_valid():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
    )
    assert cfg.endpoint == "https://auth.example.com/session"
    assert cfg.credential_ref == "MY_CRED"


def test_method_default_post():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
    )
    assert cfg.method == "POST"


def test_token_field_default():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
    )
    assert cfg.token_field == "token"


def test_token_lifetime_secs_default():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
    )
    assert cfg.token_lifetime_secs == 3600.0


def test_request_body_default_empty():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
    )
    assert cfg.request_body == {}


def test_token_header_default():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
    )
    assert cfg.token_header == "X-Session-Token"


def test_custom_method():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
        method="GET",
    )
    assert cfg.method == "GET"


def test_custom_token_field():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
        token_field="access_token",
    )
    assert cfg.token_field == "access_token"


def test_custom_token_lifetime():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
        token_lifetime_secs=1800.0,
    )
    assert cfg.token_lifetime_secs == 1800.0


def test_request_body_set():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
        request_body={"grant_type": "session"},
    )
    assert cfg.request_body["grant_type"] == "session"


def test_custom_token_header():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
        token_header="X-Auth-Token",
    )
    assert cfg.token_header == "X-Auth-Token"


def test_missing_endpoint_raises():
    with pytest.raises(ValidationError):
        PreRequestAuthConfig(credential_ref="MY_CRED")


def test_missing_credential_ref_raises():
    with pytest.raises(ValidationError):
        PreRequestAuthConfig(endpoint="https://auth.example.com/session")


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        PreRequestAuthConfig(
            endpoint="https://auth.example.com/session",
            credential_ref="MY_CRED",
            unknown_field="bad",
        )


def test_round_trip_json():
    cfg = PreRequestAuthConfig(
        endpoint="https://auth.example.com/session",
        credential_ref="MY_CRED",
        token_field="access_token",
        token_lifetime_secs=900.0,
    )
    loaded = PreRequestAuthConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.token_field == "access_token"
    assert loaded.token_lifetime_secs == 900.0
