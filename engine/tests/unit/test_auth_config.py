"""Unit tests for auth config models: ApiKeyAuth, OAuth2Auth, JwtAuth."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.auth import (
    ApiKeyAuth,
    ApiKeyConfig,
    JwtAuth,
    JwtConfig,
    OAuth2Auth,
    OAuth2Config,
)


# --- ApiKeyConfig ---

def test_api_key_config_header_location():
    cfg = ApiKeyConfig(location="header", name="Authorization")
    assert cfg.location == "header"
    assert cfg.name == "Authorization"


def test_api_key_config_query_location():
    cfg = ApiKeyConfig(location="query", name="api_key")
    assert cfg.location == "query"


def test_api_key_config_invalid_location_raises():
    with pytest.raises(ValidationError):
        ApiKeyConfig(location="cookie", name="key")


def test_api_key_config_extra_forbidden():
    with pytest.raises(ValidationError):
        ApiKeyConfig(location="header", name="X-Key", extra_field="bad")


# --- ApiKeyAuth ---

def test_api_key_auth_valid():
    cfg = ApiKeyAuth(
        type="api_key",
        credential_ref="MY_KEY",
        api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
    )
    assert cfg.type == "api_key"
    assert cfg.credential_ref == "MY_KEY"


def test_api_key_auth_missing_credential_ref_raises():
    with pytest.raises(ValidationError):
        ApiKeyAuth(
            type="api_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        )


def test_api_key_auth_invalid_credential_ref_pattern_raises():
    with pytest.raises(ValidationError):
        ApiKeyAuth(
            type="api_key",
            credential_ref="1-invalid_start",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        )


# --- OAuth2Config and OAuth2Auth ---

def test_oauth2_config_valid():
    cfg = OAuth2Config(
        grant_type="client_credentials",
        token_url="https://auth.example.com/token",
    )
    assert cfg.grant_type == "client_credentials"
    assert cfg.scopes == []


def test_oauth2_config_scopes():
    cfg = OAuth2Config(
        grant_type="client_credentials",
        token_url="https://auth.example.com/token",
        scopes=["read", "write"],
    )
    assert "read" in cfg.scopes


def test_oauth2_auth_valid():
    cfg = OAuth2Auth(
        type="oauth2",
        credential_ref="MY_OAUTH",
        oauth2=OAuth2Config(
            grant_type="client_credentials",
            token_url="https://auth.example.com/token",
        ),
    )
    assert cfg.type == "oauth2"
    assert cfg.credential_ref == "MY_OAUTH"


# --- JwtConfig and JwtAuth ---

def test_jwt_config_valid():
    cfg = JwtConfig(algorithm="HS256", issuer="my-app", audience="api")
    assert cfg.algorithm == "HS256"
    assert cfg.expiry is None


def test_jwt_config_expiry_set():
    cfg = JwtConfig(algorithm="HS256", issuer="my-app", audience="api", expiry=3600)
    assert cfg.expiry == 3600


def test_jwt_config_expiry_must_be_positive():
    with pytest.raises(ValidationError):
        JwtConfig(algorithm="HS256", issuer="my-app", audience="api", expiry=0)


def test_jwt_auth_valid():
    cfg = JwtAuth(
        type="jwt",
        credential_ref="MY_JWT_KEY",
        jwt=JwtConfig(algorithm="RS256", issuer="issuer", audience="aud"),
    )
    assert cfg.type == "jwt"
    assert cfg.credential_ref == "MY_JWT_KEY"
