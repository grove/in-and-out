"""Unit tests for OAuth2 client_credentials auth provider."""
from __future__ import annotations

import time

import pytest
import respx
import httpx

from inandout.config.auth import OAuth2Auth, OAuth2Config
from inandout.transport.auth import OAuth2ClientCredentialsAuth, build_auth_provider


def _make_oauth2_config(token_url: str = "https://auth.example.com/token") -> OAuth2Auth:
    return OAuth2Auth(
        type="oauth2",
        credential_ref="my_oauth2",
        oauth2=OAuth2Config(
            grant_type="client_credentials",
            token_url=token_url,
            scopes=["read", "write"],
        ),
    )


@pytest.fixture(autouse=True)
def clear_oauth2_cache():
    """Clear the module-level token cache between tests."""
    OAuth2ClientCredentialsAuth._cache.clear()
    OAuth2ClientCredentialsAuth._locks.clear()
    yield
    OAuth2ClientCredentialsAuth._cache.clear()
    OAuth2ClientCredentialsAuth._locks.clear()


def test_build_auth_provider_returns_oauth2(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "client_id:client_secret")
    config = _make_oauth2_config()
    provider = build_auth_provider(config)
    assert isinstance(provider, OAuth2ClientCredentialsAuth)


def test_token_fetched_and_cached(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "cid:csecret")
    config = _make_oauth2_config()
    provider = OAuth2ClientCredentialsAuth(config)

    with respx.mock:
        respx.post("https://auth.example.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "tok123", "expires_in": 3600})
        )
        token = provider._fetch_token_sync()

    assert token == "tok123"
    assert provider._cache_key in OAuth2ClientCredentialsAuth._cache
    assert OAuth2ClientCredentialsAuth._cache[provider._cache_key]["access_token"] == "tok123"


def test_cached_token_returned_without_refetch(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "cid:csecret")
    config = _make_oauth2_config()
    provider = OAuth2ClientCredentialsAuth(config)

    # Pre-populate cache with a fresh token
    OAuth2ClientCredentialsAuth._cache[provider._cache_key] = {
        "access_token": "cached_token",
        "expires_at": time.monotonic() + 3600,
    }

    token = provider._get_cached_token()
    assert token == "cached_token"


def test_expired_token_triggers_refetch(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "cid:csecret")
    config = _make_oauth2_config()
    provider = OAuth2ClientCredentialsAuth(config)

    # Cache an expired token (30 seconds left — below 60 s threshold)
    OAuth2ClientCredentialsAuth._cache[provider._cache_key] = {
        "access_token": "old_token",
        "expires_at": time.monotonic() + 30,
    }

    with respx.mock:
        respx.post("https://auth.example.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "new_token", "expires_in": 3600})
        )
        token = provider._get_cached_token()

    assert token == "new_token"


def test_invalidate_cache(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "cid:csecret")
    config = _make_oauth2_config()
    provider = OAuth2ClientCredentialsAuth(config)

    OAuth2ClientCredentialsAuth._cache[provider._cache_key] = {
        "access_token": "some_token",
        "expires_at": time.monotonic() + 3600,
    }

    provider._invalidate_cache()
    assert provider._cache_key not in OAuth2ClientCredentialsAuth._cache


def test_fetch_token_sends_scope(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "cid:csecret")
    config = _make_oauth2_config()
    provider = OAuth2ClientCredentialsAuth(config)

    captured_data: dict = {}

    def capture(request: httpx.Request) -> httpx.Response:
        # httpx encodes form data as bytes; decode it
        body = request.content.decode()
        for part in body.split("&"):
            k, _, v = part.partition("=")
            captured_data[k] = v
        return httpx.Response(200, json={"access_token": "scoped_tok", "expires_in": 3600})

    with respx.mock:
        respx.post("https://auth.example.com/token").mock(side_effect=capture)
        provider._fetch_token_sync()

    assert captured_data.get("grant_type") == "client_credentials"
    assert "read" in captured_data.get("scope", "")
    assert "write" in captured_data.get("scope", "")


def test_fetch_token_returns_none_on_error(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH2", "cid:csecret")
    config = _make_oauth2_config()
    provider = OAuth2ClientCredentialsAuth(config)

    with respx.mock:
        respx.post("https://auth.example.com/token").mock(
            return_value=httpx.Response(401, json={"error": "invalid_client"})
        )
        token = provider._fetch_token_sync()

    assert token is None
