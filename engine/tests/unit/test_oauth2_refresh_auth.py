"""Unit tests for OAuth2 refresh_token auth provider."""
from __future__ import annotations

import time

import pytest
import respx
import httpx

from inandout.config.auth import OAuth2Auth, OAuth2Config
from inandout.transport.auth import OAuth2RefreshTokenAuth, build_auth_provider


TOKEN_URL = "https://auth.example.com/token"
REFRESH_URL = "https://auth.example.com/refresh"


def _make_config(
    *,
    refresh_url: str | None = None,
    scopes: list[str] | None = None,
) -> OAuth2Auth:
    return OAuth2Auth(
        type="oauth2",
        credential_ref="my_refresh",
        oauth2=OAuth2Config(
            grant_type="authorization_code",
            token_url=TOKEN_URL,
            refresh_url=refresh_url,
            scopes=scopes or [],
        ),
    )


@pytest.fixture(autouse=True)
def clear_cache():
    OAuth2RefreshTokenAuth._cache.clear()
    OAuth2RefreshTokenAuth._locks.clear()
    yield
    OAuth2RefreshTokenAuth._cache.clear()
    OAuth2RefreshTokenAuth._locks.clear()


def test_build_auth_provider_returns_refresh_token_auth(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:refresh_tok")
    config = _make_config()
    provider = build_auth_provider(config)
    assert isinstance(provider, OAuth2RefreshTokenAuth)


def test_parse_credential_three_parts(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "client:secret:rtoken")
    provider = OAuth2RefreshTokenAuth(_make_config())
    cid, csec, rt = provider._parse_credential()
    assert (cid, csec, rt) == ("client", "secret", "rtoken")


def test_parse_credential_two_parts(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "client:rtoken")
    provider = OAuth2RefreshTokenAuth(_make_config())
    cid, csec, rt = provider._parse_credential()
    assert (cid, csec, rt) == ("client", "", "rtoken")


def test_parse_credential_single_value(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "just_refresh_token")
    provider = OAuth2RefreshTokenAuth(_make_config())
    cid, csec, rt = provider._parse_credential()
    assert (cid, csec, rt) == ("", "", "just_refresh_token")


def test_token_fetched_and_cached(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    provider = OAuth2RefreshTokenAuth(_make_config())

    with respx.mock:
        respx.post(TOKEN_URL).mock(
            return_value=httpx.Response(
                200, json={"access_token": "new_access", "expires_in": 3600}
            )
        )
        token = provider._fetch_token_sync()

    assert token == "new_access"
    assert provider._cache_key in OAuth2RefreshTokenAuth._cache
    assert OAuth2RefreshTokenAuth._cache[provider._cache_key]["access_token"] == "new_access"


def test_uses_refresh_url_when_provided(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    config = _make_config(refresh_url=REFRESH_URL)
    provider = OAuth2RefreshTokenAuth(config)

    called_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        called_urls.append(str(request.url))
        return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})

    with respx.mock:
        respx.post(REFRESH_URL).mock(side_effect=handler)
        provider._fetch_token_sync()

    assert REFRESH_URL in called_urls[0]


def test_sends_refresh_token_grant_type(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:my_rt")
    provider = OAuth2RefreshTokenAuth(_make_config())

    captured_data: dict = {}

    def capture(request: httpx.Request) -> httpx.Response:
        body = request.content.decode()
        for part in body.split("&"):
            k, _, v = part.partition("=")
            captured_data[k] = v
        return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})

    with respx.mock:
        respx.post(TOKEN_URL).mock(side_effect=capture)
        provider._fetch_token_sync()

    assert captured_data["grant_type"] == "refresh_token"
    assert captured_data["refresh_token"] == "my_rt"
    assert captured_data["client_id"] == "cid"
    assert captured_data["client_secret"] == "csec"


def test_sends_scopes(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    config = _make_config(scopes=["read", "write"])
    provider = OAuth2RefreshTokenAuth(config)

    captured_data: dict = {}

    def capture(request: httpx.Request) -> httpx.Response:
        body = request.content.decode()
        for part in body.split("&"):
            k, _, v = part.partition("=")
            captured_data[k] = v
        return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})

    with respx.mock:
        respx.post(TOKEN_URL).mock(side_effect=capture)
        provider._fetch_token_sync()

    assert "read" in captured_data.get("scope", "")


def test_rotated_refresh_token_is_persisted(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:old_rt")
    provider = OAuth2RefreshTokenAuth(_make_config())

    with respx.mock:
        respx.post(TOKEN_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "at1",
                    "expires_in": 3600,
                    "refresh_token": "new_rt",
                },
            )
        )
        provider._fetch_token_sync()

    entry = OAuth2RefreshTokenAuth._cache[provider._cache_key]
    assert entry["refresh_token"] == "new_rt"


def test_cached_token_returned_without_refetch(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    provider = OAuth2RefreshTokenAuth(_make_config())

    OAuth2RefreshTokenAuth._cache[provider._cache_key] = {
        "access_token": "cached_tok",
        "expires_at": time.monotonic() + 3600,
    }

    token = provider._get_cached_token()
    assert token == "cached_tok"


def test_expired_cache_triggers_refetch(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    provider = OAuth2RefreshTokenAuth(_make_config())

    OAuth2RefreshTokenAuth._cache[provider._cache_key] = {
        "access_token": "old",
        "expires_at": time.monotonic() + 30,
    }

    with respx.mock:
        respx.post(TOKEN_URL).mock(
            return_value=httpx.Response(200, json={"access_token": "fresh", "expires_in": 3600})
        )
        token = provider._get_cached_token()

    assert token == "fresh"


def test_invalidate_cache(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    provider = OAuth2RefreshTokenAuth(_make_config())

    OAuth2RefreshTokenAuth._cache[provider._cache_key] = {
        "access_token": "some_tok",
        "expires_at": time.monotonic() + 3600,
    }

    provider._invalidate_cache()
    assert provider._cache_key not in OAuth2RefreshTokenAuth._cache


def test_error_returns_none(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_REFRESH", "cid:csec:rtok")
    provider = OAuth2RefreshTokenAuth(_make_config())

    with respx.mock:
        respx.post(TOKEN_URL).mock(
            return_value=httpx.Response(400, json={"error": "invalid_grant"})
        )
        token = provider._fetch_token_sync()

    assert token is None
