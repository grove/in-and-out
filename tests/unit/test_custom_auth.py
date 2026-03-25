"""Unit tests for CustomAuth provider."""
from __future__ import annotations

import time

import pytest
import respx
import httpx

from inandout.config.auth import CustomAuth, CustomConfig
from inandout.transport.auth import CustomAuthProvider, build_auth_provider


STEP_URL = "https://api.example.com/auth/login"


def _make_config(
    *,
    steps: list[dict] | None = None,
    inject: dict | None = None,
    refresh: dict | None = None,
) -> CustomAuth:
    return CustomAuth(
        type="custom",
        credential_ref="my_custom",
        custom=CustomConfig(
            steps=steps or [{"url": STEP_URL, "method": "POST", "token_field": "data.token"}],
            inject=inject or {"header": "X-Session-Token"},
            refresh=refresh,
        ),
    )


@pytest.fixture(autouse=True)
def clear_cache():
    CustomAuthProvider._cache.clear()
    CustomAuthProvider._locks.clear()
    yield
    CustomAuthProvider._cache.clear()
    CustomAuthProvider._locks.clear()


def test_build_auth_provider_returns_custom_auth(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    config = _make_config()
    provider = build_auth_provider(config)
    assert isinstance(provider, CustomAuthProvider)


def test_single_step_fetches_token(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    provider = CustomAuthProvider(_make_config())

    with respx.mock:
        respx.post(STEP_URL).mock(
            return_value=httpx.Response(200, json={"data": {"token": "sess123"}})
        )
        token = provider._execute_steps_sync()

    assert token == "sess123"


def test_token_is_cached(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    provider = CustomAuthProvider(_make_config())

    with respx.mock:
        respx.post(STEP_URL).mock(
            return_value=httpx.Response(200, json={"data": {"token": "sess123"}})
        )
        provider._execute_steps_sync()

    assert provider._cache_key in CustomAuthProvider._cache
    assert CustomAuthProvider._cache[provider._cache_key]["token"] == "sess123"


def test_cached_token_returned_without_refetch(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    provider = CustomAuthProvider(_make_config())

    CustomAuthProvider._cache[provider._cache_key] = {
        "token": "cached_tok",
        "expires_at": time.monotonic() + 3600,
    }

    token = provider._get_cached_token()
    assert token == "cached_tok"


def test_expired_cache_triggers_refetch(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    provider = CustomAuthProvider(_make_config())

    CustomAuthProvider._cache[provider._cache_key] = {
        "token": "old_tok",
        "expires_at": time.monotonic() + 30,
    }

    with respx.mock:
        respx.post(STEP_URL).mock(
            return_value=httpx.Response(200, json={"data": {"token": "fresh_tok"}})
        )
        token = provider._get_cached_token()

    assert token == "fresh_tok"


def test_invalidate_cache(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    provider = CustomAuthProvider(_make_config())

    CustomAuthProvider._cache[provider._cache_key] = {
        "token": "t",
        "expires_at": time.monotonic() + 3600,
    }

    provider._invalidate_cache()
    assert provider._cache_key not in CustomAuthProvider._cache


def test_credential_placeholder_substitution(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "sekrit_api_key")
    config = _make_config(
        steps=[{
            "url": STEP_URL,
            "method": "POST",
            "body": {"api_key": "${credential}"},
            "token_field": "token",
        }],
    )
    provider = CustomAuthProvider(config)

    captured_body: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        captured_body.update(json.loads(request.content))
        return httpx.Response(200, json={"token": "tok123"})

    with respx.mock:
        respx.post(STEP_URL).mock(side_effect=handler)
        provider._execute_steps_sync()

    assert captured_body["api_key"] == "sekrit_api_key"


def test_multi_step_execution(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    step1_url = "https://api.example.com/auth/step1"
    step2_url = "https://api.example.com/auth/step2"
    config = _make_config(
        steps=[
            {"url": step1_url, "method": "POST", "token_field": "session_id"},
            {"url": step2_url, "method": "POST", "token_field": "access_token"},
        ],
    )
    provider = CustomAuthProvider(config)

    with respx.mock:
        respx.post(step1_url).mock(
            return_value=httpx.Response(200, json={"session_id": "sid1"})
        )
        respx.post(step2_url).mock(
            return_value=httpx.Response(200, json={"access_token": "final_tok"})
        )
        token = provider._execute_steps_sync()

    assert token == "final_tok"


def test_inject_header(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    config = _make_config(inject={"header": "Authorization"})
    provider = CustomAuthProvider(config)

    request = httpx.Request("GET", "https://api.example.com/data")
    provider._inject_token(request, "mytoken")
    assert request.headers["Authorization"] == "mytoken"


def test_inject_query(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    config = _make_config(inject={"query": "api_token"})
    provider = CustomAuthProvider(config)

    request = httpx.Request("GET", "https://api.example.com/data")
    provider._inject_token(request, "mytoken")
    assert "api_token=mytoken" in str(request.url)


def test_token_lifetime_from_refresh_config(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    config = _make_config(refresh={"token_lifetime_secs": 600})
    provider = CustomAuthProvider(config)

    with respx.mock:
        respx.post(STEP_URL).mock(
            return_value=httpx.Response(200, json={"data": {"token": "tok"}})
        )
        provider._execute_steps_sync()

    entry = CustomAuthProvider._cache[provider._cache_key]
    # Should expire in ~600s, not the default 3600
    remaining = entry["expires_at"] - time.monotonic()
    assert 580 < remaining < 620


def test_step_failure_returns_empty_string(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    provider = CustomAuthProvider(_make_config())

    with respx.mock:
        respx.post(STEP_URL).mock(
            return_value=httpx.Response(500, json={"error": "internal"})
        )
        token = provider._execute_steps_sync()

    assert token == ""


def test_missing_token_field_returns_empty(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_CUSTOM", "user:pass")
    config = _make_config(
        steps=[{"url": STEP_URL, "method": "POST", "token_field": "nonexistent.path"}],
    )
    provider = CustomAuthProvider(config)

    with respx.mock:
        respx.post(STEP_URL).mock(
            return_value=httpx.Response(200, json={"other": "value"})
        )
        token = provider._execute_steps_sync()

    assert token == ""


def test_resolve_path_nested():
    assert CustomAuthProvider._resolve_path(
        {"a": {"b": {"c": "deep"}}}, "a.b.c"
    ) == "deep"


def test_resolve_path_missing():
    assert CustomAuthProvider._resolve_path({"a": 1}, "x.y") == ""
