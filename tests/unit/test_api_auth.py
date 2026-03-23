"""Unit tests for API bearer token authentication middleware."""
from __future__ import annotations

import os

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from inandout.api.auth import BearerTokenMiddleware, resolve_auth_token
from inandout.config.tool import ApiAuthConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _ok_endpoint(request: Request) -> PlainTextResponse:
    return PlainTextResponse("ok")


async def _health_endpoint(request: Request) -> PlainTextResponse:
    return PlainTextResponse("healthy")


def _build_test_app(api_auth: ApiAuthConfig) -> TestClient:
    """Build a minimal Starlette app wrapped in BearerTokenMiddleware."""
    routes = [
        Route("/some-endpoint", _ok_endpoint),
        Route("/health", _health_endpoint),
        Route("/ready", _health_endpoint),
        Route("/metrics", _health_endpoint),
    ]
    app = Starlette(routes=routes)
    wrapped = BearerTokenMiddleware(app, api_auth=api_auth)
    return TestClient(wrapped, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_correct_token_passes():
    """Request with correct bearer token should get 200."""
    auth = ApiAuthConfig(enabled=True, token="secret-token")
    client = _build_test_app(auth)
    resp = client.get("/some-endpoint", headers={"Authorization": "Bearer secret-token"})
    assert resp.status_code == 200


def test_wrong_token_returns_401():
    """Request with wrong bearer token should get 401."""
    auth = ApiAuthConfig(enabled=True, token="secret-token")
    client = _build_test_app(auth)
    resp = client.get("/some-endpoint", headers={"Authorization": "Bearer wrong-token"})
    assert resp.status_code == 401
    assert "WWW-Authenticate" in resp.headers


def test_missing_authorization_header_returns_401():
    """Request without Authorization header should get 401 when auth is enabled."""
    auth = ApiAuthConfig(enabled=True, token="secret-token")
    client = _build_test_app(auth)
    resp = client.get("/some-endpoint")
    assert resp.status_code == 401
    assert "WWW-Authenticate" in resp.headers


def test_auth_disabled_passes_all():
    """When auth is disabled, all requests should pass through without auth check."""
    auth = ApiAuthConfig(enabled=False)
    client = _build_test_app(auth)
    resp = client.get("/some-endpoint")
    assert resp.status_code == 200


def test_auth_disabled_no_token_passes():
    """When auth is disabled, even requests without tokens pass."""
    auth = ApiAuthConfig(enabled=False)
    client = _build_test_app(auth)
    resp = client.get("/some-endpoint", headers={"Authorization": "Bearer garbage"})
    assert resp.status_code == 200


def test_health_endpoint_exempt_when_auth_enabled():
    """/health endpoint should be accessible even when auth is enabled."""
    auth = ApiAuthConfig(enabled=True, token="secret-token")
    client = _build_test_app(auth)
    resp = client.get("/health")
    assert resp.status_code == 200


def test_ready_endpoint_exempt_when_auth_enabled():
    """/ready endpoint should be accessible even when auth is enabled."""
    auth = ApiAuthConfig(enabled=True, token="secret-token")
    client = _build_test_app(auth)
    resp = client.get("/ready")
    assert resp.status_code == 200


def test_metrics_endpoint_exempt_when_auth_enabled():
    """/metrics endpoint should be accessible even when auth is enabled."""
    auth = ApiAuthConfig(enabled=True, token="secret-token")
    client = _build_test_app(auth)
    resp = client.get("/metrics")
    assert resp.status_code == 200


def test_www_authenticate_header_includes_realm():
    """401 response should include realm in WWW-Authenticate header."""
    auth = ApiAuthConfig(enabled=True, token="secret-token", realm="my-realm")
    client = _build_test_app(auth)
    resp = client.get("/some-endpoint")
    assert resp.status_code == 401
    assert 'realm="my-realm"' in resp.headers["WWW-Authenticate"]


# ---------------------------------------------------------------------------
# resolve_auth_token tests
# ---------------------------------------------------------------------------

def test_resolve_auth_token_from_token_field():
    """resolve_auth_token returns token value directly when set."""
    auth = ApiAuthConfig(enabled=True, token="direct-token")
    assert resolve_auth_token(auth) == "direct-token"


def test_resolve_auth_token_from_env_var(monkeypatch: pytest.MonkeyPatch):
    """resolve_auth_token reads from env var when token_env_var is set."""
    monkeypatch.setenv("MY_API_TOKEN", "env-token-value")
    auth = ApiAuthConfig(enabled=True, token_env_var="MY_API_TOKEN")
    assert resolve_auth_token(auth) == "env-token-value"


def test_resolve_auth_token_env_var_missing(monkeypatch: pytest.MonkeyPatch):
    """resolve_auth_token returns None when env var is not set."""
    monkeypatch.delenv("MISSING_TOKEN_VAR", raising=False)
    auth = ApiAuthConfig(enabled=True, token_env_var="MISSING_TOKEN_VAR")
    assert resolve_auth_token(auth) is None


def test_resolve_auth_token_none_when_nothing_set():
    """resolve_auth_token returns None when no token or env var is configured."""
    auth = ApiAuthConfig(enabled=False)
    assert resolve_auth_token(auth) is None


def test_token_env_var_takes_precedence_over_token(monkeypatch: pytest.MonkeyPatch):
    """When token_env_var is set, it is preferred over direct token."""
    monkeypatch.setenv("TOKEN_FROM_ENV", "env-wins")
    auth = ApiAuthConfig(enabled=True, token="direct", token_env_var="TOKEN_FROM_ENV")
    # token_env_var is checked first
    assert resolve_auth_token(auth) == "env-wins"


def test_api_auth_config_defaults():
    """ApiAuthConfig should have sensible defaults."""
    auth = ApiAuthConfig()
    assert auth.enabled is False
    assert auth.token is None
    assert auth.token_env_var is None
    assert auth.realm == "in-and-out"
