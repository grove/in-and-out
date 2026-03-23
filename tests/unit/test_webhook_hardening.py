"""Unit tests for webhook HTTP server hardening (T1 #42)."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Helper to build a test app with hardening middleware
# ---------------------------------------------------------------------------

def _make_test_app(ip_allowlist: list[str] = [], rate_limit_per_minute: int = 300):
    """Build a minimal Starlette app with hardening middleware for testing."""
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from inandout.ingestion.webhook_server import IpAllowlistMiddleware, RateLimitMiddleware

    async def _ok(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    app = Starlette(routes=[Route("/test", _ok, methods=["GET", "POST"])])
    app = RateLimitMiddleware(app, rate_limit_per_minute)
    if ip_allowlist:
        app = IpAllowlistMiddleware(app, ip_allowlist)
    return app


# ---------------------------------------------------------------------------
# IP allowlist tests
# ---------------------------------------------------------------------------

def test_allowed_ip_passes():
    """An IP in the allowlist should pass through."""
    app = _make_test_app(ip_allowlist=["192.168.1.100"])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/test", headers={"X-Forwarded-For": "192.168.1.100"})
    assert resp.status_code == 200


def test_blocked_ip_gets_403():
    """An IP not in the allowlist should get 403."""
    app = _make_test_app(ip_allowlist=["192.168.1.100"])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/test", headers={"X-Forwarded-For": "10.0.0.1"})
    assert resp.status_code == 403


def test_empty_allowlist_allows_all():
    """Empty allowlist means all IPs are allowed."""
    app = _make_test_app(ip_allowlist=[])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/test", headers={"X-Forwarded-For": "1.2.3.4"})
    assert resp.status_code == 200


def test_cidr_notation_allows_ip_in_range():
    """CIDR allowlist: IP in the range should pass."""
    app = _make_test_app(ip_allowlist=["192.168.0.0/24"])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/test", headers={"X-Forwarded-For": "192.168.0.55"})
    assert resp.status_code == 200


def test_cidr_notation_blocks_ip_out_of_range():
    """CIDR allowlist: IP outside the range should get 403."""
    app = _make_test_app(ip_allowlist=["192.168.0.0/24"])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/test", headers={"X-Forwarded-For": "192.168.1.1"})
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Rate limiting tests
# ---------------------------------------------------------------------------

def test_rate_limit_not_exceeded_allows_request():
    """First request under rate limit should pass through."""
    app = _make_test_app(rate_limit_per_minute=10)
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/test", headers={"X-Forwarded-For": "10.0.0.1"})
    assert resp.status_code == 200


def test_rate_limit_exceeded_returns_429():
    """After exceeding rate limit, 429 is returned."""
    app = _make_test_app(rate_limit_per_minute=3)
    client = TestClient(app, raise_server_exceptions=True)

    # Make 3 successful requests
    for _ in range(3):
        resp = client.get("/test", headers={"X-Forwarded-For": "10.0.0.99"})
        assert resp.status_code == 200

    # 4th request should be rate limited
    resp = client.get("/test", headers={"X-Forwarded-For": "10.0.0.99"})
    assert resp.status_code == 429


def test_rate_limit_exceeded_has_retry_after_header():
    """429 response should include Retry-After header."""
    app = _make_test_app(rate_limit_per_minute=1)
    client = TestClient(app, raise_server_exceptions=True)

    # First request passes
    client.get("/test", headers={"X-Forwarded-For": "10.0.0.77"})
    # Second request rate-limited
    resp = client.get("/test", headers={"X-Forwarded-For": "10.0.0.77"})
    assert resp.status_code == 429
    assert "Retry-After" in resp.headers


def test_rate_limit_different_ips_tracked_separately():
    """Rate limit is per IP — different IPs have independent counters."""
    app = _make_test_app(rate_limit_per_minute=1)
    client = TestClient(app, raise_server_exceptions=True)

    # IP A gets 2 requests — second is rate limited
    client.get("/test", headers={"X-Forwarded-For": "10.0.0.1"})
    resp_a2 = client.get("/test", headers={"X-Forwarded-For": "10.0.0.1"})
    assert resp_a2.status_code == 429

    # IP B has a fresh counter — first request passes
    resp_b = client.get("/test", headers={"X-Forwarded-For": "10.0.0.2"})
    assert resp_b.status_code == 200


# ---------------------------------------------------------------------------
# IpAllowlistMiddleware unit tests
# ---------------------------------------------------------------------------

def test_ip_allowlist_middleware_is_allowed_with_cidr():
    """IpAllowlistMiddleware._is_allowed should work with CIDR."""
    from inandout.ingestion.webhook_server import IpAllowlistMiddleware
    from starlette.applications import Starlette

    mw = IpAllowlistMiddleware(Starlette(routes=[]), ["192.168.0.0/24"])
    assert mw._is_allowed("192.168.0.1") is True
    assert mw._is_allowed("192.168.1.1") is False


def test_ip_allowlist_middleware_empty_allows_all():
    """IpAllowlistMiddleware with empty list allows all IPs."""
    from inandout.ingestion.webhook_server import IpAllowlistMiddleware
    from starlette.applications import Starlette

    mw = IpAllowlistMiddleware(Starlette(routes=[]), [])
    assert mw._is_allowed("1.2.3.4") is True
    assert mw._is_allowed("255.255.255.255") is True


# ---------------------------------------------------------------------------
# WebhookServerConfig model tests
# ---------------------------------------------------------------------------

def test_webhook_server_config_has_hardening_fields():
    """WebhookServerConfig should have rate_limit_per_minute and ip_allowlist."""
    from inandout.config.tool import WebhookServerConfig

    cfg = WebhookServerConfig()
    assert cfg.rate_limit_per_minute == 300
    assert cfg.ip_allowlist == []
    assert cfg.tls_cert_file is None
    assert cfg.tls_key_file is None


def test_webhook_server_config_configurable():
    """WebhookServerConfig fields can be set."""
    from inandout.config.tool import WebhookServerConfig

    cfg = WebhookServerConfig(
        rate_limit_per_minute=100,
        ip_allowlist=["10.0.0.0/8"],
        tls_cert_file="/etc/certs/server.crt",
        tls_key_file="/etc/certs/server.key",
    )
    assert cfg.rate_limit_per_minute == 100
    assert cfg.ip_allowlist == ["10.0.0.0/8"]
    assert cfg.tls_cert_file == "/etc/certs/server.crt"
