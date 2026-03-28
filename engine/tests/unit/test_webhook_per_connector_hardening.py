"""Unit tests for T1 #42 — per-connector IP allowlist and rate-limit in WebhookConfig."""
from __future__ import annotations

import time
from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Config field tests
# ---------------------------------------------------------------------------

def test_webhook_config_ip_allowlist_default():
    """ip_allowlist defaults to empty list."""
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm, UnmatchedAction
    cfg = WebhookConfig(
        path="/hook",
        signature=SignatureConfig(algorithm=SignatureAlgorithm.hmac_sha256, header="X-Sig", credential_ref="k"),
        fan_out=FanOutConfig(discriminator="type", routes=[], unmatched=UnmatchedAction.log_and_discard),
    )
    assert cfg.ip_allowlist == []


def test_webhook_config_rate_limit_default():
    """rate_limit_per_minute defaults to None."""
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm, UnmatchedAction
    cfg = WebhookConfig(
        path="/hook",
        signature=SignatureConfig(algorithm=SignatureAlgorithm.hmac_sha256, header="X-Sig", credential_ref="k"),
        fan_out=FanOutConfig(discriminator="type", routes=[], unmatched=UnmatchedAction.log_and_discard),
    )
    assert cfg.rate_limit_per_minute is None


def test_webhook_config_ip_allowlist_can_be_set():
    """ip_allowlist can hold CIDR strings."""
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm, UnmatchedAction
    cfg = WebhookConfig(
        path="/hook",
        signature=SignatureConfig(algorithm=SignatureAlgorithm.hmac_sha256, header="X-Sig", credential_ref="k"),
        fan_out=FanOutConfig(discriminator="type", routes=[], unmatched=UnmatchedAction.log_and_discard),
        ip_allowlist=["10.0.0.0/8", "192.168.1.50"],
    )
    assert "10.0.0.0/8" in cfg.ip_allowlist
    assert "192.168.1.50" in cfg.ip_allowlist


def test_webhook_config_rate_limit_can_be_set():
    """rate_limit_per_minute can be configured."""
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm, UnmatchedAction
    cfg = WebhookConfig(
        path="/hook",
        signature=SignatureConfig(algorithm=SignatureAlgorithm.hmac_sha256, header="X-Sig", credential_ref="k"),
        fan_out=FanOutConfig(discriminator="type", routes=[], unmatched=UnmatchedAction.log_and_discard),
        rate_limit_per_minute=60,
    )
    assert cfg.rate_limit_per_minute == 60


# ---------------------------------------------------------------------------
# Helper functions tests
# ---------------------------------------------------------------------------

def test_parse_networks_valid_cidr():
    """_parse_networks parses valid CIDR entries."""
    from inandout.ingestion.webhook_server import _parse_networks
    nets = _parse_networks(["192.168.0.0/24", "10.0.0.1"])
    assert len(nets) == 2


def test_parse_networks_ignores_invalid():
    """_parse_networks silently ignores invalid entries."""
    from inandout.ingestion.webhook_server import _parse_networks
    nets = _parse_networks(["not-an-ip", "192.168.0.0/24"])
    assert len(nets) == 1


def test_is_ip_allowed_empty_allowlist():
    """Empty allowlist means all IPs are allowed."""
    from inandout.ingestion.webhook_server import _is_ip_allowed
    assert _is_ip_allowed("1.2.3.4", []) is True


def test_is_ip_allowed_ip_in_range():
    """IP inside CIDR range is allowed."""
    from inandout.ingestion.webhook_server import _parse_networks, _is_ip_allowed
    nets = _parse_networks(["192.168.1.0/24"])
    assert _is_ip_allowed("192.168.1.100", nets) is True


def test_is_ip_allowed_ip_not_in_range():
    """IP outside all ranges is blocked."""
    from inandout.ingestion.webhook_server import _parse_networks, _is_ip_allowed
    nets = _parse_networks(["192.168.1.0/24"])
    assert _is_ip_allowed("10.0.0.1", nets) is False


# ---------------------------------------------------------------------------
# Per-connector handler behaviour
# ---------------------------------------------------------------------------

def _build_app_with_connector_webhook(ip_allowlist: list[str] = [], rate_limit: int | None = None):
    """Build a minimal Starlette app that simulates per-connector webhook routing."""
    from collections import defaultdict
    import time as _time

    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from inandout.ingestion.webhook_server import _parse_networks, _is_ip_allowed

    _conn_networks = _parse_networks(ip_allowlist)
    _conn_rate_limit = rate_limit
    _conn_rate_windows: dict[str, list[float]] = defaultdict(list)

    async def _handler(request: Request) -> JSONResponse:
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "")
        )
        if _conn_networks and not _is_ip_allowed(client_ip, _conn_networks):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        if _conn_rate_limit is not None and _conn_rate_limit > 0:
            now = _time.monotonic()
            window_start = now - 60.0
            timestamps = _conn_rate_windows[client_ip]
            timestamps[:] = [t for t in timestamps if t > window_start]
            if len(timestamps) >= _conn_rate_limit:
                retry_after = max(1, int(60 - (now - timestamps[0])) + 1)
                return JSONResponse(
                    {"error": "rate limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            timestamps.append(now)
        return JSONResponse({"status": "ok"})

    return Starlette(routes=[Route("/webhook", _handler, methods=["POST"])])


def test_per_connector_ip_allowlist_blocks():
    """IP not in connector allowlist gets 403."""
    app = _build_app_with_connector_webhook(ip_allowlist=["10.0.0.0/8"])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.post("/webhook", headers={"X-Forwarded-For": "192.168.1.1"})
    assert resp.status_code == 403


def test_per_connector_ip_allowlist_passes():
    """IP in connector allowlist gets through."""
    app = _build_app_with_connector_webhook(ip_allowlist=["192.168.1.0/24"])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.post("/webhook", headers={"X-Forwarded-For": "192.168.1.50"})
    assert resp.status_code == 200


def test_per_connector_rate_limit_exceeded():
    """Per-connector rate limit triggers 429 when exceeded."""
    app = _build_app_with_connector_webhook(rate_limit=2)
    client = TestClient(app, raise_server_exceptions=True)
    headers = {"X-Forwarded-For": "10.0.0.1"}
    resp1 = client.post("/webhook", headers=headers)
    resp2 = client.post("/webhook", headers=headers)
    resp3 = client.post("/webhook", headers=headers)
    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert resp3.status_code == 429
    assert "Retry-After" in resp3.headers


def test_per_connector_no_allowlist_passes_all():
    """When connector ip_allowlist is empty, all IPs pass (no connector-level check)."""
    app = _build_app_with_connector_webhook(ip_allowlist=[])
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.post("/webhook", headers={"X-Forwarded-For": "203.0.113.5"})
    assert resp.status_code == 200
