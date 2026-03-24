"""Webhook-only HTTP server with IP allowlist and rate limiting middleware.

Provides a separate Starlette app for internet-facing webhook routes,
distinct from the internal health/metrics server.
"""
from __future__ import annotations

import ipaddress
import time
from collections import defaultdict
from typing import Any

import structlog
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from inandout.ingestion.webhooks import handle_webhook

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# IP allowlist middleware
# ---------------------------------------------------------------------------


def _parse_networks(allowlist: list[str]) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Parse a list of CIDR strings into network objects, discarding invalid entries."""
    nets: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for entry in allowlist:
        try:
            nets.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            logger.warning("ip_allowlist_invalid_entry", entry=entry)
    return nets


def _is_ip_allowed(
    client_ip: str,
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network],
) -> bool:
    """Return True when client_ip is covered by at least one network (or networks is empty)."""
    if not networks:
        return True
    try:
        addr = ipaddress.ip_address(client_ip)
        return any(addr in net for net in networks)
    except ValueError:
        return False


class IpAllowlistMiddleware(BaseHTTPMiddleware):
    """Reject requests from IPs not in the allowlist. Supports CIDR notation."""

    def __init__(self, app: Any, allowlist: list[str]) -> None:
        super().__init__(app)
        self._networks = _parse_networks(allowlist)

    def _is_allowed(self, client_ip: str) -> bool:
        return _is_ip_allowed(client_ip, self._networks)

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "")
        )
        if not self._is_allowed(client_ip):
            logger.warning("ip_allowlist_blocked", client_ip=client_ip)
            return JSONResponse(
                {"error": "forbidden", "reason": "IP not in allowlist"},
                status_code=403,
            )
        return await call_next(request)


# ---------------------------------------------------------------------------
# Rate limiting middleware (sliding window per IP)
# ---------------------------------------------------------------------------


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding window rate limiter: rate_limit_per_minute requests per IP per minute."""

    def __init__(self, app: Any, rate_limit_per_minute: int) -> None:
        super().__init__(app)
        self._limit = rate_limit_per_minute
        # ip → list of timestamps (float, seconds)
        self._windows: dict[str, list[float]] = defaultdict(list)

    def _check_rate(self, client_ip: str) -> tuple[bool, int]:
        """Return (allowed, retry_after_seconds)."""
        now = time.monotonic()
        window_start = now - 60.0
        timestamps = self._windows[client_ip]
        # Prune old entries
        timestamps[:] = [t for t in timestamps if t > window_start]
        if len(timestamps) >= self._limit:
            # Oldest entry determines when the window opens up
            retry_after = max(1, int(60 - (now - timestamps[0])) + 1)
            return False, retry_after
        timestamps.append(now)
        return True, 0

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "unknown")
        )
        allowed, retry_after = self._check_rate(client_ip)
        if not allowed:
            logger.warning("rate_limit_exceeded", client_ip=client_ip, retry_after=retry_after)
            return JSONResponse(
                {"error": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
        return await call_next(request)


# ---------------------------------------------------------------------------
# Webhook-only Starlette app builder
# ---------------------------------------------------------------------------


def build_webhook_app(
    engine: Any,
    connector_configs: list[Any],
    webhook_server_cfg: Any,
) -> Starlette:
    """Build a Starlette app serving ONLY webhook routes.

    Applies IP allowlist and rate limiting middleware based on
    webhook_server_cfg.
    """
    routes: list[Any] = []
    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        webhook_cfg = getattr(connector_cfg, "webhook", None)
        if webhook_cfg is None:
            continue

        def _make_handler(c_cfg: Any, w_cfg: Any) -> Any:
            # T1 #42: per-connector IP allowlist + rate limit (applied before server-wide middleware)
            _conn_networks = _parse_networks(getattr(w_cfg, "ip_allowlist", None) or [])
            _conn_rate_limit: int | None = getattr(w_cfg, "rate_limit_per_minute", None)
            _conn_rate_windows: dict[str, list[float]] = defaultdict(list)

            async def _webhook_handler(request: Request) -> Any:
                client_ip = (
                    request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                    or (request.client.host if request.client else "")
                )
                # Per-connector IP allowlist check
                if _conn_networks and not _is_ip_allowed(client_ip, _conn_networks):
                    logger.warning(
                        "connector_ip_allowlist_blocked",
                        connector=c_cfg.name, client_ip=client_ip,
                    )
                    return JSONResponse(
                        {"error": "forbidden", "reason": "IP not in allowlist"},
                        status_code=403,
                    )
                # Per-connector rate limit check
                if _conn_rate_limit is not None and _conn_rate_limit > 0:
                    now = time.monotonic()
                    window_start = now - 60.0
                    timestamps = _conn_rate_windows[client_ip]
                    timestamps[:] = [t for t in timestamps if t > window_start]
                    if len(timestamps) >= _conn_rate_limit:
                        retry_after = max(1, int(60 - (now - timestamps[0])) + 1)
                        logger.warning(
                            "connector_rate_limit_exceeded",
                            connector=c_cfg.name, client_ip=client_ip,
                        )
                        return JSONResponse(
                            {"error": "rate limit exceeded"},
                            status_code=429,
                            headers={"Retry-After": str(retry_after)},
                        )
                    timestamps.append(now)
                return await handle_webhook(request, c_cfg, w_cfg, engine)
            return _webhook_handler

        routes.append(
            Route(webhook_cfg.path, _make_handler(connector_cfg, webhook_cfg), methods=["POST"])
        )

    if not routes:
        # No webhook routes — add a placeholder so Starlette doesn't error
        async def _no_webhooks(request: Request) -> JSONResponse:
            return JSONResponse({"status": "no_webhooks_configured"}, status_code=404)
        routes.append(Route("/", _no_webhooks))

    app: Any = Starlette(routes=routes)

    # Apply rate limiting middleware
    rate_limit_per_minute = getattr(webhook_server_cfg, "rate_limit_per_minute", 300)
    if rate_limit_per_minute > 0:
        app = RateLimitMiddleware(app, rate_limit_per_minute)

    # Apply IP allowlist middleware (innermost so it runs first)
    ip_allowlist = getattr(webhook_server_cfg, "ip_allowlist", [])
    if ip_allowlist:
        app = IpAllowlistMiddleware(app, ip_allowlist)

    return app
