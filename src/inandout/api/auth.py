"""Bearer token authentication middleware for the management API."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

if TYPE_CHECKING:
    from inandout.config.tool import ApiAuthConfig

logger = structlog.get_logger(__name__)

# Paths that are always exempt from authentication
_EXEMPT_PATHS = frozenset({"/health", "/ready", "/metrics"})


def resolve_auth_token(config: "ApiAuthConfig") -> str | None:
    """Return the configured bearer token, reading from env var if configured."""
    if config.token_env_var:
        return os.environ.get(config.token_env_var)
    return config.token


class BearerTokenMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that enforces bearer token authentication.

    When ``api_auth.enabled`` is False, all requests pass through.
    When enabled, requests must include ``Authorization: Bearer <token>``.
    Paths in ``_EXEMPT_PATHS`` are never challenged.
    """

    def __init__(self, app: Any, api_auth: "ApiAuthConfig") -> None:  # type: ignore[override]
        super().__init__(app)
        self._enabled = api_auth.enabled
        self._realm = api_auth.realm
        self._token: str | None = resolve_auth_token(api_auth)

    async def dispatch(self, request: Request, call_next: Any) -> Response:  # type: ignore[override]
        # Always pass through if auth is disabled
        if not self._enabled:
            return await call_next(request)

        # Exempt paths never require auth
        path = request.url.path
        if path in _EXEMPT_PATHS or path.startswith("/metrics"):
            return await call_next(request)

        # Extract Authorization header
        authorization = request.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            logger.info("api_auth_missing_header", path=path)
            return Response(
                content="Unauthorized",
                status_code=401,
                headers={"WWW-Authenticate": f'Bearer realm="{self._realm}"'},
            )

        provided_token = authorization[len("Bearer "):]
        if provided_token != self._token:
            logger.info("api_auth_invalid_token", path=path)
            return Response(
                content="Unauthorized",
                status_code=401,
                headers={"WWW-Authenticate": f'Bearer realm="{self._realm}"'},
            )

        return await call_next(request)
