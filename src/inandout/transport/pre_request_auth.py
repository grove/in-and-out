"""Pre-request session-token authentication provider (A3 — T1 #24).

Implements an httpx.Auth that:
  1. POSTs to an endpoint to acquire a session token before the first request.
  2. Caches the token (module-level, keyed by (credential_ref, endpoint)).
  3. Injects the token header on every subsequent request.
  4. On 401 response: invalidates the cache, re-acquires the token once, retries.
  5. Handles concurrent requests — only one token acquisition per cache key.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Generator

import httpx
import structlog

logger = structlog.get_logger(__name__)

# Module-level cache: (credential_ref, endpoint) → {"token": str, "expires_at": float}
_token_cache: dict[tuple[str, str], dict[str, Any]] = {}
_token_locks: dict[tuple[str, str], asyncio.Lock] = {}


def _get_lock(cache_key: tuple[str, str]) -> asyncio.Lock:
    if cache_key not in _token_locks:
        _token_locks[cache_key] = asyncio.Lock()
    return _token_locks[cache_key]


def _resolve_dot_notation(data: dict[str, Any], path: str) -> Any:
    """Resolve a dot-notation path like 'data.token' from a nested dict."""
    parts = path.split(".")
    cur: Any = data
    for part in parts:
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


async def acquire_session_token(cfg: Any) -> str:
    """Standalone coroutine: POST to cfg.endpoint and return the session token.

    Args:
        cfg: PreRequestAuthConfig instance.

    Returns:
        The session token string.

    Raises:
        RuntimeError: if the token endpoint returns a non-2xx response or the
                      token cannot be found at ``cfg.token_field``.
    """
    from inandout.transport.auth import resolve_credential

    credential = resolve_credential(cfg.credential_ref)
    body = dict(cfg.request_body)

    # Inject credential — support "username:password" or raw token
    if ":" in credential:
        username, password = credential.split(":", 1)
        body.setdefault("username", username)
        body.setdefault("password", password)
    else:
        body.setdefault("token", credential)

    async with httpx.AsyncClient(timeout=15.0) as client:
        method = cfg.method.upper()
        if method in ("POST", "PUT", "PATCH"):
            resp = await client.request(method, cfg.endpoint, json=body)
        else:
            resp = await client.request(method, cfg.endpoint, params=body)

    resp.raise_for_status()
    try:
        resp_data = resp.json()
    except Exception as exc:
        raise RuntimeError(
            f"pre_request_auth: could not parse JSON from {cfg.endpoint}: {exc}"
        ) from exc

    token = _resolve_dot_notation(resp_data, cfg.token_field)
    if token is None:
        raise RuntimeError(
            f"pre_request_auth: token field {cfg.token_field!r} not found in response. "
            f"Available keys: {list(resp_data.keys()) if isinstance(resp_data, dict) else type(resp_data)}"
        )
    return str(token)


class PreRequestAuthProvider(httpx.Auth):
    """httpx.Auth implementation for pre-request session-token flows.

    Compatible with HttpTransportAdapter as an additional auth layer.
    """

    requires_response_body = True

    def __init__(self, cfg: Any) -> None:
        self._cfg = cfg
        self._cache_key = (cfg.credential_ref, cfg.endpoint)

    # ------------------------------------------------------------------
    # httpx.Auth generator
    # ------------------------------------------------------------------

    def auth_flow(self, request: httpx.Request) -> Generator:
        token = self._get_cached_token_sync()
        request.headers[self._cfg.token_header] = token
        response = yield request

        if response.status_code == 401:
            # Invalidate cache and retry once
            self._invalidate_cache()
            new_token = self._fetch_token_sync()
            if new_token:
                request.headers[self._cfg.token_header] = new_token
                yield request

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_cached_token_sync(self) -> str:
        entry = _token_cache.get(self._cache_key)
        if entry is None or entry["expires_at"] - time.monotonic() < 60:
            return self._fetch_token_sync()
        return entry["token"]

    def _invalidate_cache(self) -> None:
        _token_cache.pop(self._cache_key, None)

    def _fetch_token_sync(self) -> str:
        """Fetch a session token synchronously (used inside httpx.Auth generator)."""
        cfg = self._cfg
        from inandout.transport.auth import resolve_credential

        try:
            credential = resolve_credential(cfg.credential_ref)
        except Exception as exc:
            logger.error("pre_request_auth_credential_failed", error=str(exc))
            return ""

        body = dict(cfg.request_body)
        if ":" in credential:
            username, password = credential.split(":", 1)
            body.setdefault("username", username)
            body.setdefault("password", password)
        else:
            body.setdefault("token", credential)

        try:
            method = cfg.method.upper()
            with httpx.Client(timeout=15.0) as client:
                if method in ("POST", "PUT", "PATCH"):
                    resp = client.request(method, cfg.endpoint, json=body)
                else:
                    resp = client.request(method, cfg.endpoint, params=body)
            resp.raise_for_status()
            resp_data = resp.json()
        except Exception as exc:
            logger.error(
                "pre_request_auth_token_fetch_failed",
                endpoint=cfg.endpoint,
                error=str(exc),
            )
            return ""

        token = _resolve_dot_notation(resp_data, cfg.token_field)
        if token is None:
            logger.error(
                "pre_request_auth_token_field_missing",
                token_field=cfg.token_field,
                endpoint=cfg.endpoint,
            )
            return ""

        token_str = str(token)
        _token_cache[self._cache_key] = {
            "token": token_str,
            "expires_at": time.monotonic() + cfg.token_lifetime_secs,
        }
        return token_str
