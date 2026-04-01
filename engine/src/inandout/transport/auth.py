"""httpx.Auth implementations for each auth scheme."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Generator
from urllib.parse import urlparse

import httpx
import structlog

from inandout.config.auth import AuthConfig, OAuth2Auth, ApiKeyAuth, JwtAuth, CustomAuth

logger = structlog.get_logger(__name__)


def _resolve_oauth_url(url: str, connector_name: str | None) -> str:
    """Resolve OAuth endpoint URL with connector-specific env overrides.

    Priority:
    1) INOUT_<CONNECTOR>_TOKEN_URL
    2) If INOUT_<CONNECTOR>_BASE_URL is set, reuse its origin/path and append
       the configured OAuth endpoint path.
    3) Connector-configured URL as-is.
    """
    if not connector_name:
        return url

    connector_key = connector_name.upper()
    explicit_token_url = os.environ.get(f"INOUT_{connector_key}_TOKEN_URL")
    if explicit_token_url:
        return explicit_token_url

    base_url = os.environ.get(f"INOUT_{connector_key}_BASE_URL")
    if not base_url:
        return url

    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        suffix = parsed.path or ""
        if parsed.query:
            suffix = f"{suffix}?{parsed.query}"
        return f"{base_url.rstrip('/')}{suffix}"

    return f"{base_url.rstrip('/')}/{url.lstrip('/')}"


def resolve_credential(credential_ref: str) -> str:
    # Try custom credential providers first
    try:
        from inandout.secrets.provider import resolve_via_providers

        provider_value = resolve_via_providers(credential_ref)
        if provider_value is not None:
            return provider_value
    except Exception:
        pass

    # Fall back to environment variable
    env_var = f"INOUT_CREDENTIAL_{credential_ref.upper().replace('-', '_')}"
    value = os.environ.get(env_var)
    if value is None:
        raise EnvironmentError(
            f"Credential not resolved: {credential_ref!r}. "
            f"Set env var {env_var} or register a CredentialProvider via "
            f"inandout.secrets.provider.register_provider()."
        )
    return value


class ApiKeyAuthProvider(httpx.Auth):
    def __init__(self, config: ApiKeyAuth) -> None:
        self._location = config.api_key.location
        self._name = config.api_key.name
        self._secret = resolve_credential(config.credential_ref)

    def auth_flow(self, request: httpx.Request) -> Generator:
        if self._location == "header":
            request.headers[self._name] = self._secret
        else:
            request.url = request.url.copy_add_param(self._name, self._secret)
        yield request


class BearerTokenAuth(httpx.Auth):
    """Simple bearer token — used internally after OAuth2 token refresh."""

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_flow(self, request: httpx.Request) -> Generator:
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


class OAuth2ClientCredentialsAuth(httpx.Auth):
    """OAuth2 client_credentials flow with in-process token caching and 401-triggered refresh.

    Token is cached per (credential_ref, token_url) key in a module-level dict.
    Proactive refresh happens when fewer than 60 seconds remain before expiry.
    On a 401 response the token is invalidated and the request is retried once.
    """

    # Module-level cache: (credential_ref, token_url) -> {"access_token", "expires_at"}
    _cache: dict[tuple[str, str], dict] = {}
    _locks: dict[tuple[str, str], asyncio.Lock] = {}

    def __init__(self, config: OAuth2Auth, connector_name: str | None = None) -> None:
        self._config = config
        self._connector_name = connector_name
        self._token_url = _resolve_oauth_url(config.oauth2.token_url, connector_name)
        self._cache_key = (config.credential_ref, self._token_url)
        if self._cache_key not in self.__class__._locks:
            self.__class__._locks[self._cache_key] = asyncio.Lock()

    def _is_simulator_static_token_mode(self) -> bool:
        if not self._connector_name:
            return False
        connector_key = self._connector_name.upper()
        # Explicit token endpoint always wins.
        if os.environ.get(f"INOUT_{connector_key}_TOKEN_URL"):
            return False
        # If base URL is overridden (typically to simulator), skip OAuth exchange.
        return bool(os.environ.get(f"INOUT_{connector_key}_BASE_URL"))

    # ------------------------------------------------------------------
    # httpx.Auth sync generator — delegates to async helpers via requires_response_body
    # ------------------------------------------------------------------

    requires_response_body = True

    def auth_flow(self, request: httpx.Request) -> Generator:
        token = self._get_cached_token()
        if token is None:
            # Synchronous fetch is not possible inside the generator; we inject a
            # sentinel token and handle the 401 path below.
            token = "__pending__"
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request

        if response.status_code == 401:
            # Invalidate and retry once
            self._invalidate_cache()
            new_token = self._fetch_token_sync()
            if new_token:
                request.headers["Authorization"] = f"Bearer {new_token}"
                yield request

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_cached_token(self) -> str | None:
        entry = self.__class__._cache.get(self._cache_key)
        if entry is None:
            return self._fetch_token_sync()
        # Proactive refresh if within 60 s of expiry
        if entry["expires_at"] - time.monotonic() < 60:
            return self._fetch_token_sync()
        return entry["access_token"]

    def _invalidate_cache(self) -> None:
        self.__class__._cache.pop(self._cache_key, None)

    def _fetch_token_sync(self) -> str | None:
        """Fetch an access token synchronously using httpx (blocking)."""
        cfg = self._config.oauth2
        credential = resolve_credential(self._config.credential_ref)

        if self._is_simulator_static_token_mode():
            logger.info(
                "oauth2_token_fetch_bypassed",
                connector=self._connector_name,
                reason="base_url_override_without_token_url",
            )
            return credential

        # credential_ref is expected to be "client_id:client_secret"
        if ":" in credential:
            client_id, client_secret = credential.split(":", 1)
        else:
            client_id, client_secret = credential, ""

        data: dict = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if cfg.scopes:
            data["scope"] = " ".join(cfg.scopes)

        try:
            with httpx.Client() as client:
                resp = client.post(self._token_url, data=data, timeout=15)
                resp.raise_for_status()
                body = resp.json()

            access_token: str = body["access_token"]
            expires_in: int = int(body.get("expires_in", 3600))
            self.__class__._cache[self._cache_key] = {
                "access_token": access_token,
                "expires_at": time.monotonic() + expires_in,
            }
            return access_token

        except Exception as exc:
            logger.error(
                "oauth2_token_fetch_failed",
                token_url=self._token_url,
                error=str(exc),
            )
            return None


class OAuth2RefreshTokenAuth(httpx.Auth):
    """OAuth2 authorization_code flow with refresh_token grant (T1 #11).

    Uses a stored refresh token (+ optional client credentials) to obtain and
    renew access tokens automatically.  Credential format is one of:
      - ``client_id:client_secret:refresh_token``
      - ``refresh_token``  (for public clients that don't require a secret)

    Token is cached per (credential_ref, token_url).  Proactive refresh occurs
    when fewer than 60 seconds remain before expiry.  On a 401 the cache is
    invalidated, the token refreshed, and the request retried once.

    If the token endpoint returns a new ``refresh_token``, the cached copy is
    updated so subsequent refreshes use the rotated value.
    """

    _cache: dict[tuple[str, str], dict] = {}
    _locks: dict[tuple[str, str], asyncio.Lock] = {}

    def __init__(self, config: OAuth2Auth, connector_name: str | None = None) -> None:
        self._config = config
        self._token_url = _resolve_oauth_url(config.oauth2.token_url, connector_name)
        self._refresh_url = (
            _resolve_oauth_url(config.oauth2.refresh_url, connector_name)
            if config.oauth2.refresh_url
            else None
        )
        self._cache_key = (config.credential_ref, self._refresh_url or self._token_url)
        if self._cache_key not in self.__class__._locks:
            self.__class__._locks[self._cache_key] = asyncio.Lock()

    requires_response_body = True

    def auth_flow(self, request: httpx.Request) -> Generator:
        token = self._get_cached_token()
        if token is None:
            token = "__pending__"
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request

        if response.status_code == 401:
            self._invalidate_cache()
            new_token = self._fetch_token_sync()
            if new_token:
                request.headers["Authorization"] = f"Bearer {new_token}"
                yield request

    def _get_cached_token(self) -> str | None:
        entry = self.__class__._cache.get(self._cache_key)
        if entry is None:
            return self._fetch_token_sync()
        if entry["expires_at"] - time.monotonic() < 60:
            return self._fetch_token_sync()
        return entry["access_token"]

    def _invalidate_cache(self) -> None:
        self.__class__._cache.pop(self._cache_key, None)

    def _parse_credential(self) -> tuple[str, str, str]:
        """Parse credential into (client_id, client_secret, refresh_token)."""
        credential = resolve_credential(self._config.credential_ref)
        parts = credential.split(":", 2)
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return parts[0], "", parts[1]
        # Single value = refresh token only (public client)
        return "", "", parts[0]

    def _fetch_token_sync(self) -> str | None:
        """Exchange refresh_token for a new access_token (blocking)."""
        cfg = self._config.oauth2
        client_id, client_secret, refresh_token = self._parse_credential()

        # Use a previously-rotated refresh token if available
        entry = self.__class__._cache.get(self._cache_key)
        if entry and entry.get("refresh_token"):
            refresh_token = entry["refresh_token"]

        token_url = self._refresh_url or self._token_url
        data: dict[str, str] = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        if client_id:
            data["client_id"] = client_id
        if client_secret:
            data["client_secret"] = client_secret
        if cfg.scopes:
            data["scope"] = " ".join(cfg.scopes)

        try:
            with httpx.Client() as client:
                resp = client.post(token_url, data=data, timeout=15)
                resp.raise_for_status()
                body = resp.json()

            access_token: str = body["access_token"]
            expires_in: int = int(body.get("expires_in", 3600))
            cache_entry: dict[str, Any] = {
                "access_token": access_token,
                "expires_at": time.monotonic() + expires_in,
            }
            # Persist rotated refresh token if the server issued a new one
            if "refresh_token" in body:
                cache_entry["refresh_token"] = body["refresh_token"]
            self.__class__._cache[self._cache_key] = cache_entry
            return access_token

        except Exception as exc:
            logger.error(
                "oauth2_refresh_token_failed",
                token_url=token_url,
                error=str(exc),
            )
            return None


class CustomAuthProvider(httpx.Auth):
    """Multi-step custom authentication flow (T1 #24).

    Executes a sequence of HTTP requests defined in ``config.custom.steps``
    to acquire a session token, then injects the result into subsequent
    requests according to ``config.custom.inject``.

    Each step is a dict with keys:
      - ``url``: endpoint to call
      - ``method``: HTTP method (default ``POST``)
      - ``body``: request body dict (optional, supports ``${credential}`` placeholder)
      - ``token_field``: dot-notation path to extract from the response

    The ``inject`` dict maps header/query names to extracted values:
      - ``{"header": "X-Session-Token"}`` — inject as header
      - ``{"query": "token"}`` — inject as query parameter

    Token is cached per (credential_ref, first step URL).  On 401 the cache
    is invalidated, steps re-executed, and the request retried once.
    """

    _cache: dict[tuple[str, str], dict[str, Any]] = {}
    _locks: dict[tuple[str, str], asyncio.Lock] = {}

    def __init__(self, config: CustomAuth) -> None:
        self._config = config
        first_url = config.custom.steps[0].get("url", "")
        self._cache_key = (config.credential_ref, first_url)
        if self._cache_key not in self.__class__._locks:
            self.__class__._locks[self._cache_key] = asyncio.Lock()

    requires_response_body = True

    def auth_flow(self, request: httpx.Request) -> Generator:
        token = self._get_cached_token()
        self._inject_token(request, token)
        response = yield request

        if response.status_code == 401:
            self._invalidate_cache()
            new_token = self._execute_steps_sync()
            if new_token:
                self._inject_token(request, new_token)
                yield request

    def _inject_token(self, request: httpx.Request, token: str) -> None:
        inject = self._config.custom.inject
        if "header" in inject:
            request.headers[inject["header"]] = token
        elif "query" in inject:
            request.url = request.url.copy_add_param(inject["query"], token)

    def _get_cached_token(self) -> str:
        entry = self.__class__._cache.get(self._cache_key)
        if entry is None or entry["expires_at"] - time.monotonic() < 60:
            return self._execute_steps_sync()
        return entry["token"]

    def _invalidate_cache(self) -> None:
        self.__class__._cache.pop(self._cache_key, None)

    def _execute_steps_sync(self) -> str:
        """Execute all auth steps sequentially and return the final token."""
        try:
            credential = resolve_credential(self._config.credential_ref)
        except Exception as exc:
            logger.error("custom_auth_credential_failed", error=str(exc))
            return ""

        token = ""
        refresh_cfg = self._config.custom.refresh
        lifetime = float(refresh_cfg.get("token_lifetime_secs", 3600)) if refresh_cfg else 3600.0

        with httpx.Client(timeout=15.0) as client:
            for step in self._config.custom.steps:
                url = step.get("url", "")
                method = step.get("method", "POST").upper()
                body = dict(step.get("body", {}))
                token_field = step.get("token_field", "token")

                # Substitute ${credential} placeholders in body values
                for k, v in body.items():
                    if isinstance(v, str) and "${credential}" in v:
                        body[k] = v.replace("${credential}", credential)

                # Inject credential as username:password if body is empty
                if not body and ":" in credential:
                    username, password = credential.split(":", 1)
                    body = {"username": username, "password": password}
                elif not body:
                    body = {"token": credential}

                try:
                    if method in ("POST", "PUT", "PATCH"):
                        resp = client.request(method, url, json=body)
                    else:
                        resp = client.request(method, url, params=body)
                    resp.raise_for_status()
                    resp_data = resp.json()
                except Exception as exc:
                    logger.error(
                        "custom_auth_step_failed",
                        url=url,
                        method=method,
                        error=str(exc),
                    )
                    return ""

                # Extract token via dot-notation path
                token = self._resolve_path(resp_data, token_field)
                if not token:
                    logger.error(
                        "custom_auth_token_field_missing",
                        url=url,
                        token_field=token_field,
                    )
                    return ""

        self.__class__._cache[self._cache_key] = {
            "token": token,
            "expires_at": time.monotonic() + lifetime,
        }
        return token

    @staticmethod
    def _resolve_path(data: dict, path: str) -> str:
        """Resolve a dot-notation path from a nested dict."""
        cur: Any = data
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return ""
        return str(cur) if cur is not None else ""


def invalidate_credential_cache(credential_ref: str) -> int:
    """Invalidate all cached tokens for a given credential_ref.

    Used by the ``rotate-credential`` control command to force re-acquisition
    of tokens after credential rotation.  Returns the number of cache entries
    invalidated.
    """
    invalidated = 0
    for cache_cls in (OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth, CustomAuthProvider):
        to_remove = [k for k in cache_cls._cache if k[0] == credential_ref]
        for k in to_remove:
            cache_cls._cache.pop(k, None)
            invalidated += 1
    return invalidated


def build_auth_provider(config: AuthConfig, connector_name: str | None = None) -> httpx.Auth:
    """Build an httpx.Auth instance from a connector auth config."""
    if isinstance(config, ApiKeyAuth):
        return ApiKeyAuthProvider(config)
    if isinstance(config, OAuth2Auth):
        if config.oauth2.grant_type == "client_credentials":
            return OAuth2ClientCredentialsAuth(config, connector_name)
        # authorization_code: use refresh_token flow for automatic token renewal
        return OAuth2RefreshTokenAuth(config, connector_name)
    if isinstance(config, JwtAuth):
        token = resolve_credential(config.credential_ref)
        return BearerTokenAuth(token)
    if isinstance(config, CustomAuth):
        return CustomAuthProvider(config)
    raise NotImplementedError(f"Auth type not yet implemented: {getattr(config, 'type', '?')}")
