"""httpx.Auth implementations for each auth scheme."""
from __future__ import annotations

import asyncio
import os
import time
from typing import Generator

import httpx

from inandout.config.auth import AuthConfig, OAuth2Auth, ApiKeyAuth, JwtAuth, CustomAuth


def resolve_credential(credential_ref: str) -> str:
    env_var = f"INOUT_CREDENTIAL_{credential_ref.upper().replace('-', '_')}"
    value = os.environ.get(env_var)
    if value is None:
        raise EnvironmentError(
            f"Credential not resolved: {credential_ref!r}. "
            f"Set env var {env_var} or use a credential store."
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

    def __init__(self, config: OAuth2Auth) -> None:
        self._config = config
        self._cache_key = (config.credential_ref, config.oauth2.token_url)
        if self._cache_key not in self.__class__._locks:
            self.__class__._locks[self._cache_key] = asyncio.Lock()

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
                resp = client.post(cfg.token_url, data=data, timeout=15)
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
            import structlog
            structlog.get_logger(__name__).error(
                "oauth2_token_fetch_failed",
                token_url=cfg.token_url,
                error=str(exc),
            )
            return None


def build_auth_provider(config: AuthConfig) -> httpx.Auth:
    """Build an httpx.Auth instance from a connector auth config."""
    if isinstance(config, ApiKeyAuth):
        return ApiKeyAuthProvider(config)
    if isinstance(config, OAuth2Auth):
        if config.oauth2.grant_type == "client_credentials":
            return OAuth2ClientCredentialsAuth(config)
        # authorization_code: resolve a pre-obtained access token from credential store
        token = resolve_credential(config.credential_ref)
        return BearerTokenAuth(token)
    if isinstance(config, JwtAuth):
        token = resolve_credential(config.credential_ref)
        return BearerTokenAuth(token)
    # CustomAuth: not yet implemented
    raise NotImplementedError(f"Auth type not yet implemented: {config.type}")
