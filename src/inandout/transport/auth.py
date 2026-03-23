"""httpx.Auth implementations for each auth scheme."""
from __future__ import annotations

import os

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

    def auth_flow(self, request: httpx.Request):
        if self._location == "header":
            request.headers[self._name] = self._secret
        else:
            request.url = request.url.copy_add_param(self._name, self._secret)
        yield request


class BearerTokenAuth(httpx.Auth):
    """Simple bearer token — used internally after OAuth2 token refresh."""

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_flow(self, request: httpx.Request):
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


def build_auth_provider(config: AuthConfig) -> httpx.Auth:
    """Build an httpx.Auth instance from a connector auth config."""
    if isinstance(config, ApiKeyAuth):
        return ApiKeyAuthProvider(config)
    if isinstance(config, OAuth2Auth):
        # For the research phase: resolve the access token directly from credential store.
        # Full OAuth2 token refresh lifecycle is a future enhancement.
        token = resolve_credential(config.credential_ref)
        return BearerTokenAuth(token)
    if isinstance(config, JwtAuth):
        token = resolve_credential(config.credential_ref)
        return BearerTokenAuth(token)
    # CustomAuth: not yet implemented
    raise NotImplementedError(f"Auth type not yet implemented: {config.type}")
