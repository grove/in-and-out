"""Authentication configuration models.

Covers all four auth schemes declared in schemas/defs/auth.schema.json:
  oauth2, api_key, jwt, custom
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class OAuth2Config(BaseModel):
    model_config = ConfigDict(extra="allow")

    grant_type: Literal["authorization_code", "client_credentials"]
    token_url: str
    refresh_url: str | None = None
    scopes: list[str] = []
    token_injection: dict[str, Any] | None = None


class OAuth2Auth(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["oauth2"]
    credential_ref: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    oauth2: OAuth2Config


class ApiKeyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location: Literal["header", "query"]
    name: str


class ApiKeyAuth(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["api_key"]
    credential_ref: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    api_key: ApiKeyConfig


class JwtConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    algorithm: str
    issuer: str
    audience: str
    expiry: int | None = Field(default=None, ge=1)
    claims: dict[str, Any] | None = None
    token_injection: dict[str, Any] | None = None


class JwtAuth(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["jwt"]
    credential_ref: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    jwt: JwtConfig


class CustomConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    steps: list[dict[str, Any]] = Field(min_length=1)
    inject: dict[str, Any]
    refresh: dict[str, Any] | None = None


class CustomAuth(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["custom"]
    credential_ref: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    custom: CustomConfig


class PreRequestAuthConfig(BaseModel):
    """Config for pre-request session-token authentication flows (A3 — T1 #24)."""

    model_config = ConfigDict(extra="forbid")

    endpoint: str                          # URL to POST to acquire session token
    method: str = "POST"
    credential_ref: str                    # env var holding "username:password" or just a token
    token_field: str = "token"             # dot-notation path to token in response JSON
    token_lifetime_secs: float = 3600.0
    request_body: dict = {}               # static request body fields (credential injected separately)
    token_header: str = "X-Session-Token" # header to inject on subsequent requests


AuthConfig = Annotated[
    OAuth2Auth | ApiKeyAuth | JwtAuth | CustomAuth,
    Field(discriminator="type"),
]
