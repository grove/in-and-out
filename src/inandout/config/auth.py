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


class SamlConfig(BaseModel):
    """SAML 2.0 authentication configuration."""
    model_config = ConfigDict(extra="allow")
    
    idp_entity_id: str                      # Identity Provider entity ID
    idp_sso_url: str                        # Identity Provider SSO URL
    idp_x509_cert: str | None = None        # IDP certificate (PEM format) or credential_ref
    sp_entity_id: str                       # Service Provider (our) entity ID
    assertion_consumer_service_url: str     # ACS URL for SAML response
    name_id_format: str = "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
    requested_authn_context: list[str] | None = None
    # Token extraction
    token_attribute: str = "sessionToken"   # SAML attribute containing API token
    token_injection: dict[str, Any] | None = None


class SamlAuth(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    type: Literal["saml"]
    credential_ref: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    saml: SamlConfig


class KerberosConfig(BaseModel):
    """Kerberos (GSSAPI) authentication configuration."""
    model_config = ConfigDict(extra="allow")
    
    service: str                            # Service principal (e.g., "HTTP@api.example.com")
    keytab_path: str | None = None          # Path to keytab file
    credential_cache: str | None = None     # Path to credential cache
    mutual_authentication: str = "REQUIRED"  # REQUIRED, OPTIONAL, or DISABLED
    delegate: bool = False                   # Enable credential delegation
    force_preemptive: bool = True            # Send auth on first request


class KerberosAuth(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    type: Literal["kerberos"]
    credential_ref: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    kerberos: KerberosConfig


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
    OAuth2Auth | ApiKeyAuth | JwtAuth | CustomAuth | SamlAuth | KerberosAuth,
    Field(discriminator="type"),
]
