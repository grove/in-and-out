"""Webhook configuration models.

Covers schemas/defs/webhooks.schema.json.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class SignatureAlgorithm(StrEnum):
    hmac_sha256 = "hmac-sha256"
    hmac_sha1 = "hmac-sha1"
    rsa_sha256 = "rsa-sha256"


class SignatureConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    algorithm: SignatureAlgorithm
    header: str
    credential_ref: str
    version: str | None = None
    # Secret rotation support
    rotation_credential_ref: str | None = None  # Secondary credential during rotation
    rotation_grace_period: str = "1h"  # How long to accept both secrets


class FanOutRoute(BaseModel):
    model_config = ConfigDict(extra="allow")

    match: str
    datatype: str
    notification_only: bool = False                      # payload is a notification, not full state
    notification_external_id_field: str = "id"           # field to extract external_id from payload


class UnmatchedAction(StrEnum):
    log_and_discard = "log_and_discard"
    reject_400 = "reject_400"


class FanOutConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discriminator: str
    routes: list[FanOutRoute] = []
    unmatched: UnmatchedAction


class WebhookRegistrationConfig(BaseModel):
    """Config for active webhook lifecycle management (T1 #7)."""

    model_config = ConfigDict(extra="forbid")

    register_path: str                             # POST to register webhook
    deregister_path: str | None = None             # DELETE to remove, ${webhook_id} interpolated
    renew_path: str | None = None                  # PUT/PATCH to renew
    renew_interval: str = "7d"                     # how often to renew
    health_check_path: str | None = None           # GET to verify still active
    id_response_path: str = "id"                   # dot-notation path to extract webhook ID
    callback_url_runtime_param: str = "callback_url"  # runtime param name for our URL
    # When True, register one subscription per fan_out route (e.g. Tripletex
    # requires a separate POST per event type like "customer.create").
    per_route_registration: bool = False
    # Extra static fields to include in every registration POST body (e.g.
    # Tripletex needs {"event": "<event_name>"} alongside the targetUrl).
    register_body_extra: dict[str, str] = {}


class WebhookConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    path: str
    # HMAC signature verification.  Optional: some providers use custom header
    # forwarding (e.g. Tripletex) instead of HMAC.  When None, set
    # auth_header_name + auth_header_credential_ref for header-equality auth.
    signature: SignatureConfig | None = None
    # For connectors that forward a static auth header to the callback URL
    # instead of signing with HMAC (e.g. Tripletex's authHeaderName/Value).
    auth_header_name: str | None = None
    auth_header_credential_ref: str | None = None
    # fan_out describes the event-type discriminator and per-datatype routing.
    # Optional for fire-and-forget notification connectors that don't multiplex
    # multiple event types on a single endpoint.
    fan_out: FanOutConfig | None = None
    registration: WebhookRegistrationConfig | None = None  # A1: lifecycle management
    event_id_field: str | None = None              # A5: dedup — field holding event ID
    dedup_ttl: str = "24h"                         # A5: how long to remember seen event IDs
    ip_allowlist: list[str] = []           # T1 #42: per-connector IP allowlist (CIDR); restricts beyond server-wide list
    rate_limit_per_minute: int | None = None  # T1 #42: per-connector rate limit (None = inherit server-wide)
