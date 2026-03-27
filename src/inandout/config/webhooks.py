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


class SignatureEncoding(StrEnum):
    hex_prefix = "hex_prefix"  # "sha256=deadbeef..." (GitHub, default)
    base64 = "base64"  # raw base64 (Shopify, SuperOffice)


class SignatureConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    algorithm: SignatureAlgorithm
    header: str
    credential_ref: str
    encoding: SignatureEncoding = SignatureEncoding.hex_prefix
    version: str | None = None
    # Secret rotation support
    rotation_credential_ref: str | None = None  # Secondary credential during rotation
    rotation_grace_period: str = "1h"  # How long to accept both secrets


class FanOutRoute(BaseModel):
    model_config = ConfigDict(extra="allow")

    match: str
    datatype: str
    notification_only: bool = False  # payload is a notification, not full state
    notification_external_id_field: str = "id"  # field to extract external_id from payload
    # When True, the route match itself is the deletion signal — no payload inspection.
    # The record ID is taken from notification_external_id_field.
    # Example: "contact.deletion" (HubSpot), "customer.delete" (Tripletex).
    is_delete: bool = False
    # When set, this payload field being null (JSON null) signals a delete.
    # Only consulted when is_delete is False (is_delete takes precedence).
    # Useful for providers that use a single event type for both upsert and delete,
    # expressing "delete" by setting a field to null.
    null_record_field: str | None = None


class UnmatchedAction(StrEnum):
    log_and_discard = "log_and_discard"
    reject_400 = "reject_400"


class FanOutConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Body-field discriminator (mutually exclusive with discriminator_header;
    # discriminator_header takes precedence if both are set).
    discriminator: str | None = None
    # HTTP header discriminator — avoids a body parse before auth (FEAT-WH-08).
    # e.g. SuperOffice sends X-SuperOffice-Event: contact.changed
    discriminator_header: str | None = None
    routes: list[FanOutRoute] = []
    unmatched: UnmatchedAction


class WebhookRegistrationConfig(BaseModel):
    """Config for active webhook lifecycle management (T1 #7)."""

    model_config = ConfigDict(extra="forbid")

    register_path: str  # POST to register webhook
    deregister_path: str | None = None  # DELETE to remove, ${webhook_id} interpolated
    renew_path: str | None = None  # PUT/PATCH to renew
    renew_interval: str = "7d"  # how often to renew
    health_check_path: str | None = None  # GET to verify still active
    # When set, the health_check response body is parsed as JSON and the value
    # at this dot-notation path is compared against health_check_active_value.
    # If it does not match, the subscription is treated as inactive even on 200.
    # Both fields must be set together; health_check_active_value has no default.
    # Example: health_check_active_field: "value.status" (Tripletex)
    health_check_active_field: str | None = None
    health_check_active_value: str | None = None
    id_response_path: str = "id"  # dot-notation path to extract webhook ID
    callback_url_runtime_param: str = "callback_url"  # runtime param name for our URL
    # When True, register one subscription per fan_out route (e.g. Tripletex
    # requires a separate POST per event type like "customer.create").
    per_route_registration: bool = False
    # Extra body fields for every registration POST.  Supports placeholders:
    #   ${route_event}          → replaced with route.match for per-route registration
    #   ${credential:<ref>}     → resolved via the standard credential resolver
    register_body_extra: dict[str, str] = {}
    # Extra headers for the registration POST (same placeholder support).
    register_headers_extra: dict[str, str] = {}
    # When set, collect all fan_out.routes[].match values into a list and
    # include it under this key in a single registration POST body
    # (SuperOffice "Events" array pattern — FEAT-WH-07).
    register_events_field: str | None = None


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
    event_id_field: str | None = None  # A5: dedup — field holding event ID
    dedup_ttl: str = "24h"  # A5: how long to remember seen event IDs
    ip_allowlist: list[
        str
    ] = []  # T1 #42: per-connector IP allowlist (CIDR); restricts beyond server-wide list
    rate_limit_per_minute: int | None = (
        None  # T1 #42: per-connector rate limit (None = inherit server-wide)
    )
