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


class WebhookConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    path: str
    signature: SignatureConfig
    fan_out: FanOutConfig
    registration: WebhookRegistrationConfig | None = None  # A1: lifecycle management
    event_id_field: str | None = None              # A5: dedup — field holding event ID
    dedup_ttl: str = "24h"                         # A5: how long to remember seen event IDs
    ip_allowlist: list[str] = []           # T1 #42: per-connector IP allowlist (CIDR); restricts beyond server-wide list
    rate_limit_per_minute: int | None = None  # T1 #42: per-connector rate limit (None = inherit server-wide)
