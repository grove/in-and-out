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


class UnmatchedAction(StrEnum):
    log_and_discard = "log_and_discard"
    reject_400 = "reject_400"


class FanOutConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    discriminator: str
    routes: list[FanOutRoute] = []
    unmatched: UnmatchedAction


class WebhookConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    path: str
    signature: SignatureConfig
    fan_out: FanOutConfig
