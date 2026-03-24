"""Alerting channel configuration models.

Declares the channels (Slack, PagerDuty, generic webhook) through which
in-and-out dispatches health events: connector unavailable, recovered,
circuit breaker open/closed, and SLA violation.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class SlackAlertingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    webhook_url: str
    username: str = "in-and-out"
    icon_emoji: str = ":warning:"


class PagerDutyAlertingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_key: str
    severity: Literal["critical", "error", "warning", "info"] = "error"


class WebhookAlertingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_secs: float = 10.0


class AlertingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    slack: SlackAlertingConfig | None = None
    pagerduty: PagerDutyAlertingConfig | None = None
    webhook: WebhookAlertingConfig | None = None

    # Per-event-type suppression
    on_connector_unavailable: bool = True
    on_connector_recovered: bool = True
    on_circuit_breaker_open: bool = True
    on_circuit_breaker_closed: bool = False
    on_sla_violation: bool = True
