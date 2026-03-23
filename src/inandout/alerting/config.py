"""Alerting configuration models."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class AlertChannel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["webhook", "slack", "pagerduty"]
    url: str  # webhook URL or Slack webhook URL
    integration_key: str | None = None  # PagerDuty only
    severity: Literal["info", "warning", "critical"] = "warning"


class AlertRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    condition: Literal["sla_violated", "circuit_open", "dead_letter_threshold", "sync_failed"]
    threshold: int | float | None = None  # for dead_letter_threshold: row count
    channels: list[str]  # channel names


class AlertingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    channels: dict[str, AlertChannel] = {}
    rules: list[AlertRule] = []
