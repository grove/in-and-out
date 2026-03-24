"""Alerting package — dispatcher and channel config for connector health events."""
from __future__ import annotations

from inandout.alerting.config import AlertingConfig
from inandout.alerting.dispatcher import AlertDispatcher, AlertEventType

__all__ = ["AlertingConfig", "AlertDispatcher", "AlertEventType"]
