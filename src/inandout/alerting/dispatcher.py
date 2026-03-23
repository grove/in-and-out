"""Alert dispatcher — fires outbound notifications to configured channels."""
from __future__ import annotations

from typing import Any

import httpx
import structlog

from inandout.alerting.config import AlertChannel, AlertingConfig

logger = structlog.get_logger(__name__)


class AlertDispatcher:
    """Dispatches alert events to registered channels."""

    def __init__(self, config: AlertingConfig) -> None:
        self._config = config

    @classmethod
    def from_config(cls, config: AlertingConfig) -> "AlertDispatcher":
        return cls(config)

    async def fire(self, rule_name: str, condition: str, context: dict[str, Any]) -> None:
        """Fire an alert for all channels matching the rule_name."""
        if not self._config.enabled:
            return

        matching_rules = [r for r in self._config.rules if r.name == rule_name]
        if not matching_rules:
            # Also match by condition
            matching_rules = [r for r in self._config.rules if r.condition == condition]

        for rule in matching_rules:
            for channel_name in rule.channels:
                channel = self._config.channels.get(channel_name)
                if channel is None:
                    logger.warning(
                        "alert_channel_not_found",
                        rule=rule_name,
                        channel=channel_name,
                    )
                    continue
                await self._dispatch_channel(rule_name, condition, context, channel)

    async def _dispatch_channel(
        self,
        rule_name: str,
        condition: str,
        context: dict[str, Any],
        channel: AlertChannel,
    ) -> None:
        """Dispatch to a single channel. Never raises — errors are logged."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                if channel.type == "webhook":
                    await client.post(
                        channel.url,
                        json={"rule": rule_name, "condition": condition, "context": context},
                    )
                elif channel.type == "slack":
                    text = f":warning: *{rule_name}*: {condition}\n{context}"
                    await client.post(channel.url, json={"text": text})
                elif channel.type == "pagerduty":
                    payload = {
                        "routing_key": channel.integration_key,
                        "event_action": "trigger",
                        "payload": {
                            "summary": f"{rule_name}: {condition}",
                            "severity": channel.severity,
                            "source": "inandout",
                            "custom_details": context,
                        },
                    }
                    await client.post(
                        "https://events.pagerduty.com/v2/enqueue",
                        json=payload,
                    )
        except Exception as exc:
            logger.warning(
                "alert_dispatch_failed",
                rule=rule_name,
                condition=condition,
                channel_type=channel.type,
                error=str(exc),
            )
