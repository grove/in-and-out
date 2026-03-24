"""Alert dispatcher — fires configured channels on connector health events.

Supported channels: Slack incoming webhook, PagerDuty Events v2 API,
generic HTTP webhook.  Each channel failure is logged as a warning and
never propagates to the caller so alerting failures never destabilise
the main sync loop.
"""
from __future__ import annotations

from enum import StrEnum
from typing import Any

import httpx
import structlog

from inandout.alerting.config import AlertingConfig

logger = structlog.get_logger(__name__)


class AlertEventType(StrEnum):
    connector_unavailable = "connector_unavailable"
    connector_recovered = "connector_recovered"
    circuit_breaker_open = "circuit_breaker_open"
    circuit_breaker_closed = "circuit_breaker_closed"
    sla_violation = "sla_violation"


class AlertDispatcher:
    """Dispatches health-event alerts to all configured channels."""

    def __init__(self, cfg: AlertingConfig) -> None:
        self._cfg = cfg

    async def dispatch(
        self,
        event_type: AlertEventType,
        connector: str,
        datatype: str | None,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """Fire an alert to all configured channels.

        Never raises — channel failures are logged as warnings.
        """
        if not self._cfg.enabled:
            return

        # Per-event suppression
        if event_type == AlertEventType.connector_unavailable and not self._cfg.on_connector_unavailable:
            return
        if event_type == AlertEventType.connector_recovered and not self._cfg.on_connector_recovered:
            return
        if event_type == AlertEventType.circuit_breaker_open and not self._cfg.on_circuit_breaker_open:
            return
        if event_type == AlertEventType.circuit_breaker_closed and not self._cfg.on_circuit_breaker_closed:
            return
        if event_type == AlertEventType.sla_violation and not self._cfg.on_sla_violation:
            return

        log = logger.bind(
            alert_event=str(event_type), connector=connector, datatype=datatype
        )

        if self._cfg.slack:
            await _dispatch_slack(
                self._cfg.slack, event_type, connector, datatype, message, log
            )
        if self._cfg.pagerduty:
            await _dispatch_pagerduty(
                self._cfg.pagerduty, event_type, connector, datatype, message, detail, log
            )
        if self._cfg.webhook:
            await _dispatch_webhook(
                self._cfg.webhook, event_type, connector, datatype, message, detail, log
            )


async def _dispatch_slack(cfg: Any, event_type: AlertEventType, connector: str, datatype: str | None, message: str, log: Any) -> None:
    dt_str = f"/{datatype}" if datatype else ""
    text = (
        f"*[in-and-out]* `{connector}{dt_str}` — "
        f"`{event_type}`: {message}"
    )
    payload = {
        "username": cfg.username,
        "icon_emoji": cfg.icon_emoji,
        "text": text,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(cfg.webhook_url, json=payload)
            resp.raise_for_status()
        log.info("alert_dispatched", channel="slack")
    except Exception as exc:
        log.warning("alert_dispatch_failed", channel="slack", error=str(exc))


_RESOLVE_EVENTS = frozenset({
    AlertEventType.connector_recovered,
    AlertEventType.circuit_breaker_closed,
})

_PD_SEVERITY: dict[AlertEventType, str] = {
    AlertEventType.connector_unavailable: "error",
    AlertEventType.circuit_breaker_open: "error",
    AlertEventType.sla_violation: "warning",
    AlertEventType.connector_recovered: "info",
    AlertEventType.circuit_breaker_closed: "info",
}


async def _dispatch_pagerduty(cfg: Any, event_type: AlertEventType, connector: str, datatype: str | None, message: str, detail: dict | None, log: Any) -> None:
    dt_str = f"/{datatype}" if datatype else ""
    summary = f"[in-and-out] {connector}{dt_str}: {message}"
    event_action = "resolve" if event_type in _RESOLVE_EVENTS else "trigger"
    payload = {
        "routing_key": cfg.integration_key,
        "event_action": event_action,
        "dedup_key": f"inout:{connector}:{datatype}:{event_type}",
        "payload": {
            "summary": summary,
            "severity": _PD_SEVERITY.get(event_type, cfg.severity),
            "source": "in-and-out",
            "custom_details": detail or {},
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://events.pagerduty.com/v2/enqueue", json=payload
            )
            resp.raise_for_status()
        log.info("alert_dispatched", channel="pagerduty")
    except Exception as exc:
        log.warning("alert_dispatch_failed", channel="pagerduty", error=str(exc))


async def _dispatch_webhook(cfg: Any, event_type: AlertEventType, connector: str, datatype: str | None, message: str, detail: dict | None, log: Any) -> None:
    payload = {
        "event_type": str(event_type),
        "connector": connector,
        "datatype": datatype,
        "message": message,
        "detail": detail or {},
    }
    try:
        async with httpx.AsyncClient(timeout=cfg.timeout_secs) as client:
            resp = await client.request(
                cfg.method, cfg.url, json=payload, headers=cfg.headers
            )
            resp.raise_for_status()
        log.info("alert_dispatched", channel="webhook")
    except Exception as exc:
        log.warning("alert_dispatch_failed", channel="webhook", error=str(exc))
