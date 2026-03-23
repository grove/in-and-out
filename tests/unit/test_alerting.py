"""Unit tests for Step 50 — Alerting webhooks."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_alerting_config(
    channel_name: str = "chan1",
    channel_type: str = "webhook",
    url: str = "https://hooks.example.com/notify",
    rule_name: str = "test_rule",
    condition: str = "sla_violated",
    integration_key: str | None = None,
) -> tuple:
    from inandout.alerting.config import AlertChannel, AlertRule, AlertingConfig

    channel = AlertChannel(type=channel_type, url=url, integration_key=integration_key)
    rule = AlertRule(name=rule_name, condition=condition, channels=[channel_name])
    cfg = AlertingConfig(
        enabled=True,
        channels={channel_name: channel},
        rules=[rule],
    )
    return cfg, channel, rule


# ---------------------------------------------------------------------------
# test_webhook_channel_fires
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_webhook_channel_fires():
    """Webhook channel posts JSON payload with rule/condition/context."""
    cfg, channel, rule = make_alerting_config(
        url="https://hooks.example.com/notify",
        channel_type="webhook",
    )
    from inandout.alerting.dispatcher import AlertDispatcher

    dispatcher = AlertDispatcher.from_config(cfg)
    with respx.mock:
        route = respx.post("https://hooks.example.com/notify").mock(
            return_value=httpx.Response(200)
        )
        await dispatcher.fire("test_rule", "sla_violated", {"connector": "hubspot"})

    assert route.called
    req = route.calls[0].request
    import json
    body = json.loads(req.content)
    assert body["rule"] == "test_rule"
    assert body["condition"] == "sla_violated"
    assert body["context"]["connector"] == "hubspot"


# ---------------------------------------------------------------------------
# test_slack_channel_formats_message
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_slack_channel_formats_message():
    """Slack channel posts text field with rule and condition."""
    cfg, channel, rule = make_alerting_config(
        channel_type="slack",
        url="https://hooks.slack.com/services/T00/B00/xxx",
    )
    from inandout.alerting.dispatcher import AlertDispatcher

    dispatcher = AlertDispatcher.from_config(cfg)
    with respx.mock:
        route = respx.post("https://hooks.slack.com/services/T00/B00/xxx").mock(
            return_value=httpx.Response(200)
        )
        await dispatcher.fire("test_rule", "sla_violated", {"detail": "lag=3600"})

    assert route.called
    req = route.calls[0].request
    import json
    body = json.loads(req.content)
    assert "text" in body
    assert "test_rule" in body["text"]
    assert "sla_violated" in body["text"]


# ---------------------------------------------------------------------------
# test_pagerduty_channel_sends_trigger
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_pagerduty_channel_sends_trigger():
    """PagerDuty channel sends event_action=trigger with routing_key and summary."""
    from inandout.alerting.config import AlertChannel, AlertRule, AlertingConfig

    channel = AlertChannel(
        type="pagerduty",
        url="https://events.pagerduty.com/v2/enqueue",
        integration_key="abc123",
    )
    rule = AlertRule(name="pd_rule", condition="circuit_open", channels=["pd"])
    cfg = AlertingConfig(
        enabled=True,
        channels={"pd": channel},
        rules=[rule],
    )
    from inandout.alerting.dispatcher import AlertDispatcher

    dispatcher = AlertDispatcher.from_config(cfg)
    with respx.mock:
        route = respx.post("https://events.pagerduty.com/v2/enqueue").mock(
            return_value=httpx.Response(202)
        )
        await dispatcher.fire("pd_rule", "circuit_open", {"connector": "salesforce"})

    assert route.called
    req = route.calls[0].request
    import json
    body = json.loads(req.content)
    assert body["routing_key"] == "abc123"
    assert body["event_action"] == "trigger"
    assert "pd_rule" in body["payload"]["summary"]
    assert "circuit_open" in body["payload"]["summary"]


# ---------------------------------------------------------------------------
# test_alert_dispatcher_never_raises
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_alert_dispatcher_never_raises():
    """HTTP error in channel → logged, not raised."""
    cfg, channel, rule = make_alerting_config(
        url="https://hooks.example.com/notify",
        channel_type="webhook",
    )
    from inandout.alerting.dispatcher import AlertDispatcher

    dispatcher = AlertDispatcher.from_config(cfg)
    with respx.mock:
        respx.post("https://hooks.example.com/notify").mock(
            side_effect=httpx.ConnectError("connection refused")
        )
        # Must not raise
        await dispatcher.fire("test_rule", "sla_violated", {})


@pytest.mark.anyio
async def test_alert_dispatcher_http_error_never_raises():
    """HTTP 500 in channel → logged, not raised."""
    cfg, channel, rule = make_alerting_config(
        url="https://hooks.example.com/notify",
        channel_type="webhook",
    )
    from inandout.alerting.dispatcher import AlertDispatcher

    dispatcher = AlertDispatcher.from_config(cfg)
    with respx.mock:
        respx.post("https://hooks.example.com/notify").mock(
            return_value=httpx.Response(500)
        )
        # Must not raise
        await dispatcher.fire("test_rule", "sla_violated", {})


# ---------------------------------------------------------------------------
# test_rule_matching
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_rule_matching():
    """Only the rule matching by name fires to its channels."""
    from inandout.alerting.config import AlertChannel, AlertRule, AlertingConfig
    from inandout.alerting.dispatcher import AlertDispatcher

    ch_a = AlertChannel(type="webhook", url="https://a.example.com/hook")
    ch_b = AlertChannel(type="webhook", url="https://b.example.com/hook")
    rule_a = AlertRule(name="rule_a", condition="sla_violated", channels=["a"])
    rule_b = AlertRule(name="rule_b", condition="circuit_open", channels=["b"])
    cfg = AlertingConfig(
        enabled=True,
        channels={"a": ch_a, "b": ch_b},
        rules=[rule_a, rule_b],
    )
    dispatcher = AlertDispatcher.from_config(cfg)

    with respx.mock:
        route_a = respx.post("https://a.example.com/hook").mock(
            return_value=httpx.Response(200)
        )
        route_b = respx.post("https://b.example.com/hook").mock(
            return_value=httpx.Response(200)
        )
        # Fire only rule_a
        await dispatcher.fire("rule_a", "sla_violated", {})

    assert route_a.called
    assert not route_b.called


@pytest.mark.anyio
async def test_disabled_alerting_does_not_fire():
    """Disabled alerting config never fires."""
    from inandout.alerting.config import AlertChannel, AlertRule, AlertingConfig
    from inandout.alerting.dispatcher import AlertDispatcher

    ch = AlertChannel(type="webhook", url="https://hooks.example.com/notify")
    rule = AlertRule(name="r", condition="sla_violated", channels=["c"])
    cfg = AlertingConfig(enabled=False, channels={"c": ch}, rules=[rule])
    dispatcher = AlertDispatcher.from_config(cfg)

    with respx.mock:
        route = respx.post("https://hooks.example.com/notify").mock(
            return_value=httpx.Response(200)
        )
        await dispatcher.fire("r", "sla_violated", {})

    assert not route.called
