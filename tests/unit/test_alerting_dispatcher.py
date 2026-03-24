"""Unit tests for the alerting dispatcher."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.alerting.config import (
    AlertingConfig,
    PagerDutyAlertingConfig,
    SlackAlertingConfig,
    WebhookAlertingConfig,
)
from inandout.alerting.dispatcher import AlertDispatcher, AlertEventType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slack_cfg() -> SlackAlertingConfig:
    return SlackAlertingConfig(webhook_url="https://hooks.slack.com/fake")


def _pd_cfg() -> PagerDutyAlertingConfig:
    return PagerDutyAlertingConfig(integration_key="fake-key")


def _webhook_cfg() -> WebhookAlertingConfig:
    return WebhookAlertingConfig(url="https://example.com/alert")


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_alerting_config_defaults():
    cfg = AlertingConfig()
    assert cfg.enabled is True
    assert cfg.slack is None
    assert cfg.pagerduty is None
    assert cfg.webhook is None


def test_alerting_config_disabled_skips_all():
    cfg = AlertingConfig(enabled=False, slack=_slack_cfg())
    dispatcher = AlertDispatcher(cfg)
    # Should return immediately without making any HTTP calls
    import asyncio
    asyncio.run(dispatcher.dispatch(
        AlertEventType.connector_unavailable,
        connector="test",
        datatype="records",
        message="down",
    ))


def test_slack_config_requires_webhook_url():
    slack = SlackAlertingConfig(webhook_url="https://hooks.slack.com/xyz")
    assert slack.webhook_url == "https://hooks.slack.com/xyz"
    assert slack.username == "in-and-out"


def test_pagerduty_config_defaults():
    pd = PagerDutyAlertingConfig(integration_key="abc123")
    assert pd.severity == "error"


def test_webhook_config_defaults():
    wh = WebhookAlertingConfig(url="https://example.com/hook")
    assert wh.method == "POST"
    assert wh.timeout_secs == 10.0


# ---------------------------------------------------------------------------
# Dispatcher — per-event-type suppression
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_suppressed_event_type_skips_dispatch():
    cfg = AlertingConfig(
        slack=_slack_cfg(),
        on_connector_recovered=False,
    )
    dispatcher = AlertDispatcher(cfg)

    with patch("httpx.AsyncClient") as mock_client_cls:
        await dispatcher.dispatch(
            AlertEventType.connector_recovered,
            connector="crm",
            datatype="contacts",
            message="all good",
        )
        # No HTTP call should be made
        mock_client_cls.assert_not_called()


@pytest.mark.anyio
async def test_sla_violation_suppressed():
    cfg = AlertingConfig(slack=_slack_cfg(), on_sla_violation=False)
    dispatcher = AlertDispatcher(cfg)

    with patch("httpx.AsyncClient") as mock_client_cls:
        await dispatcher.dispatch(
            AlertEventType.sla_violation,
            connector="crm",
            datatype="contacts",
            message="lag exceeded",
        )
        mock_client_cls.assert_not_called()


# ---------------------------------------------------------------------------
# Dispatcher — Slack channel
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_slack_dispatch_sends_post():
    cfg = AlertingConfig(slack=_slack_cfg())
    dispatcher = AlertDispatcher(cfg)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatcher.dispatch(
            AlertEventType.connector_unavailable,
            connector="hubspot",
            datatype="contacts",
            message="connection refused",
        )

    mock_client.post.assert_called_once()
    call_kwargs = mock_client.post.call_args
    payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs.get("json")
    assert payload is not None
    assert "hubspot" in payload["text"]
    assert "connector_unavailable" in payload["text"]


@pytest.mark.anyio
async def test_slack_dispatch_failure_does_not_raise():
    cfg = AlertingConfig(slack=_slack_cfg())
    dispatcher = AlertDispatcher(cfg)

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=Exception("network error"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        # Must not raise
        await dispatcher.dispatch(
            AlertEventType.connector_unavailable,
            connector="crm",
            datatype=None,
            message="down",
        )


# ---------------------------------------------------------------------------
# Dispatcher — PagerDuty channel
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_pagerduty_trigger_on_unavailable():
    cfg = AlertingConfig(pagerduty=_pd_cfg())
    dispatcher = AlertDispatcher(cfg)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatcher.dispatch(
            AlertEventType.connector_unavailable,
            connector="sf",
            datatype="accounts",
            message="timeout",
        )

    mock_client.post.assert_called_once()
    payload = mock_client.post.call_args.kwargs["json"]
    assert payload["event_action"] == "trigger"
    assert payload["routing_key"] == "fake-key"


@pytest.mark.anyio
async def test_pagerduty_resolve_on_recovered():
    cfg = AlertingConfig(pagerduty=_pd_cfg(), on_connector_recovered=True)
    dispatcher = AlertDispatcher(cfg)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatcher.dispatch(
            AlertEventType.connector_recovered,
            connector="sf",
            datatype="accounts",
            message="back online",
        )

    payload = mock_client.post.call_args.kwargs["json"]
    assert payload["event_action"] == "resolve"


# ---------------------------------------------------------------------------
# Dispatcher — generic webhook channel
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_webhook_dispatch_sends_event_type():
    cfg = AlertingConfig(webhook=_webhook_cfg())
    dispatcher = AlertDispatcher(cfg)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatcher.dispatch(
            AlertEventType.circuit_breaker_open,
            connector="crm",
            datatype="leads",
            message="too many errors",
            detail={"error_count": 10},
        )

    mock_client.request.assert_called_once()
    payload = mock_client.request.call_args.kwargs["json"]
    assert payload["event_type"] == "circuit_breaker_open"
    assert payload["connector"] == "crm"
    assert payload["detail"]["error_count"] == 10


@pytest.mark.anyio
async def test_webhook_dispatch_failure_does_not_raise():
    cfg = AlertingConfig(webhook=_webhook_cfg())
    dispatcher = AlertDispatcher(cfg)

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(side_effect=Exception("refused"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatcher.dispatch(
            AlertEventType.sla_violation,
            connector="crm",
            datatype=None,
            message="lag > 5m",
        )


# ---------------------------------------------------------------------------
# Multiple channels fire simultaneously
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_all_channels_dispatched():
    cfg = AlertingConfig(
        slack=_slack_cfg(),
        pagerduty=_pd_cfg(),
        webhook=_webhook_cfg(),
    )
    dispatcher = AlertDispatcher(cfg)

    post_calls = []
    request_calls = []

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response, side_effect=lambda *a, **kw: post_calls.append(kw) or mock_response)
    mock_client.request = AsyncMock(return_value=mock_response, side_effect=lambda *a, **kw: request_calls.append(kw) or mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.alerting.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatcher.dispatch(
            AlertEventType.connector_unavailable,
            connector="crm",
            datatype="contacts",
            message="down",
        )

    # Slack + PagerDuty each call .post; webhook calls .request
    assert mock_client.post.call_count == 2
    assert mock_client.request.call_count == 1
