"""Unit tests for _route_event in ingestion/webhooks.py."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inandout.ingestion.webhooks import _route_event
from inandout.config.webhooks import FanOutConfig, FanOutRoute, UnmatchedAction, WebhookConfig


def _make_fan_out(discriminator: str, routes: list[dict], unmatched: str = "log_and_discard") -> FanOutConfig:
    return FanOutConfig(
        discriminator=discriminator,
        routes=[FanOutRoute(**r) for r in routes],
        unmatched=unmatched,
    )


def _make_webhook_cfg(fan_out: FanOutConfig) -> MagicMock:
    wh = MagicMock()
    wh.fan_out = fan_out
    return wh


def test_exact_match_returns_datatype():
    fan_out = _make_fan_out("type", [{"match": "contact.created", "datatype": "contacts"}])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"type": "contact.created"})
    assert result == "contacts"


def test_startswith_match_returns_datatype():
    fan_out = _make_fan_out("type", [{"match": "contact.", "datatype": "contacts"}])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"type": "contact.updated"})
    assert result == "contacts"


def test_no_match_returns_none():
    fan_out = _make_fan_out("type", [{"match": "deal.created", "datatype": "deals"}])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"type": "contact.created"})
    assert result is None


def test_empty_routes_returns_none():
    fan_out = _make_fan_out("type", [])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"type": "anything"})
    assert result is None


def test_missing_discriminator_field_returns_none():
    fan_out = _make_fan_out("type", [{"match": "contact.created", "datatype": "contacts"}])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"other_field": "contact.created"})
    # Empty string won't match "contact.created"
    assert result is None


def test_first_matching_route_wins():
    fan_out = _make_fan_out("event", [
        {"match": "order.", "datatype": "orders"},
        {"match": "order.paid", "datatype": "payments"},
    ])
    wh = _make_webhook_cfg(fan_out)
    # "order.paid" starts with "order." → first route wins
    result = _route_event(wh, {"event": "order.paid"})
    assert result == "orders"


def test_multiple_routes_second_matches():
    fan_out = _make_fan_out("event", [
        {"match": "contact.", "datatype": "contacts"},
        {"match": "deal.", "datatype": "deals"},
    ])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"event": "deal.created"})
    assert result == "deals"


def test_discriminator_value_coerced_to_string():
    # Numeric discriminator value should still match via str()
    fan_out = _make_fan_out("event_type", [{"match": "42", "datatype": "events"}])
    wh = _make_webhook_cfg(fan_out)
    result = _route_event(wh, {"event_type": 42})
    assert result == "events"
