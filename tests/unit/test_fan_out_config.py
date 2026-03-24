"""Unit tests for FanOutRoute and FanOutConfig Pydantic models."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.webhooks import FanOutConfig, FanOutRoute, UnmatchedAction


# --- FanOutRoute ---

def test_fan_out_route_minimal():
    route = FanOutRoute(match="contact.created", datatype="contacts")
    assert route.match == "contact.created"
    assert route.datatype == "contacts"


def test_fan_out_route_notification_only_default_false():
    route = FanOutRoute(match="x", datatype="y")
    assert route.notification_only is False


def test_fan_out_route_notification_only_true():
    route = FanOutRoute(match="x", datatype="y", notification_only=True)
    assert route.notification_only is True


def test_fan_out_route_extra_field_allowed():
    # FanOutRoute uses extra="allow"
    route = FanOutRoute(match="x", datatype="y", custom_field="extra")
    assert route.custom_field == "extra"  # type: ignore[attr-defined]


def test_fan_out_route_missing_match_raises():
    with pytest.raises(ValidationError):
        FanOutRoute(datatype="contacts")


def test_fan_out_route_missing_datatype_raises():
    with pytest.raises(ValidationError):
        FanOutRoute(match="contact.created")


def test_fan_out_route_notification_external_id_field_default():
    route = FanOutRoute(match="x", datatype="y")
    assert route.notification_external_id_field == "id"


def test_fan_out_route_custom_external_id_field():
    route = FanOutRoute(match="x", datatype="y", notification_external_id_field="uuid")
    assert route.notification_external_id_field == "uuid"


# --- FanOutConfig ---

def test_fan_out_config_minimal():
    cfg = FanOutConfig(discriminator="type", unmatched="log_and_discard", routes=[])
    assert cfg.discriminator == "type"


def test_fan_out_config_routes_default_empty():
    cfg = FanOutConfig(discriminator="type", unmatched="log_and_discard")
    assert cfg.routes == []


def test_fan_out_config_routes_populated():
    route = FanOutRoute(match="x", datatype="y")
    cfg = FanOutConfig(discriminator="type", unmatched="log_and_discard", routes=[route])
    assert len(cfg.routes) == 1
    assert cfg.routes[0].match == "x"


def test_fan_out_config_extra_field_forbidden():
    with pytest.raises(ValidationError):
        FanOutConfig(discriminator="type", unmatched="log_and_discard", extra_field="bad")


def test_fan_out_config_missing_discriminator_raises():
    with pytest.raises(ValidationError):
        FanOutConfig(unmatched="log_and_discard")


def test_fan_out_config_unmatched_log_and_discard():
    cfg = FanOutConfig(discriminator="type", unmatched="log_and_discard")
    assert cfg.unmatched == UnmatchedAction.log_and_discard


def test_fan_out_config_unmatched_reject_400():
    cfg = FanOutConfig(discriminator="type", unmatched="reject_400")
    assert cfg.unmatched == UnmatchedAction.reject_400


def test_fan_out_config_invalid_unmatched_raises():
    with pytest.raises(ValidationError):
        FanOutConfig(discriminator="type", unmatched="invalid_action")
