"""Unit tests for WebhookConfig Pydantic model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.webhooks import (
    FanOutConfig,
    FanOutRoute,
    SignatureAlgorithm,
    SignatureConfig,
    WebhookConfig,
    WebhookRegistrationConfig,
)


def _make_sig() -> SignatureConfig:
    return SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Sig",
        credential_ref="KEY",
    )


def _make_fan_out() -> FanOutConfig:
    return FanOutConfig(
        discriminator="type",
        routes=[],
        unmatched="log_and_discard",
    )


# --- Required fields ---


def test_minimal_valid():
    cfg = WebhookConfig(path="/webhook", signature=_make_sig(), fan_out=_make_fan_out())
    assert cfg.path == "/webhook"


def test_missing_path_raises():
    with pytest.raises(ValidationError):
        WebhookConfig(signature=_make_sig(), fan_out=_make_fan_out())


def test_missing_signature_allowed():
    # signature is optional — connectors may use auth_header_name instead (e.g. Tripletex) 22b9bd0 (feat(webhooks): implement registration-based webhook support)
    cfg = WebhookConfig(path="/webhook", fan_out=_make_fan_out())
    assert cfg.signature is None


def test_missing_fan_out_allowed():
    # fan_out is optional — fire-and-forget notification connectors don't need it 22b9bd0 (feat(webhooks): implement registration-based webhook support)
    cfg = WebhookConfig(path="/webhook", signature=_make_sig())
    assert cfg.fan_out is None


# --- Optional fields ---


def test_registration_default_none():
    cfg = WebhookConfig(path="/webhook", signature=_make_sig(), fan_out=_make_fan_out())
    assert cfg.registration is None


def test_event_id_field_default_none():
    cfg = WebhookConfig(path="/webhook", signature=_make_sig(), fan_out=_make_fan_out())
    assert cfg.event_id_field is None


def test_dedup_ttl_default():
    cfg = WebhookConfig(path="/webhook", signature=_make_sig(), fan_out=_make_fan_out())
    assert cfg.dedup_ttl == "24h"


def test_event_id_field_set():
    cfg = WebhookConfig(
        path="/webhook",
        signature=_make_sig(),
        fan_out=_make_fan_out(),
        event_id_field="event_id",
    )
    assert cfg.event_id_field == "event_id"


def test_registration_set():
    reg = WebhookRegistrationConfig(register_path="/webhooks")
    cfg = WebhookConfig(
        path="/webhook",
        signature=_make_sig(),
        fan_out=_make_fan_out(),
        registration=reg,
    )
    assert cfg.registration.register_path == "/webhooks"


def test_extra_fields_allowed():
    # WebhookConfig uses extra="allow"
    cfg = WebhookConfig(
        path="/webhook",
        signature=_make_sig(),
        fan_out=_make_fan_out(),
        custom_extra="ok",
    )
    assert cfg.custom_extra == "ok"  # type: ignore[attr-defined]


def test_fan_out_stored():
    fan_out = _make_fan_out()
    cfg = WebhookConfig(path="/webhook", signature=_make_sig(), fan_out=fan_out)
    assert cfg.fan_out.discriminator == "type"


def test_custom_dedup_ttl():
    cfg = WebhookConfig(
        path="/webhook",
        signature=_make_sig(),
        fan_out=_make_fan_out(),
        dedup_ttl="48h",
    )
    assert cfg.dedup_ttl == "48h"


# --- FanOutRoute.null_record_field ---


def test_fan_out_route_null_record_field_defaults_none():
    route = FanOutRoute(match="customer.delete", datatype="customers")
    assert route.null_record_field is None


def test_fan_out_route_null_record_field_set():
    route = FanOutRoute(match="customer.delete", datatype="customers", null_record_field="value")
    assert route.null_record_field == "value"


def test_fan_out_route_null_record_field_arbitrary_name():
    route = FanOutRoute(match="order.deleted", datatype="orders", null_record_field="object")
    assert route.null_record_field == "object"


def test_fan_out_route_null_record_field_in_fan_out_config():
    fan_out = FanOutConfig(
        discriminator="event",
        routes=[
            FanOutRoute(match="customer.delete", datatype="customers", null_record_field="value"),
            FanOutRoute(match="customer.created", datatype="customers"),
        ],
        unmatched="log_and_discard",
    )
    delete_route = fan_out.routes[0]
    create_route = fan_out.routes[1]
    assert delete_route.null_record_field == "value"
    assert create_route.null_record_field is None
