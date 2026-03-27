"""Unit tests for webhook signature verification and fan-out routing."""
from __future__ import annotations

import hashlib
import hmac
import time

import pytest

from inandout.config.webhooks import (
    FanOutConfig,
    FanOutRoute,
    SignatureAlgorithm,
    SignatureConfig,
    UnmatchedAction,
    WebhookConfig,
)
from inandout.ingestion.webhooks import _verify_hmac_sha256, _verify_signature, _route_event


# ---------------------------------------------------------------------------
# _verify_hmac_sha256
# ---------------------------------------------------------------------------

def test_hmac_sha256_plain_match():
    body = b'{"event":"contact.created"}'
    secret = "supersecret"
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert _verify_hmac_sha256(secret, body, sig) is True


def test_hmac_sha256_plain_mismatch():
    body = b'{"event":"contact.created"}'
    assert _verify_hmac_sha256("supersecret", body, "deadbeef") is False


def test_hmac_sha256_with_v1_prefix():
    body = b"payload"
    secret = "s3cr3t"
    raw_sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert _verify_hmac_sha256(secret, body, f"v1={raw_sig}") is True


def test_hmac_sha256_with_sha256_prefix():
    body = b"payload"
    secret = "s3cr3t"
    raw_sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert _verify_hmac_sha256(secret, body, f"sha256={raw_sig}") is True


def test_hmac_sha256_timestamp_binding():
    body = b"body_content"
    secret = "ts_secret"
    ts = "1234567890"
    payload = f"{ts}.".encode() + body
    raw_sig = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    assert _verify_hmac_sha256(secret, body, raw_sig, timestamp=ts) is True


# ---------------------------------------------------------------------------
# _verify_signature via WebhookConfig
# ---------------------------------------------------------------------------

def _make_webhook_cfg(algorithm: SignatureAlgorithm = SignatureAlgorithm.hmac_sha256) -> WebhookConfig:
    return WebhookConfig(
        path="/webhooks/test",
        signature=SignatureConfig(
            algorithm=algorithm,
            header="X-Hub-Signature-256",
            credential_ref="webhook_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event",
            routes=[FanOutRoute(match="contact.created", datatype="contacts")],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )


def test_verify_signature_valid(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "mysecret")
    cfg = _make_webhook_cfg()
    body = b'{"event":"contact.created"}'
    sig = hmac.new(b"mysecret", body, hashlib.sha256).hexdigest()
    headers = {"x-hub-signature-256": sig}
    assert _verify_signature(cfg, body, headers) is True


def test_verify_signature_missing_header(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "mysecret")
    cfg = _make_webhook_cfg()
    assert _verify_signature(cfg, b"body", {}) is False


def test_verify_signature_wrong_sig(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "mysecret")
    cfg = _make_webhook_cfg()
    headers = {"x-hub-signature-256": "wrongsig"}
    assert _verify_signature(cfg, b"body", headers) is False


def test_verify_signature_stripe_style_valid(monkeypatch):
    """Stripe-style: header is 't=<ts>,v1=<sig>' with timestamp binding."""
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "stripe_secret")
    cfg = WebhookConfig(
        path="/webhooks/stripe",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="Stripe-Signature",
            credential_ref="webhook_secret",
            version="v1",
        ),
        fan_out=FanOutConfig(
            discriminator="type",
            routes=[],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )
    body = b'{"type":"charge.succeeded"}'
    ts = str(int(time.time()))
    payload = f"{ts}.".encode() + body
    raw_sig = hmac.new(b"stripe_secret", payload, hashlib.sha256).hexdigest()
    header_val = f"t={ts},v1={raw_sig}"
    assert _verify_signature(cfg, body, {"stripe-signature": header_val}) is True


def test_verify_signature_stripe_old_timestamp_rejected(monkeypatch):
    """Stripe-style: timestamps older than 5 min should be rejected."""
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "stripe_secret")
    cfg = WebhookConfig(
        path="/webhooks/stripe",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="Stripe-Signature",
            credential_ref="webhook_secret",
            version="v1",
        ),
        fan_out=FanOutConfig(
            discriminator="type",
            routes=[],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )
    body = b'{"type":"charge.succeeded"}'
    old_ts = str(int(time.time()) - 400)  # 400 s ago — exceeds 300 s limit
    payload = f"{old_ts}.".encode() + body
    raw_sig = hmac.new(b"stripe_secret", payload, hashlib.sha256).hexdigest()
    header_val = f"t={old_ts},v1={raw_sig}"
    assert _verify_signature(cfg, body, {"stripe-signature": header_val}) is False


# ---------------------------------------------------------------------------
# _route_event
# ---------------------------------------------------------------------------

def _make_fan_out() -> WebhookConfig:
    return WebhookConfig(
        path="/hooks",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Sig",
            credential_ref="s",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[
                FanOutRoute(match="contact.created", datatype="contacts"),
                FanOutRoute(match="deal.updated", datatype="deals"),
            ],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )


def test_route_event_match():
    cfg = _make_fan_out()
    assert _route_event(cfg, {"event_type": "contact.created"}) == "contacts"
    assert _route_event(cfg, {"event_type": "deal.updated"}) == "deals"


def test_route_event_unmatched():
    cfg = _make_fan_out()
    assert _route_event(cfg, {"event_type": "unknown.event"}) is None


def test_route_event_prefix_match():
    """Routes match by prefix if exact match fails."""
    cfg = _make_fan_out()
    # "contact.created.sub" should match "contact.created" prefix
    assert _route_event(cfg, {"event_type": "contact.created.sub"}) == "contacts"


def test_route_event_missing_discriminator():
    cfg = _make_fan_out()
    assert _route_event(cfg, {}) is None


# ---------------------------------------------------------------------------
# null_record_field delete detection logic
# ---------------------------------------------------------------------------

def _resolve_null_field(matched_route: FanOutRoute | None, payload: dict) -> str | None:
    """Mirror the null_field resolution logic from handle_webhook (FEAT-WH-04)."""
    if matched_route is not None and matched_route.null_record_field is not None:
        return matched_route.null_record_field
    if matched_route is None and "value" in payload:
        return "value"
    return None


def test_null_record_field_route_configured():
    route = FanOutRoute(match="customer.delete", datatype="customers", null_record_field="value")
    payload = {"id": 1001, "value": None}
    null_field = _resolve_null_field(route, payload)
    assert null_field == "value"
    assert null_field in payload and payload[null_field] is None


def test_null_record_field_not_null_no_delete():
    """Configured null_record_field but value is a dict — should NOT trigger delete."""
    route = FanOutRoute(match="customer.update", datatype="customers", null_record_field="value")
    payload = {"id": 1001, "value": {"name": "Acme"}}
    null_field = _resolve_null_field(route, payload)
    assert null_field == "value"
    # value is present but NOT null — delete condition is false
    assert not (null_field in payload and payload[null_field] is None)


def test_null_record_field_arbitrary_name():
    route = FanOutRoute(match="order.deleted", datatype="orders", null_record_field="object")
    payload = {"id": 42, "object": None}
    null_field = _resolve_null_field(route, payload)
    assert null_field == "object"
    assert null_field in payload and payload[null_field] is None


def test_null_record_field_not_set_no_delete():
    """Route has no null_record_field — even a null 'value' key must NOT trigger delete."""
    route = FanOutRoute(match="customer.update", datatype="customers")  # no null_record_field
    payload = {"id": 1001, "value": None}
    null_field = _resolve_null_field(route, payload)
    # Route is matched but null_record_field is None → null_field is None
    assert null_field is None


def test_null_record_field_legacy_fallback_no_route():
    """When no route is matched and payload has 'value': null, legacy path triggers."""
    payload = {"id": 1001, "value": None}
    null_field = _resolve_null_field(None, payload)
    assert null_field == "value"


def test_null_record_field_legacy_fallback_absent_key():
    """When no route is matched and payload has no 'value' key, no delete triggered."""
    payload = {"id": 1001, "data": None}
    null_field = _resolve_null_field(None, payload)
    assert null_field is None
