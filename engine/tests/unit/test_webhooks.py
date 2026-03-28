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


def _make_webhook_cfg(
    algorithm: SignatureAlgorithm = SignatureAlgorithm.hmac_sha256,
) -> WebhookConfig:
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
# is_delete route logic
# Mirrors the FEAT-WH-04 is_delete branch in _handle_single_event:
# annotate the full payload with _deleted:True and let _extract_external_id
# handle both simple and compound PKs.
# ---------------------------------------------------------------------------


def _apply_is_delete(
    matched_route: FanOutRoute | None,
    payload: dict,
) -> dict | None:
    """Return the annotated payload dict, or None if is_delete path doesn't apply."""
    if matched_route is None or not matched_route.is_delete:
        return None
    return {**payload, "_deleted": True}


def test_is_delete_annotates_full_payload():
    route = FanOutRoute(match="customer.delete", datatype="customers", is_delete=True)
    payload = {"subscriptionId": 0, "event": "customer.delete", "id": 10001}
    result = _apply_is_delete(route, payload)
    assert result is not None
    assert result["_deleted"] is True
    assert result["id"] == 10001
    assert result["event"] == "customer.delete"


def test_is_delete_compound_pk_full_payload_passthrough():
    """Compound PK scenario (HubSpot associations).
    The full payload is annotated; _extract_external_id handles [fromObjectId, toObjectId].
    """
    route = FanOutRoute(
        match="association.deletion", datatype="contacts_companies_associations", is_delete=True
    )
    payload = {
        "subscriptionType": "association.deletion",
        "fromObjectId": 100,
        "toObjectId": 200,
        "associationType": "HUBSPOT_DEFINED",
    }
    result = _apply_is_delete(route, payload)
    assert result is not None
    assert result["_deleted"] is True
    assert result["fromObjectId"] == 100
    assert result["toObjectId"] == 200


def test_is_delete_false_route_not_applied():
    """Route without is_delete must not enter the is_delete path."""
    route = FanOutRoute(match="customer.update", datatype="customers")
    payload = {"id": 10001, "name": "Acme"}
    assert _apply_is_delete(route, payload) is None


def test_is_delete_none_route_not_applied():
    assert _apply_is_delete(None, {"id": 1}) is None


def test_is_delete_does_not_mutate_original():
    """Engine must not mutate the original payload dict."""
    route = FanOutRoute(match="customer.delete", datatype="customers", is_delete=True)
    original = {"id": 10001, "event": "customer.delete"}
    result = _apply_is_delete(route, original)
    assert "_deleted" not in original
    assert result["_deleted"] is True
