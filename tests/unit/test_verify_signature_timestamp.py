"""Unit tests for _verify_signature with Stripe-style timestamp header."""
from __future__ import annotations

import hashlib
import hmac as _hmac
import time
from unittest.mock import patch

import pytest

from inandout.ingestion.webhooks import _verify_signature, _MAX_TIMESTAMP_SKEW_SECS
from inandout.config.webhooks import SignatureAlgorithm


SECRET = "stripe-style-secret"
BODY = b'{"type":"charge.succeeded"}'


def _stripe_sign(secret: str, body: bytes, ts: int) -> str:
    payload = f"{ts}.".encode() + body
    return _hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def _make_stripe_header(ts: int, sig: str) -> str:
    return f"t={ts},v1={sig}"


def _make_sig_cfg(version: str = "v1"):
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.header = "stripe-signature"
    cfg.algorithm = SignatureAlgorithm.hmac_sha256
    cfg.credential_ref = "MY_SECRET"
    cfg.version = version
    return cfg


def _make_webhook_cfg(sig_cfg):
    from unittest.mock import MagicMock
    wh = MagicMock()
    wh.signature = sig_cfg
    return wh


def _patched_secret(secret: str):
    return patch("inandout.ingestion.webhooks.resolve_credential", return_value=secret)


# --- _MAX_TIMESTAMP_SKEW_SECS constant ---

def test_max_skew_is_300():
    assert _MAX_TIMESTAMP_SKEW_SECS == 300


# --- Valid Stripe-style header (within skew window) ---

def test_stripe_header_valid_recent_timestamp():
    now = int(time.time())
    sig = _stripe_sign(SECRET, BODY, now)
    header = _make_stripe_header(now, sig)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"stripe-signature": header}
    with _patched_secret(SECRET):
        with patch("inandout.ingestion.webhooks.time") as mock_time:
            mock_time.time.return_value = float(now)
            assert _verify_signature(wh, BODY, headers) is True


def test_stripe_header_valid_at_skew_boundary():
    now = 1700000000
    sig = _stripe_sign(SECRET, BODY, now)
    header = _make_stripe_header(now, sig)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"stripe-signature": header}
    with _patched_secret(SECRET):
        with patch("inandout.ingestion.webhooks.time") as mock_time:
            # Exactly at boundary (300 seconds) should still be accepted
            mock_time.time.return_value = float(now + _MAX_TIMESTAMP_SKEW_SECS)
            assert _verify_signature(wh, BODY, headers) is True


# --- Timestamp too old ---

def test_stripe_header_rejected_when_timestamp_too_old():
    now = 1700000000
    sig = _stripe_sign(SECRET, BODY, now)
    header = _make_stripe_header(now, sig)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"stripe-signature": header}
    with _patched_secret(SECRET):
        with patch("inandout.ingestion.webhooks.time") as mock_time:
            # 301 seconds in the future → exceeds skew
            mock_time.time.return_value = float(now + _MAX_TIMESTAMP_SKEW_SECS + 1)
            assert _verify_signature(wh, BODY, headers) is False


def test_stripe_header_rejected_when_timestamp_in_future():
    now = 1700000000
    sig = _stripe_sign(SECRET, BODY, now)
    header = _make_stripe_header(now, sig)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"stripe-signature": header}
    with _patched_secret(SECRET):
        with patch("inandout.ingestion.webhooks.time") as mock_time:
            # Timestamp far in the future
            mock_time.time.return_value = float(now - _MAX_TIMESTAMP_SKEW_SECS - 1)
            assert _verify_signature(wh, BODY, headers) is False


# --- Wrong secret still fails after timestamp check passes ---

def test_stripe_header_wrong_secret_rejected():
    now = int(time.time())
    sig = _stripe_sign("other-secret", BODY, now)
    header = _make_stripe_header(now, sig)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"stripe-signature": header}
    with _patched_secret(SECRET):
        with patch("inandout.ingestion.webhooks.time") as mock_time:
            mock_time.time.return_value = float(now)
            assert _verify_signature(wh, BODY, headers) is False
