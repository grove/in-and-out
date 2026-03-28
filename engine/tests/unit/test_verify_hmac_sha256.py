"""Unit tests for _verify_hmac_sha256 in ingestion/webhooks.py."""
from __future__ import annotations

import hashlib
import hmac as _hmac

import pytest

from inandout.ingestion.webhooks import _verify_hmac_sha256


def _sign(secret: str, body: bytes, timestamp: str | None = None) -> str:
    key = secret.encode()
    payload = body if timestamp is None else f"{timestamp}.".encode() + body
    return _hmac.new(key, payload, hashlib.sha256).hexdigest()


SECRET = "test-secret"
BODY = b'{"event":"created"}'


def test_plain_hex_sig_valid():
    sig = _sign(SECRET, BODY)
    assert _verify_hmac_sha256(SECRET, BODY, sig) is True


def test_wrong_secret_returns_false():
    sig = _sign("wrong-secret", BODY)
    assert _verify_hmac_sha256(SECRET, BODY, sig) is False


def test_tampered_body_returns_false():
    sig = _sign(SECRET, BODY)
    assert _verify_hmac_sha256(SECRET, b"tampered", sig) is False


def test_v1_prefix_stripped():
    sig = "v1=" + _sign(SECRET, BODY)
    assert _verify_hmac_sha256(SECRET, BODY, sig) is True


def test_sha256_prefix_stripped():
    sig = "sha256=" + _sign(SECRET, BODY)
    assert _verify_hmac_sha256(SECRET, BODY, sig) is True


def test_custom_version_prefix_stripped():
    sig = "myver=" + _sign(SECRET, BODY)
    assert _verify_hmac_sha256(SECRET, BODY, sig, version="myver") is True


def test_timestamp_binding_valid():
    ts = "1700000000"
    sig = _sign(SECRET, BODY, timestamp=ts)
    assert _verify_hmac_sha256(SECRET, BODY, sig, timestamp=ts) is True


def test_timestamp_binding_wrong_ts_fails():
    ts = "1700000000"
    sig = _sign(SECRET, BODY, timestamp=ts)
    assert _verify_hmac_sha256(SECRET, BODY, sig, timestamp="9999999999") is False


def test_timestamp_binding_with_v1_prefix():
    ts = "1700000000"
    sig = "v1=" + _sign(SECRET, BODY, timestamp=ts)
    assert _verify_hmac_sha256(SECRET, BODY, sig, timestamp=ts) is True


def test_empty_body_valid():
    sig = _sign(SECRET, b"")
    assert _verify_hmac_sha256(SECRET, b"", sig) is True
