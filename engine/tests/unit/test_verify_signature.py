"""Unit tests for _verify_signature in ingestion/webhooks.py."""
from __future__ import annotations

import hashlib
import hmac as _hmac
from unittest.mock import patch

import pytest

from inandout.ingestion.webhooks import _verify_signature
from inandout.config.webhooks import SignatureAlgorithm


def _make_sig_cfg(
    header: str = "x-hub-signature-256",
    algorithm: SignatureAlgorithm = SignatureAlgorithm.hmac_sha256,
    credential_ref: str = "MY_SECRET",
    version: str | None = None,
):
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.header = header
    cfg.algorithm = algorithm
    cfg.credential_ref = credential_ref
    cfg.version = version
    return cfg


def _make_webhook_cfg(sig_cfg):
    from unittest.mock import MagicMock
    wh = MagicMock()
    wh.signature = sig_cfg
    return wh


SECRET = "test-webhook-secret"
BODY = b'{"type":"test"}'


def _sha256_hex(secret: str, body: bytes) -> str:
    return _hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _sha1_hex(secret: str, body: bytes) -> str:
    return _hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()


def _with_resolved_secret(secret: str):
    return patch("inandout.ingestion.webhooks.resolve_credential", return_value=secret)


# --- HMAC-SHA256 ---

def test_verify_signature_sha256_valid():
    sig = _sha256_hex(SECRET, BODY)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"x-hub-signature-256": sig}
    with _with_resolved_secret(SECRET):
        assert _verify_signature(wh, BODY, headers) is True


def test_verify_signature_sha256_wrong_secret():
    sig = _sha256_hex("wrong-secret", BODY)
    sig_cfg = _make_sig_cfg()
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"x-hub-signature-256": sig}
    with _with_resolved_secret(SECRET):
        assert _verify_signature(wh, BODY, headers) is False


def test_verify_signature_missing_header_returns_false():
    sig_cfg = _make_sig_cfg(header="x-sig")
    wh = _make_webhook_cfg(sig_cfg)
    headers = {}  # header absent
    with _with_resolved_secret(SECRET):
        assert _verify_signature(wh, BODY, headers) is False


# --- HMAC-SHA1 ---

def test_verify_signature_sha1_valid():
    sig = "sha1=" + _sha1_hex(SECRET, BODY)
    sig_cfg = _make_sig_cfg(
        header="x-hub-signature",
        algorithm=SignatureAlgorithm.hmac_sha1,
    )
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"x-hub-signature": sig}
    with _with_resolved_secret(SECRET):
        assert _verify_signature(wh, BODY, headers) is True


def test_verify_signature_sha1_without_prefix_valid():
    sig = _sha1_hex(SECRET, BODY)
    sig_cfg = _make_sig_cfg(
        header="x-hub-signature",
        algorithm=SignatureAlgorithm.hmac_sha1,
    )
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"x-hub-signature": sig}
    with _with_resolved_secret(SECRET):
        assert _verify_signature(wh, BODY, headers) is True


def test_verify_signature_sha1_tampered_body():
    sig = "sha1=" + _sha1_hex(SECRET, BODY)
    sig_cfg = _make_sig_cfg(
        header="x-hub-signature",
        algorithm=SignatureAlgorithm.hmac_sha1,
    )
    wh = _make_webhook_cfg(sig_cfg)
    headers = {"x-hub-signature": sig}
    with _with_resolved_secret(SECRET):
        assert _verify_signature(wh, BODY + b"extra", headers) is False
