"""Unit tests for webhook secret rotation support."""
from __future__ import annotations

import pytest


def test_signature_config_rotation_fields():
    """SignatureConfig should support rotation_credential_ref and rotation_grace_period."""
    from inandout.config.webhooks import SignatureConfig, SignatureAlgorithm
    
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Webhook-Signature",
        credential_ref="webhook_secret",
        rotation_credential_ref="webhook_secret_new",
        rotation_grace_period="2h",
    )
    assert cfg.rotation_credential_ref == "webhook_secret_new"
    assert cfg.rotation_grace_period == "2h"


def test_signature_config_rotation_defaults():
    """Rotation fields should have sensible defaults."""
    from inandout.config.webhooks import SignatureConfig, SignatureAlgorithm
    
    cfg = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Webhook-Signature",
        credential_ref="webhook_secret",
    )
    assert cfg.rotation_credential_ref is None
    assert cfg.rotation_grace_period == "1h"


def test_webhook_signature_verification_with_rotation():
    """Signature verification should accept both primary and rotation secrets."""
    # This is an integration concept test
    # The actual implementation in _verify_signature handles rotation
    
    from inandout.ingestion.webhooks import _verify_signature
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, SignatureAlgorithm, FanOutConfig, UnmatchedAction
    from unittest.mock import patch
    import hmac
    import hashlib
    
    body = b'{"event": "user.created", "id": "123"}'
    primary_secret = "old_secret"
    rotation_secret = "new_secret"
    
    # Create signature with rotation secret
    sig = hmac.new(rotation_secret.encode(), body, hashlib.sha256).hexdigest()
    headers = {"x-signature": sig}
    
    webhook_cfg = WebhookConfig(
        path="/webhook",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Signature",
            credential_ref="primary",
            rotation_credential_ref="rotation",
        ),
        fan_out=FanOutConfig(
            discriminator="event",
            routes=[],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )
    
    # Mock credential resolution
    def mock_resolve(ref: str) -> str:
        if ref == "primary":
            return primary_secret
        elif ref == "rotation":
            return rotation_secret
        raise ValueError(f"Unknown ref: {ref}")
    
    with patch("inandout.ingestion.webhooks.resolve_credential", side_effect=mock_resolve):
        # Should accept signature from rotation secret during grace period
        result = _verify_signature(webhook_cfg, body, headers)
        assert result is True


def test_webhook_signature_primary_still_works():
    """Primary secret should continue to work during rotation."""
    from inandout.ingestion.webhooks import _verify_signature
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, SignatureAlgorithm, FanOutConfig, UnmatchedAction
    from unittest.mock import patch
    import hmac
    import hashlib
    
    body = b'{"event": "user.created", "id": "123"}'
    primary_secret = "old_secret"
    
    # Create signature with primary secret
    sig = hmac.new(primary_secret.encode(), body, hashlib.sha256).hexdigest()
    headers = {"x-signature": sig}
    
    webhook_cfg = WebhookConfig(
        path="/webhook",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Signature",
            credential_ref="primary",
            rotation_credential_ref="rotation",
        ),
        fan_out=FanOutConfig(
            discriminator="event",
            routes=[],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )
    
    def mock_resolve(ref: str) -> str:
        if ref == "primary":
            return primary_secret
        elif ref == "rotation":
            return "new_secret"
        raise ValueError(f"Unknown ref: {ref}")
    
    with patch("inandout.ingestion.webhooks.resolve_credential", side_effect=mock_resolve):
        # Should accept signature from primary secret
        result = _verify_signature(webhook_cfg, body, headers)
        assert result is True
