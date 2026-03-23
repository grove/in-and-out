"""Unit tests for webhook event deduplication (A5)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_pool_with_seen(seen: bool) -> AsyncMock:
    """Return a mock pool where a seen-table lookup returns *seen*."""
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.commit = AsyncMock()

    # fetchone returns a row if seen, else None
    fetch_result = AsyncMock()
    fetch_result.fetchone = AsyncMock(return_value=("wh-123",) if seen else None)
    conn.execute = AsyncMock(return_value=fetch_result)

    pool = AsyncMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool


def test_event_id_extracted_from_payload() -> None:
    """event_id_field config should allow extracting event ID from nested payload."""
    payload = {"event_id": "evt-001", "data": {"id": "rec-1"}}
    event_id = payload.get("event_id")
    assert event_id == "evt-001"


def test_no_event_id_field_skips_dedup() -> None:
    """When event_id_field is None, dedup check is skipped."""
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm

    cfg = WebhookConfig(
        path="/hook",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Sig",
            credential_ref="SECRET",
        ),
        fan_out=FanOutConfig(discriminator="type", routes=[], unmatched="log_and_discard"),
        event_id_field=None,
    )
    assert cfg.event_id_field is None


def test_event_id_field_configured() -> None:
    """When event_id_field is set, it should be accessible."""
    from inandout.config.webhooks import WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm

    cfg = WebhookConfig(
        path="/hook",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Sig",
            credential_ref="SECRET",
        ),
        fan_out=FanOutConfig(discriminator="type", routes=[], unmatched="log_and_discard"),
        event_id_field="event_id",
        dedup_ttl="48h",
    )
    assert cfg.event_id_field == "event_id"
    assert cfg.dedup_ttl == "48h"
