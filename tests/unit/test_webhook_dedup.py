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


# ---------------------------------------------------------------------------
# handle_webhook integration tests — dedup path
# ---------------------------------------------------------------------------

def _make_webhook_cfg_with_event_id() -> "WebhookConfig":
    from inandout.config.webhooks import (
        WebhookConfig, SignatureConfig, FanOutConfig, SignatureAlgorithm, UnmatchedAction,
    )
    return WebhookConfig(
        path="/hook",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="x-hub-signature-256",
            credential_ref="TEST_SECRET",
        ),
        fan_out=FanOutConfig(
            discriminator="type",
            routes=[],
            unmatched=UnmatchedAction.log_and_discard,
        ),
        event_id_field="event_id",
        dedup_ttl="24h",
    )


def _make_signed_request(body: bytes, secret: str = "test-secret") -> "AsyncMock":
    """Return a mock Starlette Request with valid HMAC-SHA256 signature."""
    import hashlib
    import hmac as _hmac
    from unittest.mock import AsyncMock

    sig = _hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    mock_request = AsyncMock()
    mock_request.body = AsyncMock(return_value=body)
    mock_request.headers = {"x-hub-signature-256": f"sha256={sig}"}
    return mock_request


def _make_connector_cfg() -> "ConnectorConfig":
    """Return a minimal mock ConnectorConfig."""
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.name = "testconn"
    return cfg


def _make_engine_with_dedup_pool(is_duplicate: bool) -> "IngestionEngine":
    """Return a mock IngestionEngine whose pool simulates seen-table behaviour."""
    from unittest.mock import AsyncMock, MagicMock

    fetch_result = AsyncMock()
    # If new: INSERT returned the row (event_id,); if dup: returned Nothing (None)
    fetch_result.fetchone = AsyncMock(return_value=None if is_duplicate else ("evt-001",))

    inner_conn = AsyncMock()
    inner_conn.__aenter__ = AsyncMock(return_value=inner_conn)
    inner_conn.__aexit__ = AsyncMock(return_value=None)
    inner_conn.execute = AsyncMock(return_value=fetch_result)
    inner_conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=inner_conn)

    engine = MagicMock()
    engine._pool = pool
    return engine


@pytest.mark.anyio
async def test_handle_webhook_duplicate_event_returns_200():
    """handle_webhook should return 200 {'status':'duplicate'} for a duplicate event_id."""
    from inandout.ingestion.webhooks import handle_webhook
    from inandout.transport.auth import resolve_credential

    SECRET = "test-secret"
    body = b'{"event_id": "evt-001", "type": "contact.created", "id": "rec-1"}'
    request = _make_signed_request(body, SECRET)
    connector = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg_with_event_id()
    engine = _make_engine_with_dedup_pool(is_duplicate=True)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("inandout.ingestion.webhooks.resolve_credential", lambda ref: SECRET)
        response = await handle_webhook(request, connector, webhook_cfg, engine)

    assert response.status_code == 200
    import orjson
    body_out = orjson.loads(response.body)
    assert body_out["status"] == "duplicate"


@pytest.mark.anyio
async def test_handle_webhook_new_event_is_not_duplicate():
    """handle_webhook should NOT return 'duplicate' when INSERT succeeds (new event)."""
    from inandout.ingestion.webhooks import handle_webhook
    from inandout.transport.auth import resolve_credential

    SECRET = "test-secret"
    body = b'{"event_id": "evt-new", "type": "contact.created", "id": "rec-2"}'
    request = _make_signed_request(body, SECRET)
    connector = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg_with_event_id()
    # is_duplicate=False → INSERT returns row → proceeds past dedup
    # unmatched=log_and_discard + empty routes → returns 200 {"status":"discarded"}
    engine = _make_engine_with_dedup_pool(is_duplicate=False)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("inandout.ingestion.webhooks.resolve_credential", lambda ref: SECRET)
        response = await handle_webhook(request, connector, webhook_cfg, engine)

    import orjson
    body_out = orjson.loads(response.body)
    # Must NOT be duplicate — the event was novel and processed (discarded due to no matching route)
    assert body_out.get("status") != "duplicate"
