"""Unit tests for full-state resolution from notification-only webhooks (T1 #8)."""
from __future__ import annotations

import hashlib
import hmac
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_webhook_cfg(notification_only: bool = False, notification_id_field: str = "id"):
    """Build a minimal WebhookConfig with optional notification_only route."""
    from inandout.config.webhooks import (
        FanOutConfig, FanOutRoute, SignatureAlgorithm, SignatureConfig, UnmatchedAction, WebhookConfig,
    )
    return WebhookConfig(
        path="/webhooks/test",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Signature",
            credential_ref="webhook_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[
                FanOutRoute(
                    match="contact.updated",
                    datatype="contacts",
                    notification_only=notification_only,
                    notification_external_id_field=notification_id_field,
                )
            ],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )


def _make_connector_cfg(datatype: str = "contacts"):
    """Build a minimal ConnectorConfig with a contacts datatype."""
    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_connector.datatypes = {
        datatype: MagicMock(
            ingestion=MagicMock(
                primary_key="id",
                webhook_events=None,  # no webhook_events config
            ),
            field_mappings=None,
            strict_field_mapping=False,
            quality_rules=None,
        )
    }
    return mock_connector


# ---------------------------------------------------------------------------
# FanOutRoute notification_only and notification_external_id_field
# ---------------------------------------------------------------------------

def test_fan_out_route_notification_only_default_false():
    """FanOutRoute.notification_only defaults to False."""
    from inandout.config.webhooks import FanOutRoute
    route = FanOutRoute(match="event", datatype="contacts")
    assert route.notification_only is False


def test_fan_out_route_notification_external_id_field_default():
    """FanOutRoute.notification_external_id_field defaults to 'id'."""
    from inandout.config.webhooks import FanOutRoute
    route = FanOutRoute(match="event", datatype="contacts")
    assert route.notification_external_id_field == "id"


def test_fan_out_route_notification_only_configurable():
    """FanOutRoute.notification_only can be set to True."""
    from inandout.config.webhooks import FanOutRoute
    route = FanOutRoute(match="event", datatype="contacts", notification_only=True,
                        notification_external_id_field="contact_id")
    assert route.notification_only is True
    assert route.notification_external_id_field == "contact_id"


# ---------------------------------------------------------------------------
# Webhook handler notification resolution
# ---------------------------------------------------------------------------

def _sign_body(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


@pytest.mark.asyncio
async def test_notification_only_with_id_calls_run_sync_single_record(monkeypatch):
    """Notification-only webhook with extractable ID → run_sync_single_record called."""
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "test_secret")

    from starlette.testclient import TestClient
    from starlette.applications import Starlette
    from starlette.routing import Route

    from inandout.ingestion.webhooks import handle_webhook
    from inandout.ingestion.engine import SyncResult

    webhook_cfg = _make_webhook_cfg(notification_only=True, notification_id_field="id")
    connector_cfg = _make_connector_cfg()

    mock_result = MagicMock(spec=SyncResult)
    mock_result.status = "completed"
    mock_result.records_inserted = 1
    mock_result.records_updated = 0

    engine = MagicMock()
    engine._pool = MagicMock()
    engine._pool.connection = MagicMock()
    engine._pool.connection.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
    engine._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    engine.run_sync_single_record = AsyncMock(return_value=mock_result)
    engine.run_sync = AsyncMock(return_value=mock_result)

    single_record_called = []

    async def fake_single(c, dt, ing, ext_id, **kwargs):
        single_record_called.append(ext_id)
        return mock_result

    engine.run_sync_single_record = fake_single

    import json
    body = json.dumps({"event_type": "contact.updated", "id": "42"}).encode()
    sig = _sign_body(body, "test_secret")

    from starlette.requests import Request
    from starlette.datastructures import Headers

    # Simulate the request
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [
            (b"x-signature", sig.encode()),
            (b"content-type", b"application/json"),
        ],
        "path": "/webhooks/test",
        "query_string": b"",
    }

    body_chunks = [body]
    chunk_idx = [0]

    async def receive():
        if chunk_idx[0] < len(body_chunks):
            data = body_chunks[chunk_idx[0]]
            chunk_idx[0] += 1
            return {"type": "http.request", "body": data, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(scope, receive)

    with patch("inandout.ingestion.webhooks._log_webhook", new_callable=AsyncMock):
        response = await handle_webhook(request, connector_cfg, webhook_cfg, engine)

    assert response.status_code == 200
    assert single_record_called == ["42"]


@pytest.mark.asyncio
async def test_notification_only_without_id_falls_back_to_full_sync(monkeypatch):
    """Notification-only webhook without ID → full run_sync called."""
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "test_secret")

    from inandout.ingestion.webhooks import handle_webhook
    from inandout.ingestion.engine import SyncResult

    webhook_cfg = _make_webhook_cfg(notification_only=True, notification_id_field="id")
    connector_cfg = _make_connector_cfg()

    mock_result = MagicMock(spec=SyncResult)
    mock_result.status = "completed"
    mock_result.records_inserted = 0
    mock_result.records_updated = 0

    full_sync_called = []

    async def fake_full_sync(c, dt, ing, **kwargs):
        full_sync_called.append(True)
        return mock_result

    engine = MagicMock()
    engine._pool = MagicMock()
    engine._pool.connection = MagicMock()
    engine._pool.connection.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
    engine._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    engine.run_sync_single_record = AsyncMock(return_value=mock_result)
    engine.run_sync = fake_full_sync

    import json
    # Payload has no 'id' field
    body = json.dumps({"event_type": "contact.updated", "some_other_field": "value"}).encode()
    sig = _sign_body(body, "test_secret")

    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "headers": [
            (b"x-signature", sig.encode()),
            (b"content-type", b"application/json"),
        ],
        "path": "/webhooks/test",
        "query_string": b"",
    }

    body_chunks = [body]
    chunk_idx = [0]

    async def receive():
        if chunk_idx[0] < len(body_chunks):
            data = body_chunks[chunk_idx[0]]
            chunk_idx[0] += 1
            return {"type": "http.request", "body": data, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(scope, receive)

    with patch("inandout.ingestion.webhooks._log_webhook", new_callable=AsyncMock):
        response = await handle_webhook(request, connector_cfg, webhook_cfg, engine)

    assert response.status_code == 200
    assert full_sync_called  # full sync was triggered


@pytest.mark.asyncio
async def test_full_payload_webhook_uses_direct_upsert(monkeypatch):
    """Full-payload webhook (not notification_only) → direct upsert, no run_sync."""
    monkeypatch.setenv("INOUT_CREDENTIAL_WEBHOOK_SECRET", "test_secret")

    from inandout.ingestion.webhooks import handle_webhook

    webhook_cfg = _make_webhook_cfg(notification_only=False)
    connector_cfg = _make_connector_cfg()

    run_sync_called = []

    engine = MagicMock()
    engine._pool = MagicMock()

    # Mock connection for upsert
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)  # no existing record → insert
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.transaction = MagicMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    engine._pool.connection = MagicMock()
    engine._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    engine._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    async def fake_run_sync(*a, **kw):
        run_sync_called.append(True)
    engine.run_sync = fake_run_sync
    engine.run_sync_single_record = AsyncMock()

    import json
    body = json.dumps({
        "event_type": "contact.updated",
        "id": "42",
        "name": "Alice",
        "email": "alice@example.com",
    }).encode()
    sig = _sign_body(body, "test_secret")

    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "headers": [
            (b"x-signature", sig.encode()),
            (b"content-type", b"application/json"),
        ],
        "path": "/webhooks/test",
        "query_string": b"",
    }

    body_chunks = [body]
    chunk_idx = [0]

    async def receive():
        if chunk_idx[0] < len(body_chunks):
            data = body_chunks[chunk_idx[0]]
            chunk_idx[0] += 1
            return {"type": "http.request", "body": data, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(scope, receive)

    with patch("inandout.ingestion.webhooks._log_webhook", new_callable=AsyncMock), \
         patch("inandout.ingestion.webhooks.ensure_source_table", new_callable=AsyncMock), \
         patch("inandout.ingestion.webhooks._upsert_record", new_callable=AsyncMock, return_value=(1, 0)):
        response = await handle_webhook(request, connector_cfg, webhook_cfg, engine)

    # Should not have called run_sync (full sync path)
    assert not run_sync_called
    # run_sync_single_record also not called
    engine.run_sync_single_record.assert_not_called()
