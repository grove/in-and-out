"""Unit tests for WebhookLifecycleManager (A1)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from inandout.config.webhooks import (
    FanOutConfig,
    SignatureAlgorithm,
    SignatureConfig,
    WebhookConfig,
    WebhookRegistrationConfig,
)


def _make_connector_cfg(name: str = "test") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    return cfg


def _make_webhook_cfg() -> WebhookConfig:
    sig = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Hub-Signature-256",
        credential_ref="MY_SECRET",
    )
    fan_out = FanOutConfig(
        discriminator="type",
        routes=[],
        unmatched="log_and_discard",
    )
    reg = WebhookRegistrationConfig(
        register_path="/webhooks",
        deregister_path="/webhooks/${webhook_id}",
        renew_path="/webhooks/${webhook_id}/renew",
        renew_interval="7d",
        health_check_path="/webhooks/${webhook_id}",
        id_response_path="id",
        callback_url_runtime_param="callback_url",
    )
    return WebhookConfig(
        path="/incoming",
        signature=sig,
        fan_out=fan_out,
        registration=reg,
    )


def _make_pool() -> MagicMock:
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock()
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool


def _make_transport_mock(response_json: dict | None = None) -> MagicMock:
    """Return a mock HttpTransportAdapter context manager."""
    resp = httpx.Response(200, json=response_json or {})
    transport = AsyncMock()
    transport._raw_request = AsyncMock(return_value=resp)
    transport.__aenter__ = AsyncMock(return_value=transport)
    transport.__aexit__ = AsyncMock(return_value=None)
    return transport


@pytest.mark.asyncio
async def test_register_posts_and_stores_webhook_id() -> None:
    """register() should POST and extract webhook ID from response."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg()
    engine = MagicMock()

    transport_mock = _make_transport_mock({"id": "wh-123", "status": "active"})

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        webhook_id = await mgr.register("https://my.server/hook")

    assert webhook_id == ["wh-123"]
    transport_mock._raw_request.assert_called_once_with(
        "POST", "/webhooks", json={"callback_url": "https://my.server/hook"}, headers=None
    )


@pytest.mark.asyncio
async def test_renew_calls_correct_endpoint() -> None:
    """renew() should call PUT on the renew path with webhook_id substituted."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg()
    engine = MagicMock()

    transport_mock = _make_transport_mock({"status": "renewed"})

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        await mgr.renew("wh-123")

    transport_mock._raw_request.assert_called_once_with("PUT", "/webhooks/wh-123/renew")


@pytest.mark.asyncio
async def test_deregister_only_removes_own_subscriptions() -> None:
    """deregister() should only remove subscriptions we created (T1 #26)."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    # Pool returns a row (we own it)
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.commit = AsyncMock()

    fetch_result = AsyncMock()
    fetch_result.fetchone = AsyncMock(return_value=("https://my.server/hook",))
    conn.execute = AsyncMock(return_value=fetch_result)

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    connector_cfg = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg()
    engine = MagicMock()

    transport_mock = _make_transport_mock()
    transport_mock._raw_request = AsyncMock(return_value=httpx.Response(204))

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        await mgr.deregister("wh-123")

    # DELETE was called since we own it
    transport_mock._raw_request.assert_called_once_with("DELETE", "/webhooks/wh-123")


@pytest.mark.asyncio
async def test_deregister_skips_unknown_subscription() -> None:
    """deregister() should skip if the subscription is not in our DB (not ours)."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)

    # Return None → subscription not found → skip
    fetch_result = AsyncMock()
    fetch_result.fetchone = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=fetch_result)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    connector_cfg = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg()
    engine = MagicMock()

    transport_mock = _make_transport_mock()

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        await mgr.deregister("wh-unknown")

    # DELETE should NOT have been called since we don't own it
    transport_mock._raw_request.assert_not_called()


@pytest.mark.asyncio
async def test_health_check_returns_false_on_404() -> None:
    """health_check() should return False when the transport raises an exception."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg()
    engine = MagicMock()

    # Simulate 404 by raising exception
    transport_mock = AsyncMock()
    transport_mock._raw_request = AsyncMock(side_effect=Exception("404 Not Found"))
    transport_mock.__aenter__ = AsyncMock(return_value=transport_mock)
    transport_mock.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        result = await mgr.health_check("wh-123")

    assert result is False


@pytest.mark.asyncio
async def test_health_check_active_field_active() -> None:
    """health_check() returns True when body field matches active value."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    reg = WebhookRegistrationConfig(
        register_path="/webhooks",
        health_check_path="/webhooks/${webhook_id}",
        health_check_active_field="value.status",
        health_check_active_value="ACTIVE",
    )
    webhook_cfg = _make_webhook_cfg()
    webhook_cfg = WebhookConfig(
        path="/incoming",
        registration=reg,
    )
    engine = MagicMock()

    resp = httpx.Response(200, json={"value": {"id": 42, "status": "ACTIVE"}})
    transport_mock = AsyncMock()
    transport_mock._raw_request = AsyncMock(return_value=resp)
    transport_mock.__aenter__ = AsyncMock(return_value=transport_mock)
    transport_mock.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        result = await mgr.health_check("42")

    assert result is True


@pytest.mark.asyncio
async def test_health_check_active_field_disabled() -> None:
    """health_check() returns False when body field does not match active value (e.g. DISABLED)."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    reg = WebhookRegistrationConfig(
        register_path="/webhooks",
        health_check_path="/webhooks/${webhook_id}",
        health_check_active_field="value.status",
        health_check_active_value="ACTIVE",
    )
    webhook_cfg = WebhookConfig(
        path="/incoming",
        registration=reg,
    )
    engine = MagicMock()

    resp = httpx.Response(200, json={"value": {"id": 42, "status": "DISABLED"}})
    transport_mock = AsyncMock()
    transport_mock._raw_request = AsyncMock(return_value=resp)
    transport_mock.__aenter__ = AsyncMock(return_value=transport_mock)
    transport_mock.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        result = await mgr.health_check("42")

    assert result is False


@pytest.mark.asyncio
async def test_health_check_active_field_missing_in_body() -> None:
    """health_check() returns False when health_check_active_field path resolves to None."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    reg = WebhookRegistrationConfig(
        register_path="/webhooks",
        health_check_path="/webhooks/${webhook_id}",
        health_check_active_field="value.status",
        health_check_active_value="ACTIVE",
    )
    webhook_cfg = WebhookConfig(
        path="/incoming",
        registration=reg,
    )
    engine = MagicMock()

    # Body does not contain "value" at all
    resp = httpx.Response(200, json={"id": 42})
    transport_mock = AsyncMock()
    transport_mock._raw_request = AsyncMock(return_value=resp)
    transport_mock.__aenter__ = AsyncMock(return_value=transport_mock)
    transport_mock.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        result = await mgr.health_check("42")

    assert result is False


@pytest.mark.asyncio
async def test_health_check_no_active_field_uses_status_code_only() -> None:
    """health_check() without active_field config still returns True on HTTP 200."""
    from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager

    pool = _make_pool()
    connector_cfg = _make_connector_cfg()
    webhook_cfg = _make_webhook_cfg()  # no health_check_active_field
    engine = MagicMock()

    resp = httpx.Response(200, json={"value": {"status": "DISABLED"}})
    transport_mock = AsyncMock()
    transport_mock._raw_request = AsyncMock(return_value=resp)
    transport_mock.__aenter__ = AsyncMock(return_value=transport_mock)
    transport_mock.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "inandout.ingestion.webhook_lifecycle.HttpTransportAdapter",
        return_value=transport_mock,
    ):
        mgr = WebhookLifecycleManager(pool, connector_cfg, webhook_cfg, engine)
        result = await mgr.health_check("wh-123")

    # Without active_field configured, 200 → True regardless of body content
    assert result is True
