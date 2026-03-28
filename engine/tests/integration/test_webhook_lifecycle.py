"""Integration tests for WebhookLifecycleManager (T1 #7, T1 #26)."""
from __future__ import annotations

import os

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.webhooks import (
    WebhookConfig, WebhookRegistrationConfig,
    SignatureConfig, SignatureAlgorithm, FanOutConfig, UnmatchedAction,
)
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig,
)
from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available",
)

_CONNECTOR_NAME = "wh_lc_test"
_BASE_URL = "https://api.wh-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_WH_LC_TEST_KEY"] = "dummy-api-key"
    yield
    os.environ.pop("INOUT_CREDENTIAL_WH_LC_TEST_KEY", None)


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR_NAME,
        system="WHTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="wh_lc_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "events": DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/events/${external_id}"),
                    ),
                ),
            ),
        },
    )


def _make_webhook_cfg() -> WebhookConfig:
    return WebhookConfig(
        path="/webhooks/events",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Hub-Signature-256",
            credential_ref="wh_lc_test_key",
        ),
        fan_out=FanOutConfig(
            discriminator="type",
            routes=[],
            unmatched=UnmatchedAction.log_and_discard,
        ),
        registration=WebhookRegistrationConfig(
            register_path="/webhooks",
            deregister_path="/webhooks/${webhook_id}",
            renew_path="/webhooks/${webhook_id}/renew",
            health_check_path="/webhooks/${webhook_id}",
            callback_url_runtime_param="url",
            id_response_path="id",
        ),
    )


async def _insert_subscription(pool, connector: str, webhook_id: str, callback_url: str, status: str = "active") -> None:
    """Manually insert a subscription row for ownership-check tests."""
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_webhook_subscriptions
                (connector, webhook_id, callback_url, status, registered_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            [connector, webhook_id, callback_url, status],
        )
        await conn.commit()


@pytest.mark.anyio
async def test_register_persists_subscription_to_db(pool):
    """register() POSTs to register_path and persists webhook_id + status=active to DB."""
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    manager = WebhookLifecycleManager(pool, connector, webhook_cfg, engine=None)

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.post("/webhooks").mock(
            return_value=httpx.Response(200, json={"id": "wh-abc-123"})
        )

        webhook_id = await manager.register("https://myapp.example.com/cb")

    assert webhook_id == "wh-abc-123"

    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT webhook_id, callback_url, status FROM inout_ops_webhook_subscriptions "
            "WHERE connector = %s AND webhook_id = %s",
            [_CONNECTOR_NAME, "wh-abc-123"],
        )).fetchone()

    assert row is not None
    assert row[0] == "wh-abc-123"
    assert row[1] == "https://myapp.example.com/cb"
    assert row[2] == "active"


@pytest.mark.anyio
async def test_health_check_returns_true_and_updates_timestamp(pool):
    """health_check() returns True on 200 and updates last_health_check_at."""
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    manager = WebhookLifecycleManager(pool, connector, webhook_cfg, engine=None)

    await _insert_subscription(pool, _CONNECTOR_NAME, "wh-hc-001", "https://cb.example.com/")

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/webhooks/wh-hc-001").mock(
            return_value=httpx.Response(200, json={"status": "active"})
        )
        result = await manager.health_check("wh-hc-001")

    assert result is True

    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT last_health_check_at FROM inout_ops_webhook_subscriptions "
            "WHERE connector = %s AND webhook_id = %s",
            [_CONNECTOR_NAME, "wh-hc-001"],
        )).fetchone()

    assert row is not None
    assert row[0] is not None  # timestamp was set


@pytest.mark.anyio
async def test_health_check_returns_false_on_404(pool):
    """health_check() returns False when the webhook endpoint responds with 404."""
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    manager = WebhookLifecycleManager(pool, connector, webhook_cfg, engine=None)

    await _insert_subscription(pool, _CONNECTOR_NAME, "wh-hc-404", "https://cb.example.com/")

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/webhooks/wh-hc-404").mock(
            return_value=httpx.Response(404)
        )
        result = await manager.health_check("wh-hc-404")

    assert result is False


@pytest.mark.anyio
async def test_deregister_sends_delete_and_updates_status(pool):
    """deregister() sends DELETE to the remote endpoint and sets status='deregistered' in DB."""
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    manager = WebhookLifecycleManager(pool, connector, webhook_cfg, engine=None)

    await _insert_subscription(pool, _CONNECTOR_NAME, "wh-dereg-001", "https://cb.example.com/")

    with respx.mock(base_url=_BASE_URL) as mock:
        delete_route = mock.delete("/webhooks/wh-dereg-001").mock(
            return_value=httpx.Response(204)
        )
        await manager.deregister("wh-dereg-001")

    assert delete_route.called

    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT status FROM inout_ops_webhook_subscriptions "
            "WHERE connector = %s AND webhook_id = %s",
            [_CONNECTOR_NAME, "wh-dereg-001"],
        )).fetchone()

    assert row is not None
    assert row[0] == "deregistered"


@pytest.mark.anyio
async def test_deregister_ownership_scope_skips_unknown_webhook(pool):
    """deregister() skips the HTTP call when the webhook_id is not in our DB (T1 #26)."""
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    manager = WebhookLifecycleManager(pool, connector, webhook_cfg, engine=None)

    # Do NOT insert a row for "wh-not-owned"
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        delete_route = mock.delete("/webhooks/wh-not-owned").mock(
            return_value=httpx.Response(204)
        )
        await manager.deregister("wh-not-owned")

    # No HTTP call should have been made — we don't own this webhook
    assert not delete_route.called
