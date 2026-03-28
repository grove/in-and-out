"""Unit tests for WebhookLifecycleManager.__init__ field storage."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.ingestion.webhook_lifecycle import WebhookLifecycleManager
from inandout.config.webhooks import (
    FanOutConfig,
    SignatureAlgorithm,
    SignatureConfig,
    WebhookConfig,
    WebhookRegistrationConfig,
)


def _make_connector_cfg(name: str = "test_conn") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    return cfg


def _make_registration() -> WebhookRegistrationConfig:
    return WebhookRegistrationConfig(
        register_path="/webhooks",
        deregister_path="/webhooks/${webhook_id}",
        renew_path="/webhooks/${webhook_id}/renew",
        renew_interval="7d",
        health_check_path="/webhooks/${webhook_id}",
        id_response_path="data.id",
    )


def _make_webhook_cfg(registration=None) -> WebhookConfig:
    sig = SignatureConfig(
        algorithm=SignatureAlgorithm.hmac_sha256,
        header="X-Sig",
        credential_ref="SECRET",
    )
    fan_out = FanOutConfig(
        discriminator="type",
        routes=[],
        unmatched="log_and_discard",
    )
    return WebhookConfig(
        path="/webhook",
        signature=sig,
        fan_out=fan_out,
        registration=registration,
    )


def test_pool_stored():
    pool = MagicMock()
    connector = _make_connector_cfg()
    wh_cfg = _make_webhook_cfg()
    engine = MagicMock()
    mgr = WebhookLifecycleManager(pool, connector, wh_cfg, engine)
    assert mgr._pool is pool


def test_connector_cfg_stored():
    pool = MagicMock()
    connector = _make_connector_cfg("my_conn")
    wh_cfg = _make_webhook_cfg()
    engine = MagicMock()
    mgr = WebhookLifecycleManager(pool, connector, wh_cfg, engine)
    assert mgr._connector is connector


def test_webhook_cfg_stored():
    pool = MagicMock()
    connector = _make_connector_cfg()
    wh_cfg = _make_webhook_cfg()
    engine = MagicMock()
    mgr = WebhookLifecycleManager(pool, connector, wh_cfg, engine)
    assert mgr._webhook_cfg is wh_cfg


def test_engine_stored():
    pool = MagicMock()
    connector = _make_connector_cfg()
    wh_cfg = _make_webhook_cfg()
    engine = MagicMock()
    mgr = WebhookLifecycleManager(pool, connector, wh_cfg, engine)
    assert mgr._engine is engine


def test_registration_set_from_webhook_cfg_none():
    pool = MagicMock()
    connector = _make_connector_cfg()
    wh_cfg = _make_webhook_cfg(registration=None)
    mgr = WebhookLifecycleManager(pool, connector, wh_cfg, MagicMock())
    assert mgr._registration is None


def test_registration_set_from_webhook_cfg_with_value():
    pool = MagicMock()
    connector = _make_connector_cfg()
    reg = _make_registration()
    wh_cfg = _make_webhook_cfg(registration=reg)
    mgr = WebhookLifecycleManager(pool, connector, wh_cfg, MagicMock())
    assert mgr._registration is reg
