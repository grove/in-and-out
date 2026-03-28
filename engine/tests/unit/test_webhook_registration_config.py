"""Unit tests for WebhookRegistrationConfig Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.webhooks import WebhookRegistrationConfig


def test_minimal_valid():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.register_path == "/webhooks"


def test_deregister_path_default_none():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.deregister_path is None


def test_renew_path_default_none():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.renew_path is None


def test_health_check_path_default_none():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.health_check_path is None


def test_renew_interval_default_7d():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.renew_interval == "7d"


def test_id_response_path_default():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.id_response_path == "id"


def test_callback_url_runtime_param_default():
    cfg = WebhookRegistrationConfig(register_path="/webhooks")
    assert cfg.callback_url_runtime_param == "callback_url"


def test_deregister_path_set():
    cfg = WebhookRegistrationConfig(
        register_path="/webhooks",
        deregister_path="/webhooks/${webhook_id}",
    )
    assert cfg.deregister_path == "/webhooks/${webhook_id}"


def test_renew_path_set():
    cfg = WebhookRegistrationConfig(
        register_path="/webhooks",
        renew_path="/webhooks/${webhook_id}/renew",
        renew_interval="7d",
    )
    assert cfg.renew_path == "/webhooks/${webhook_id}/renew"


def test_health_check_path_set():
    cfg = WebhookRegistrationConfig(
        register_path="/webhooks",
        health_check_path="/webhooks/${webhook_id}",
    )
    assert cfg.health_check_path == "/webhooks/${webhook_id}"


def test_id_response_path_nested_dot():
    cfg = WebhookRegistrationConfig(
        register_path="/webhooks",
        id_response_path="data.webhook.id",
    )
    assert cfg.id_response_path == "data.webhook.id"


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        WebhookRegistrationConfig(
            register_path="/webhooks",
            unknown_field="bad",
        )


def test_missing_register_path_raises():
    with pytest.raises(ValidationError):
        WebhookRegistrationConfig()


def test_custom_renew_interval():
    cfg = WebhookRegistrationConfig(
        register_path="/webhooks",
        renew_interval="30d",
    )
    assert cfg.renew_interval == "30d"
