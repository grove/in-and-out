"""Unit tests for WebhookEventsConfig Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.ingestion import (
    OutOfOrderConfig,
    OutOfOrderStrategy,
    WebhookEventsConfig,
    WebhookPayloadType,
)


def _minimal_events(**overrides) -> dict:
    base = {
        "subscriptions": [{"event": "contact.created"}],
        "record_id_path": "data.id",
        "payload_type": "full_state",
        "ordering": {"field": "updated_at"},
    }
    base.update(overrides)
    return base


# --- WebhookPayloadType enum ---

def test_payload_type_full_state():
    assert WebhookPayloadType.full_state == "full_state"


def test_payload_type_partial():
    assert WebhookPayloadType.partial == "partial"


def test_payload_type_notification():
    assert WebhookPayloadType.notification == "notification"


# --- WebhookEventsConfig ---

def test_minimal_valid():
    cfg = WebhookEventsConfig(**_minimal_events())
    assert cfg.record_id_path == "data.id"


def test_subscriptions_stored():
    cfg = WebhookEventsConfig(**_minimal_events())
    assert len(cfg.subscriptions) == 1
    assert cfg.subscriptions[0]["event"] == "contact.created"


def test_subscriptions_min_length_one():
    with pytest.raises(ValidationError):
        WebhookEventsConfig(**_minimal_events(subscriptions=[]))


def test_payload_type_stored():
    cfg = WebhookEventsConfig(**_minimal_events())
    assert cfg.payload_type == WebhookPayloadType.full_state


def test_payload_type_partial():
    cfg = WebhookEventsConfig(**_minimal_events(payload_type="partial"))
    assert cfg.payload_type == WebhookPayloadType.partial


def test_ordering_stored():
    cfg = WebhookEventsConfig(**_minimal_events(ordering={"field": "seq", "type": "integer"}))
    assert cfg.ordering["field"] == "seq"


def test_debounce_default_none():
    cfg = WebhookEventsConfig(**_minimal_events())
    assert cfg.debounce is None


def test_debounce_set():
    cfg = WebhookEventsConfig(**_minimal_events(debounce={"window": "5s"}))
    assert cfg.debounce["window"] == "5s"


def test_out_of_order_default():
    cfg = WebhookEventsConfig(**_minimal_events())
    assert cfg.out_of_order.strategy == OutOfOrderStrategy.accept_latest_timestamp


def test_out_of_order_custom():
    cfg = WebhookEventsConfig(
        **_minimal_events(
            out_of_order={"strategy": "ignore"},
        )
    )
    assert cfg.out_of_order.strategy == OutOfOrderStrategy.ignore


def test_missing_record_id_path_raises():
    data = _minimal_events()
    del data["record_id_path"]
    with pytest.raises(ValidationError):
        WebhookEventsConfig(**data)


def test_missing_payload_type_raises():
    data = _minimal_events()
    del data["payload_type"]
    with pytest.raises(ValidationError):
        WebhookEventsConfig(**data)


def test_missing_ordering_raises():
    data = _minimal_events()
    del data["ordering"]
    with pytest.raises(ValidationError):
        WebhookEventsConfig(**data)


def test_extra_fields_allowed():
    cfg = WebhookEventsConfig(**_minimal_events(custom="extra"))
    assert cfg.custom == "extra"  # type: ignore[attr-defined]
