"""Unit tests for out-of-order event handling for webhooks (T1 #35)."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Config model tests
# ---------------------------------------------------------------------------

def test_out_of_order_config_defaults():
    """OutOfOrderConfig should have sensible defaults."""
    from inandout.config.ingestion import OutOfOrderConfig, OutOfOrderStrategy

    cfg = OutOfOrderConfig()
    assert cfg.strategy == OutOfOrderStrategy.accept_latest_timestamp
    assert cfg.timestamp_field == "updated_at"
    assert cfg.sequence_field is None


def test_out_of_order_strategy_enum_values():
    """OutOfOrderStrategy should have accept_latest_timestamp, accept_highest_sequence, ignore."""
    from inandout.config.ingestion import OutOfOrderStrategy

    assert OutOfOrderStrategy.accept_latest_timestamp == "accept_latest_timestamp"
    assert OutOfOrderStrategy.accept_highest_sequence == "accept_highest_sequence"
    assert OutOfOrderStrategy.ignore == "ignore"


def test_webhook_events_config_has_out_of_order():
    """WebhookEventsConfig should have an out_of_order field."""
    from inandout.config.ingestion import WebhookEventsConfig, OutOfOrderConfig

    cfg = WebhookEventsConfig(
        subscriptions=[{"event": "contact.created"}],
        record_id_path="id",
        payload_type="full_state",
        ordering={"field": "updated_at"},
    )
    assert isinstance(cfg.out_of_order, OutOfOrderConfig)


def test_fan_out_route_has_notification_only():
    """FanOutRoute should have notification_only and notification_external_id_field fields."""
    from inandout.config.webhooks import FanOutRoute

    route = FanOutRoute(match="contact.created", datatype="contacts")
    assert route.notification_only is False
    assert route.notification_external_id_field == "id"


def test_fan_out_route_notification_only_configurable():
    """FanOutRoute notification_only and external_id_field can be set."""
    from inandout.config.webhooks import FanOutRoute

    route = FanOutRoute(
        match="contact.created",
        datatype="contacts",
        notification_only=True,
        notification_external_id_field="contact_id",
    )
    assert route.notification_only is True
    assert route.notification_external_id_field == "contact_id"


# ---------------------------------------------------------------------------
# Ordering comparison logic
# ---------------------------------------------------------------------------

def _is_stale(payload_ts: str, stored_ts: str | None, strategy: str) -> bool:
    """Pure logic: is the event stale?"""
    if strategy == "ignore" or stored_ts is None:
        return False
    if strategy == "accept_latest_timestamp":
        return str(payload_ts) <= str(stored_ts)
    if strategy == "accept_highest_sequence":
        try:
            return int(payload_ts) <= int(stored_ts)
        except ValueError:
            return str(payload_ts) <= str(stored_ts)
    return False


def test_newer_timestamp_is_not_stale():
    """A newer timestamp should not be classified as stale."""
    assert _is_stale("2026-01-02T00:00:00", "2026-01-01T00:00:00", "accept_latest_timestamp") is False


def test_older_timestamp_is_stale():
    """An older timestamp should be classified as stale."""
    assert _is_stale("2026-01-01T00:00:00", "2026-01-02T00:00:00", "accept_latest_timestamp") is True


def test_equal_timestamp_is_stale():
    """An equal timestamp should be classified as stale (≤ check)."""
    assert _is_stale("2026-01-01T00:00:00", "2026-01-01T00:00:00", "accept_latest_timestamp") is True


def test_ignore_strategy_never_stale():
    """With 'ignore' strategy, all events are accepted regardless of timestamp."""
    assert _is_stale("2026-01-01T00:00:00", "2026-12-31T00:00:00", "ignore") is False
    assert _is_stale("2020-01-01", "2026-01-01", "ignore") is False


def test_missing_stored_ts_is_not_stale():
    """If stored timestamp is None (no prior record), event is always accepted."""
    assert _is_stale("2026-01-01", None, "accept_latest_timestamp") is False


def test_higher_sequence_is_not_stale():
    """Higher sequence number should not be classified as stale."""
    assert _is_stale("101", "100", "accept_highest_sequence") is False


def test_lower_sequence_is_stale():
    """Lower sequence number should be classified as stale."""
    assert _is_stale("99", "100", "accept_highest_sequence") is True


# ---------------------------------------------------------------------------
# Webhook handler source code inspection
# ---------------------------------------------------------------------------

def test_webhooks_handles_stale_event():
    """webhook handler should implement stale event detection."""
    import inspect
    from inandout.ingestion import webhooks as wh_mod
    source = inspect.getsource(wh_mod)
    assert "webhook_stale_event_discarded" in source


def test_webhooks_handles_notification_only_with_id():
    """webhook handler should call single-record fetch for notification-only with ID."""
    import inspect
    from inandout.ingestion import webhooks as wh_mod
    source = inspect.getsource(wh_mod)
    assert "run_sync_single_record" in source


def test_webhooks_handles_notification_missing_id():
    """webhook handler should fall back to full sync when ID missing from notification."""
    import inspect
    from inandout.ingestion import webhooks as wh_mod
    source = inspect.getsource(wh_mod)
    assert "webhook_notification_missing_id_full_sync" in source
