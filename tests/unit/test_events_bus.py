"""Unit tests for the in-process async event bus."""
from __future__ import annotations

import asyncio
import pytest

from inandout.events.bus import EventBus, EventType, get_event_bus, reset_event_bus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_bus():
    reset_event_bus()
    yield
    reset_event_bus()


# ---------------------------------------------------------------------------
# subscribe / subscriber_count
# ---------------------------------------------------------------------------


def test_subscribe_registers_handler():
    bus = EventBus()

    async def handler(**_):
        pass

    bus.subscribe(EventType.REINGEST_SIGNAL, handler)
    assert bus.subscriber_count(EventType.REINGEST_SIGNAL) == 1


def test_subscribe_same_handler_twice_is_no_op():
    bus = EventBus()

    async def handler(**_):
        pass

    bus.subscribe(EventType.REINGEST_SIGNAL, handler)
    bus.subscribe(EventType.REINGEST_SIGNAL, handler)
    assert bus.subscriber_count(EventType.REINGEST_SIGNAL) == 1


def test_subscriber_count_zero_for_unknown_type():
    bus = EventBus()
    assert bus.subscriber_count(EventType.SYNC_COMPLETED) == 0


# ---------------------------------------------------------------------------
# unsubscribe
# ---------------------------------------------------------------------------


def test_unsubscribe_removes_handler():
    bus = EventBus()

    async def handler(**_):
        pass

    bus.subscribe(EventType.REINGEST_SIGNAL, handler)
    removed = bus.unsubscribe(EventType.REINGEST_SIGNAL, handler)
    assert removed is True
    assert bus.subscriber_count(EventType.REINGEST_SIGNAL) == 0


def test_unsubscribe_returns_false_if_not_subscribed():
    bus = EventBus()

    async def handler(**_):
        pass

    removed = bus.unsubscribe(EventType.CONNECTOR_UNAVAILABLE, handler)
    assert removed is False


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------


def test_clear_specific_event_type():
    bus = EventBus()

    async def h1(**_): pass
    async def h2(**_): pass

    bus.subscribe(EventType.REINGEST_SIGNAL, h1)
    bus.subscribe(EventType.CONNECTOR_RECOVERED, h2)

    bus.clear(EventType.REINGEST_SIGNAL)
    assert bus.subscriber_count(EventType.REINGEST_SIGNAL) == 0
    assert bus.subscriber_count(EventType.CONNECTOR_RECOVERED) == 1


def test_clear_all():
    bus = EventBus()

    async def h1(**_): pass

    bus.subscribe(EventType.REINGEST_SIGNAL, h1)
    bus.subscribe(EventType.SYNC_COMPLETED, h1)

    bus.clear()
    assert bus.subscriber_count(EventType.REINGEST_SIGNAL) == 0
    assert bus.subscriber_count(EventType.SYNC_COMPLETED) == 0


# ---------------------------------------------------------------------------
# publish — invocation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_publish_calls_handler():
    bus = EventBus()
    calls = []

    async def handler(connector, datatype, **_):
        calls.append((connector, datatype))

    bus.subscribe(EventType.REINGEST_SIGNAL, handler)
    count = await bus.publish(
        EventType.REINGEST_SIGNAL,
        connector="sf",
        datatype="contacts",
        external_id="xyz",
    )
    assert count == 1
    assert calls == [("sf", "contacts")]


@pytest.mark.anyio
async def test_publish_calls_multiple_handlers():
    bus = EventBus()
    calls = []

    async def h1(**_): calls.append("h1")
    async def h2(**_): calls.append("h2")

    bus.subscribe(EventType.SYNC_COMPLETED, h1)
    bus.subscribe(EventType.SYNC_COMPLETED, h2)

    await bus.publish(EventType.SYNC_COMPLETED, connector="sf", datatype="leads", processed=5, failed=0, skipped=0)
    assert "h1" in calls
    assert "h2" in calls


@pytest.mark.anyio
async def test_publish_returns_zero_when_no_handlers():
    bus = EventBus()
    count = await bus.publish(EventType.WRITEBACK_CONFLICT, connector="sf", datatype="contacts")
    assert count == 0


@pytest.mark.anyio
async def test_publish_returns_handler_count():
    bus = EventBus()

    async def h1(**_): pass
    async def h2(**_): pass
    async def h3(**_): pass

    for h in (h1, h2, h3):
        bus.subscribe(EventType.CONNECTOR_UNAVAILABLE, h)

    count = await bus.publish(EventType.CONNECTOR_UNAVAILABLE, connector="sf", datatype="accounts")
    assert count == 3


# ---------------------------------------------------------------------------
# publish — error isolation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_failing_handler_does_not_block_others():
    bus = EventBus()
    calls = []

    async def bad_handler(**_):
        raise RuntimeError("handler exploded")

    async def good_handler(**_):
        calls.append("good")

    bus.subscribe(EventType.CIRCUIT_BREAKER_STATE_CHANGE, bad_handler)
    bus.subscribe(EventType.CIRCUIT_BREAKER_STATE_CHANGE, good_handler)

    # Should not raise
    count = await bus.publish(
        EventType.CIRCUIT_BREAKER_STATE_CHANGE,
        connector="sf",
        datatype="contacts",
        state="open",
    )
    assert count == 2
    assert calls == ["good"]


@pytest.mark.anyio
async def test_all_handlers_failing_does_not_raise():
    bus = EventBus()

    async def bad(**_):
        raise ValueError("kaboom")

    bus.subscribe(EventType.REINGEST_SIGNAL, bad)
    # Must not propagate
    await bus.publish(EventType.REINGEST_SIGNAL, connector="sf", datatype="contacts", external_id="1")


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


def test_get_event_bus_returns_same_instance():
    b1 = get_event_bus()
    b2 = get_event_bus()
    assert b1 is b2


def test_reset_event_bus_creates_fresh_instance():
    b1 = get_event_bus()
    reset_event_bus()
    b2 = get_event_bus()
    assert b1 is not b2


@pytest.mark.anyio
async def test_singleton_shares_subscriptions():
    """Handlers registered via the singleton are callable from any publish."""
    calls = []

    async def handler(**_):
        calls.append(True)

    bus = get_event_bus()
    bus.subscribe(EventType.SYNC_COMPLETED, handler)

    # Publish through the same singleton reference
    await get_event_bus().publish(
        EventType.SYNC_COMPLETED,
        connector="sf", datatype="accounts",
        processed=10, failed=0, skipped=0,
    )
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# EventType enum values
# ---------------------------------------------------------------------------


def test_event_type_values_are_strings():
    for et in EventType:
        assert isinstance(et.value, str)
        assert len(et.value) > 0


def test_reingest_signal_value():
    assert EventType.REINGEST_SIGNAL == "reingest_signal"
