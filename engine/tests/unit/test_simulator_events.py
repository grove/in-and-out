"""Unit tests for the simulator EventBus."""

from __future__ import annotations

import asyncio

import pytest

from inandout.simulator.events import EventBus, SimulatorEvent
from inandout.simulator.store import MutationEvent


def _mutation(record_id: str = "1", operation: str = "create") -> MutationEvent:
    return MutationEvent(
        connector="acme",
        datatype="contacts",
        operation=operation,
        record_id=record_id,
    )


def _event(record_id: str = "r1") -> SimulatorEvent:
    return SimulatorEvent(
        event_type="mutation",
        connector="acme",
        datatype="contacts",
        operation="create",
        record_id=record_id,
    )


# ---------------------------------------------------------------------------
# publish / subscribe
# ---------------------------------------------------------------------------


async def test_subscriber_receives_published_event() -> None:
    bus = EventBus()
    q = bus.subscribe()
    ev = _event()
    bus.publish(ev)
    received = q.get_nowait()
    assert received is ev


async def test_multiple_subscribers_each_receive_event() -> None:
    bus = EventBus()
    q1 = bus.subscribe()
    q2 = bus.subscribe()
    bus.publish(_event())
    assert not q1.empty()
    assert not q2.empty()


async def test_unsubscribe_stops_delivery() -> None:
    bus = EventBus()
    q = bus.subscribe()
    bus.unsubscribe(q)
    bus.publish(_event())
    assert q.empty()


async def test_unsubscribe_nonexistent_is_silent() -> None:
    bus = EventBus()
    q: asyncio.Queue[SimulatorEvent] = asyncio.Queue()
    bus.unsubscribe(q)  # should not raise


# ---------------------------------------------------------------------------
# publish_mutation
# ---------------------------------------------------------------------------


async def test_publish_mutation_wraps_event() -> None:
    bus = EventBus()
    q = bus.subscribe()
    bus.publish_mutation(_mutation("42", "update"))
    ev = q.get_nowait()
    assert ev.event_type == "mutation"
    assert ev.connector == "acme"
    assert ev.datatype == "contacts"
    assert ev.operation == "update"
    assert ev.record_id == "42"


# ---------------------------------------------------------------------------
# publish_request
# ---------------------------------------------------------------------------


async def test_publish_request_emits_request_event() -> None:
    bus = EventBus()
    q = bus.subscribe()
    bus.publish_request("acme", "contacts", "GET", "/v1/contacts", 200, 15)
    ev = q.get_nowait()
    assert ev.event_type == "request"
    assert ev.method == "GET"
    assert ev.status == 200
    assert ev.duration_ms == 15


# ---------------------------------------------------------------------------
# recent / history
# ---------------------------------------------------------------------------


async def test_recent_returns_events_in_reverse_chronological_order() -> None:
    bus = EventBus()
    ev1 = _event("1")
    ev2 = _event("2")
    bus.publish(ev1)
    bus.publish(ev2)
    recent = bus.recent(limit=10)
    # recent() reverses history, so newest is first
    assert recent[0] is ev2
    assert recent[1] is ev1


async def test_recent_limit_is_respected() -> None:
    bus = EventBus()
    for i in range(10):
        bus.publish(_event(str(i)))
    assert len(bus.recent(limit=3)) == 3


async def test_history_is_bounded_by_history_size() -> None:
    bus = EventBus(history_size=5)
    for i in range(10):
        bus.publish(_event(str(i)))
    # recent() should return at most history_size entries
    assert len(bus.recent(limit=100)) == 5


async def test_recent_returns_empty_before_any_publish() -> None:
    bus = EventBus()
    assert bus.recent() == []


# ---------------------------------------------------------------------------
# to_sse serialisation
# ---------------------------------------------------------------------------


async def test_to_sse_format() -> None:
    ev = SimulatorEvent(event_type="mutation", connector="acme", datatype="contacts", record_id="7")
    sse = ev.to_sse()
    assert sse.startswith("event: mutation\n")
    assert "acme" in sse
    assert sse.endswith("\n\n")
