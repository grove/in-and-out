"""In-process async event bus for inandout internal signalling.

Supports lightweight pub/sub between components within the same process.
For cross-process coordination (e.g. the conflict-driven re-ingestion signal)
the DB-based control table is still the authoritative mechanism; this bus
provides a low-latency in-process shortcut.

Usage
-----
    from inandout.events import get_event_bus, EventType

    bus = get_event_bus()

    # subscriber
    async def on_reingest(connector, datatype, external_id, **kwargs):
        ...

    bus.subscribe(EventType.REINGEST_SIGNAL, on_reingest)

    # publisher (e.g. writeback engine on conflict)
    await bus.publish(
        EventType.REINGEST_SIGNAL,
        connector="salesforce",
        datatype="contacts",
        external_id="00Q123",
    )
"""
from inandout.events.bus import EventBus, EventType, get_event_bus

__all__ = ["EventBus", "EventType", "get_event_bus"]
