# Lifecycle Events

in-and-out ships an in-process async pub/sub event bus. You can subscribe to lifecycle events to build monitoring integrations, custom alerting, audit trails, or cross-component coordination — all without modifying the core engine.

---

## Quick start

```python
from inandout.events.bus import get_event_bus, EventType

bus = get_event_bus()

@bus.subscribe(EventType.SYNC_STARTED)
async def on_sync_started(**payload):
    print(f"Sync started: {payload['connector']}/{payload['datatype']}")
```

`get_event_bus()` returns the module-level singleton. Handlers are plain async coroutines; synchronous callables are not supported.

---

## Event reference

### Ingestion events

| EventType | When emitted | Payload keys |
|---|---|---|
| `SYNC_STARTED` | An ingestion sync cycle begins | `connector`, `datatype`, `mode` (`"full"` or `"incremental"`) |
| `SYNC_COMPLETED` | A sync cycle finishes successfully | `connector`, `datatype`, `processed`, `failed`, `skipped` |
| `SYNC_FAILED` | A sync cycle aborts with an unhandled exception | `connector`, `datatype`, `error` |
| `SCHEMA_DRIFT_DETECTED` | New or orphan columns found during a full sync | `connector`, `datatype`, `new_fields` (list), `orphan_columns` (list) |
| `ROW_DEAD_LETTERED` | A record is written to the ingestion dead-letter table | `connector`, `datatype`, `external_id`, `error_class`, `error_message` |
| `REINGEST_SIGNAL` | Writeback detects external drift and requests re-ingestion | `connector`, `datatype`, `external_id`, `reason` |

### Writeback events

| EventType | When emitted | Payload keys |
|---|---|---|
| `WRITEBACK_CYCLE_COMPLETED` | A writeback batch finishes (success or partial failure) | `connector`, `datatype`, `processed`, `skipped`, `failed`, `conflicts` |
| `WRITEBACK_CONFLICT` | A conflict is detected but not yet resolved | `connector`, `datatype`, `external_id`, `resolution_strategy` |

### Availability events

| EventType | When emitted | Payload keys |
|---|---|---|
| `CONNECTOR_UNAVAILABLE` | A connector or datatype becomes unreachable | `connector`, `datatype`, `reason` |
| `CONNECTOR_RECOVERED` | A previously-unavailable connector recovers | `connector`, `datatype` |
| `CIRCUIT_BREAKER_STATE_CHANGE` | Circuit breaker state changes | `connector`, `datatype`, `state` (`"open"`, `"closed"`, `"half_open"`) |

---

## Subscribing to events

### Decorator style

```python
bus = get_event_bus()

@bus.subscribe(EventType.SYNC_FAILED)
async def on_sync_failed(**payload):
    await alert_channel.send(
        f"Sync failed for {payload['connector']}: {payload['error']}"
    )
```

### Direct registration

```python
async def on_schema_drift(**payload):
    await migration_service.propose_migration(
        connector=payload["connector"],
        new_fields=payload["new_fields"],
    )

bus.subscribe(EventType.SCHEMA_DRIFT_DETECTED, on_schema_drift)
```

### Unsubscribing

```python
bus.unsubscribe(EventType.SYNC_FAILED, on_sync_failed)
```

---

## Guarantees and constraints

- Handlers are called **concurrently** via `asyncio.gather(return_exceptions=True)`.
- A failing handler **never blocks** other handlers or the pipeline itself.
- Events are in-process only — they are not persisted or sent to a message broker.
- All handlers must be `async def` coroutines.
- Handlers run in the same event loop as the engine; blocking I/O inside a handler will stall the pipeline. Use `anyio.to_thread.run_sync` for blocking work.
- Registering the same handler twice for the same event type is a no-op.

---

## Testing with the event bus

```python
import pytest
from inandout.events.bus import get_event_bus, EventType

@pytest.mark.asyncio
async def test_sync_started_event():
    bus = get_event_bus()
    received = []

    async def capture(**payload):
        received.append(payload)

    bus.subscribe(EventType.SYNC_STARTED, capture)
    try:
        # ... trigger sync ...
        assert received[0]["connector"] == "hubspot"
    finally:
        bus.unsubscribe(EventType.SYNC_STARTED, capture)
```

Always unsubscribe in teardown to avoid cross-test pollution.
