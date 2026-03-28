"""Lightweight in-process async event bus.

Design goals:
  - Zero external dependencies (only asyncio).
  - Handlers are async coroutines; synchronous callables are not supported.
  - Failures in one handler do not prevent other handlers from running.
  - Thread-safe subscription management (asyncio.Lock).
  - A module-level singleton is available via :func:`get_event_bus`.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from enum import Enum
from typing import Any, Awaitable, Callable

import structlog

logger = structlog.get_logger(__name__)

Handler = Callable[..., Awaitable[None]]


class EventType(str, Enum):
    """Event types understood by the internal event bus."""

    # Writeback detects external drift; ingestion should re-fetch the record.
    # Payload: connector, datatype, external_id, reason (optional str)
    REINGEST_SIGNAL = "reingest_signal"

    # A connector or datatype has become unavailable.
    # Payload: connector, datatype, reason (optional str)
    CONNECTOR_UNAVAILABLE = "connector_unavailable"

    # A connector or datatype has recovered.
    # Payload: connector, datatype
    CONNECTOR_RECOVERED = "connector_recovered"

    # Circuit breaker state changed.
    # Payload: connector, datatype, state ("open"|"closed"|"half_open")
    CIRCUIT_BREAKER_STATE_CHANGE = "circuit_breaker_state_change"

    # A sync cycle completed for a (connector, datatype).
    # Payload: connector, datatype, processed, failed, skipped
    SYNC_COMPLETED = "sync_completed"

    # A writeback conflict was detected (but not yet resolved).
    # Payload: connector, datatype, external_id, resolution_strategy
    WRITEBACK_CONFLICT = "writeback_conflict"

    # An ingestion sync cycle has started.
    # Payload: connector, datatype, mode ("full"|"incremental")
    SYNC_STARTED = "sync_started"

    # An ingestion sync cycle has failed with an unhandled exception.
    # Payload: connector, datatype, error (str)
    SYNC_FAILED = "sync_failed"

    # Schema drift was detected during a full sync.
    # Payload: connector, datatype, new_fields (list[str]), orphan_columns (list[str])
    SCHEMA_DRIFT_DETECTED = "schema_drift_detected"

    # A record was written to the ingestion dead-letter table.
    # Payload: connector, datatype, external_id, error_class, error_message
    ROW_DEAD_LETTERED = "row_dead_lettered"

    # A writeback cycle has completed (success or partial failure).
    # Payload: connector, datatype, processed, skipped, failed, conflicts
    WRITEBACK_CYCLE_COMPLETED = "writeback_cycle_completed"


class EventBus:
    """Async in-process pub/sub bus.

    Subscribers are coroutine functions.  Publishing an event calls all
    registered handlers concurrently via :func:`asyncio.gather` with
    ``return_exceptions=True`` so that a failing handler never blocks others.
    """

    def __init__(self) -> None:
        # event_type → list[handler]
        self._handlers: dict[EventType, list[Handler]] = defaultdict(list)
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    def subscribe(self, event_type: EventType, handler: Handler) -> None:
        """Register *handler* for *event_type*.

        Registering the same handler twice is a no-op.
        """
        handlers = self._handlers[event_type]
        if handler not in handlers:
            handlers.append(handler)
            logger.debug(
                "event_bus_subscribed",
                event_type=event_type.value,
                handler=getattr(handler, "__qualname__", repr(handler)),
            )

    def unsubscribe(self, event_type: EventType, handler: Handler) -> bool:
        """Remove *handler* from *event_type* subscriptions.

        Returns ``True`` if the handler was found and removed, ``False``
        otherwise.
        """
        handlers = self._handlers.get(event_type, [])
        try:
            handlers.remove(handler)
            return True
        except ValueError:
            return False

    def subscriber_count(self, event_type: EventType) -> int:
        """Return the number of handlers registered for *event_type*."""
        return len(self._handlers.get(event_type, []))

    def clear(self, event_type: EventType | None = None) -> None:
        """Remove all handlers, optionally scoped to a single event type."""
        if event_type is None:
            self._handlers.clear()
        else:
            self._handlers.pop(event_type, None)

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    async def publish(self, event_type: EventType, **kwargs: Any) -> int:
        """Fire all handlers registered for *event_type*.

        Handlers run concurrently.  Exceptions are caught and logged as
        warnings; they do not propagate to the caller.

        Returns the number of handlers invoked.
        """
        handlers = list(self._handlers.get(event_type, []))
        if not handlers:
            return 0

        logger.debug(
            "event_bus_publish",
            event_type=event_type.value,
            handler_count=len(handlers),
            **{k: str(v)[:120] for k, v in kwargs.items()},
        )

        results = await asyncio.gather(
            *(h(**kwargs) for h in handlers),
            return_exceptions=True,
        )
        for handler, result in zip(handlers, results):
            if isinstance(result, Exception):
                logger.warning(
                    "event_bus_handler_error",
                    event_type=event_type.value,
                    handler=getattr(handler, "__qualname__", repr(handler)),
                    error=str(result),
                )
        return len(handlers)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_DEFAULT_BUS: EventBus | None = None


def get_event_bus() -> EventBus:
    """Return the process-global EventBus singleton.

    The singleton is created on first access and is safe to use from any
    coroutine within the same event loop.
    """
    global _DEFAULT_BUS
    if _DEFAULT_BUS is None:
        _DEFAULT_BUS = EventBus()
    return _DEFAULT_BUS


def reset_event_bus() -> None:
    """Replace the singleton with a fresh bus.

    Intended for use in tests only.
    """
    global _DEFAULT_BUS
    _DEFAULT_BUS = None
