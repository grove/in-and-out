"""Event bus for real-time SSE broadcasting in the demo simulator."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from inandout.simulator.store import MutationEvent, _new_id, _now_iso


@dataclass
class SimulatorEvent:
    """A single event pushed to all connected SSE clients."""

    event_type: str = "mutation"  # "mutation" | "request" | "webhook"
    connector: str = ""
    datatype: str = ""
    operation: str = ""  # create | update | delete  (mutation events)
    record_id: str = ""
    source: str = "engine"  # engine | ui               (mutation events)
    method: str = ""  # GET | POST | PATCH …      (request events)
    path: str = ""  # URL path                  (request events)
    status: int = 0  # HTTP status               (request/webhook events)
    duration_ms: int = 0  # round-trip ms             (request/webhook events)
    webhook_url: str = ""  # full URL                  (webhook events)
    payload_json: str = ""  # serialised payload        (webhook events)
    sent_headers_json: str = ""  # headers sent with webhook (webhook events)
    request_body_json: str = ""  # request body JSON          (request events)
    request_headers_json: str = ""  # received headers JSON   (request events)
    timestamp: str = field(default_factory=_now_iso)
    event_id: str = field(default_factory=_new_id)

    def to_sse(self) -> str:
        data = json.dumps({k: v for k, v in asdict(self).items() if k != "event_id"})
        return f"event: {self.event_type}\ndata: {data}\n\n"

    @classmethod
    def from_mutation(cls, ev: MutationEvent) -> "SimulatorEvent":
        return cls(
            event_type="mutation",
            connector=ev.connector,
            datatype=ev.datatype,
            operation=ev.operation,
            record_id=ev.record_id,
            source=ev.source,
            timestamp=ev.timestamp,
            event_id=ev.event_id,
        )


class EventBus:
    """Fan-out event bus.  Synchronous publish; async subscription via queues."""

    def __init__(self, history_size: int = 200) -> None:
        self._subscribers: list[asyncio.Queue[SimulatorEvent]] = []
        self._history: list[SimulatorEvent] = []
        self._max_history = history_size

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    def publish(self, event: SimulatorEvent) -> None:
        """Publish an event (sync-safe; can be called from any async context)."""
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass  # slow subscriber — drop oldest

    def publish_mutation(self, ev: MutationEvent) -> None:
        self.publish(SimulatorEvent.from_mutation(ev))

    def publish_request(
        self,
        connector: str,
        datatype: str,
        method: str,
        path: str,
        status: int,
        duration_ms: int = 0,
        request_body_json: str = "",
        request_headers_json: str = "",
        record_id: str = "",
    ) -> None:
        self.publish(
            SimulatorEvent(
                event_type="request",
                connector=connector,
                datatype=datatype,
                method=method,
                path=path,
                status=status,
                duration_ms=duration_ms,
                request_body_json=request_body_json,
                request_headers_json=request_headers_json,
                record_id=record_id,
            )
        )

    def publish_webhook(
        self,
        connector: str,
        datatype: str,
        operation: str,
        record_id: str,
        url: str,
        status: int,
        duration_ms: int = 0,
        payload_json: str = "",
        sent_headers_json: str = "",
    ) -> None:
        self.publish(
            SimulatorEvent(
                event_type="webhook",
                connector=connector,
                datatype=datatype,
                operation=operation,
                record_id=record_id,
                webhook_url=url,
                status=status,
                duration_ms=duration_ms,
                payload_json=payload_json,
                sent_headers_json=sent_headers_json,
            )
        )

    # ------------------------------------------------------------------
    # Subscribe / unsubscribe
    # ------------------------------------------------------------------

    def subscribe(self) -> asyncio.Queue[SimulatorEvent]:
        q: asyncio.Queue[SimulatorEvent] = asyncio.Queue(maxsize=200)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[SimulatorEvent]) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def recent(self, limit: int = 50) -> list[SimulatorEvent]:
        return list(reversed(self._history[-limit:]))
