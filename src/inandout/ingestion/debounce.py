"""Webhook event debouncer: coalesces bursts for the same (connector, datatype, external_id)."""
from __future__ import annotations

import time
from typing import Any, Callable


class WebhookDebouncer:
    """
    Debounces incoming webhook events by (connector, datatype, external_id).

    When multiple events arrive for the same key within `window_secs`, only the
    last one is processed (last-write-wins coalescing).

    Usage:
        debouncer = WebhookDebouncer(window_secs=0.5)
        async with anyio.create_task_group() as tg:
            tg.start_soon(debouncer.run_sweep_loop)
            # ... handle incoming webhooks:
            await debouncer.enqueue(connector, datatype, external_id, payload, handler)
    """

    def __init__(self, window_secs: float = 0.5):
        self._window = window_secs
        # key → (payload, arrived_at, handler)
        self._pending: dict[tuple, tuple[dict, float, Any]] = {}

    async def enqueue(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        payload: dict,
        handler: Callable,
    ) -> None:
        """Enqueue an event. If already pending for this key, replace with newer payload."""
        key = (connector, datatype, external_id)
        self._pending[key] = (payload, time.monotonic(), handler)

    async def run_sweep_loop(self) -> None:
        """Background sweep: process events whose debounce window has expired."""
        import anyio
        while True:
            await anyio.sleep(self._window / 2)
            now = time.monotonic()
            ready = [
                (k, v) for k, v in list(self._pending.items())
                if now - v[1] >= self._window
            ]
            for key, (payload, arrived_at, handler) in ready:
                self._pending.pop(key, None)
                try:
                    await handler(payload)
                except Exception as exc:
                    import structlog
                    structlog.get_logger(__name__).error(
                        "debounce_handler_failed",
                        key=str(key),
                        error=str(exc),
                    )

    def pending_count(self) -> int:
        return len(self._pending)
