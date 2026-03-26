"""Server-Sent Events streaming endpoint for the demo simulator UI."""

from __future__ import annotations

import asyncio
import json

from fastapi import Request
from fastapi.responses import StreamingResponse

from inandout.simulator.events import EventBus


async def sse_endpoint(request: Request) -> StreamingResponse:
    """Stream simulator events to connected browsers.

    Each event is a JSON object on the ``mutation`` or ``request`` channel.
    A 20-second keep-alive comment is sent when no events arrive, preventing
    browser-side SSE reconnection timeouts.
    """
    event_bus: EventBus = request.app.state.event_bus
    queue = event_bus.subscribe()

    async def generate():
        # Replay recent history so newly-opened tabs see context immediately.
        for ev in event_bus.recent(limit=30):
            yield ev.to_sse()
        try:
            while not await request.is_disconnected():
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=3.0)
                    yield event.to_sse()
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            event_bus.unsubscribe(queue)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
