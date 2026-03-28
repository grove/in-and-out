"""Server-Sent Events streaming endpoint for the demo simulator UI."""

from __future__ import annotations

import asyncio
import json

from fastapi import Request
from fastapi.responses import StreamingResponse

from inandout_simulator.events import EventBus


async def sse_endpoint(request: Request) -> StreamingResponse:
    """Stream simulator events to connected browsers.

    Each event is a JSON object on the ``mutation`` or ``request`` channel.
    A 20-second keep-alive comment is sent when no events arrive, preventing
    browser-side SSE reconnection timeouts.
    """
    event_bus: EventBus = request.app.state.event_bus
    queue = event_bus.subscribe()

    async def generate():
        # NOTE: history is rendered server-side by the Jinja templates.
        # Replaying it here caused duplicates (Jinja row + SSE row for the same event).
        # SSE only pushes live events that arrive *after* the connection is established.
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
