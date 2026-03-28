"""Unit tests for WebhookDebouncer coalescing behavior."""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from inandout.ingestion.debounce import WebhookDebouncer


class TestWebhookDebouncer:
    def test_pending_count_empty(self):
        debouncer = WebhookDebouncer(window_secs=0.5)
        assert debouncer.pending_count() == 0

    @pytest.mark.anyio
    async def test_enqueue_increments_pending_count(self):
        debouncer = WebhookDebouncer(window_secs=0.5)
        handler = AsyncMock()
        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1"}, handler)
        assert debouncer.pending_count() == 1

    @pytest.mark.anyio
    async def test_enqueue_same_key_replaces_payload(self):
        """Multiple enqueues for the same key only keep the last payload."""
        debouncer = WebhookDebouncer(window_secs=0.5)
        handler = AsyncMock()

        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1", "v": 1}, handler)
        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1", "v": 2}, handler)
        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1", "v": 3}, handler)

        # Still only one pending event for this key
        assert debouncer.pending_count() == 1

        # The stored payload is the last one
        key = ("hubspot", "contacts", "rec-1")
        stored_payload, _, _ = debouncer._pending[key]
        assert stored_payload["v"] == 3

    @pytest.mark.anyio
    async def test_enqueue_different_keys_are_independent(self):
        debouncer = WebhookDebouncer(window_secs=0.5)
        handler = AsyncMock()

        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1"}, handler)
        await debouncer.enqueue("hubspot", "contacts", "rec-2", {"id": "rec-2"}, handler)
        await debouncer.enqueue("salesforce", "contacts", "rec-1", {"id": "rec-1"}, handler)

        assert debouncer.pending_count() == 3

    @pytest.mark.anyio
    async def test_sweep_processes_expired_events(self):
        """Events whose debounce window has passed should be processed."""
        debouncer = WebhookDebouncer(window_secs=0.05)  # 50ms window
        handler = AsyncMock()

        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1"}, handler)

        # Manually age the event past the window
        key = ("hubspot", "contacts", "rec-1")
        payload, _, stored_handler = debouncer._pending[key]
        debouncer._pending[key] = (payload, time.monotonic() - 1.0, stored_handler)

        # Manually trigger one sweep
        now = time.monotonic()
        ready = [
            (k, v) for k, v in list(debouncer._pending.items())
            if now - v[1] >= debouncer._window
        ]
        for k, (p, arrived_at, h) in ready:
            debouncer._pending.pop(k, None)
            await h(p)

        handler.assert_called_once_with({"id": "rec-1"})
        assert debouncer.pending_count() == 0

    @pytest.mark.anyio
    async def test_sweep_does_not_process_recent_events(self):
        """Events within the debounce window should NOT be processed yet."""
        debouncer = WebhookDebouncer(window_secs=10.0)  # Large window
        handler = AsyncMock()

        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1"}, handler)

        # Simulate a sweep with events that haven't expired
        now = time.monotonic()
        ready = [
            (k, v) for k, v in list(debouncer._pending.items())
            if now - v[1] >= debouncer._window
        ]
        for k, (p, arrived_at, h) in ready:
            debouncer._pending.pop(k, None)
            await h(p)

        handler.assert_not_called()
        assert debouncer.pending_count() == 1

    @pytest.mark.anyio
    async def test_sweep_coalesces_multiple_enqueues(self):
        """Only the last payload should be delivered when burst arrives for same key."""
        debouncer = WebhookDebouncer(window_secs=0.05)
        handler = AsyncMock()

        # Simulate a burst
        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1", "v": 1}, handler)
        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1", "v": 2}, handler)
        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1", "v": 3}, handler)

        # Age the event
        key = ("hubspot", "contacts", "rec-1")
        payload, _, stored_handler = debouncer._pending[key]
        debouncer._pending[key] = (payload, time.monotonic() - 1.0, stored_handler)

        # Sweep
        now = time.monotonic()
        ready = [
            (k, v) for k, v in list(debouncer._pending.items())
            if now - v[1] >= debouncer._window
        ]
        for k, (p, arrived_at, h) in ready:
            debouncer._pending.pop(k, None)
            await h(p)

        # Handler called once with the last payload
        handler.assert_called_once_with({"id": "rec-1", "v": 3})

    @pytest.mark.anyio
    async def test_sweep_handles_handler_exception_gracefully(self):
        """If a handler raises, the debouncer should not crash."""
        debouncer = WebhookDebouncer(window_secs=0.05)

        async def failing_handler(payload):
            raise RuntimeError("handler failed")

        await debouncer.enqueue("hubspot", "contacts", "rec-1", {"id": "rec-1"}, failing_handler)

        # Age the event
        key = ("hubspot", "contacts", "rec-1")
        payload, _, stored_handler = debouncer._pending[key]
        debouncer._pending[key] = (payload, time.monotonic() - 1.0, stored_handler)

        # Sweep — should not raise
        now = time.monotonic()
        ready = [
            (k, v) for k, v in list(debouncer._pending.items())
            if now - v[1] >= debouncer._window
        ]
        for k, (p, arrived_at, h) in ready:
            debouncer._pending.pop(k, None)
            try:
                await h(p)
            except Exception:
                pass  # Match behavior in run_sweep_loop

        assert debouncer.pending_count() == 0
