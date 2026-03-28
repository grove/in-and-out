"""Event buffer for out-of-order event reordering (T1 #35).

Buffers webhook events that arrive out of sequence and releases them in order
once the missing events arrive or a timeout expires.
"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class EventBuffer:
    """Buffer for reordering out-of-sequence events.
    
    Holds events keyed by (connector, datatype, external_id) and releases them
    in sequence order once gaps are filled or timeout expires.
    """

    def __init__(self, timeout_secs: float = 30.0, max_size: int = 100) -> None:
        self._timeout_secs = timeout_secs
        self._max_size = max_size
        # Buffer: (connector, datatype, external_id) -> {seq: (payload, arrival_time)}
        self._buffer: dict[tuple[str, str, str], dict[int, tuple[dict, float]]] = defaultdict(dict)
        # Expected next sequence per entity
        self._next_seq: dict[tuple[str, str, str], int] = {}
        # Oldest arrival time per entity (for timeout tracking)
        self._oldest: dict[tuple[str, str, str], float] = {}

    def buffer_event(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        sequence: int,
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Buffer an event and return any events ready for processing.
        
        Returns a list of payloads in sequence order if:
        - The sequence fills a gap and subsequent sequences are available
        - A timeout has expired and buffered events should be released
        - The buffer is full and oldest events must be evicted
        
        Returns:
            List of payloads ready to process (in sequence order)
        """
        key = (connector, datatype, external_id)
        now = time.monotonic()
        
        # Check if buffer is full for this entity
        if len(self._buffer[key]) >= self._max_size:
            logger.warning(
                "event_buffer_full_evicting_oldest",
                connector=connector,
                datatype=datatype,
                external_id=external_id,
                buffer_size=len(self._buffer[key]),
            )
            # Evict oldest, release everything, and add current
            buffered = self._flush_buffer(key)
            buffered.append(payload)
            self._next_seq[key] = sequence + 1
            return buffered
        
        # Check for timeout
        if key in self._oldest and (now - self._oldest[key]) >= self._timeout_secs:
            logger.info(
                "event_buffer_timeout_releasing",
                connector=connector,
                datatype=datatype,
                external_id=external_id,
                buffered_count=len(self._buffer[key]),
                timeout_secs=self._timeout_secs,
            )
            # Release everything in buffer and add current event
            buffered = self._flush_buffer(key)
            buffered.append(payload)
            self._next_seq[key] = sequence + 1
            return buffered
        
        # Get expected next sequence (default to 0 for new entity)
        expected_seq = self._next_seq.get(key, 0)
        
        # Case 1: Event is exactly what we're waiting for
        if sequence == expected_seq:
            result = [payload]
            self._next_seq[key] = expected_seq + 1
            
            # Check if we can release buffered events in sequence
            while self._next_seq[key] in self._buffer[key]:
                next_payload, _ = self._buffer[key].pop(self._next_seq[key])
                result.append(next_payload)
                self._next_seq[key] += 1
            
            # Clear oldest timestamp if buffer is now empty
            if not self._buffer[key]:
                self._oldest.pop(key, None)
            
            return result
        
        # Case 2: Event is ahead of what we're expecting - buffer it
        if sequence > expected_seq:
            self._buffer[key][sequence] = (payload, now)
            
            # Track oldest arrival if this is first buffered event
            if key not in self._oldest:
                self._oldest[key] = now
            
            logger.debug(
                "event_buffered_out_of_order",
                connector=connector,
                datatype=datatype,
                external_id=external_id,
                sequence=sequence,
                expected=expected_seq,
                buffered_count=len(self._buffer[key]),
            )
            return []  # Nothing ready yet
        
        # Case 3: Event is older than expected (duplicate or very late) - discard
        logger.info(
            "event_discarded_already_processed",
            connector=connector,
            datatype=datatype,
            external_id=external_id,
            sequence=sequence,
            expected=expected_seq,
        )
        return []

    def _flush_buffer(self, key: tuple[str, str, str]) -> list[dict[str, Any]]:
        """Flush all buffered events for a key in sequence order."""
        if key not in self._buffer or not self._buffer[key]:
            return []
        
        # Sort by sequence number and extract payloads
        sorted_items = sorted(self._buffer[key].items())
        result = [payload for seq, (payload, _) in sorted_items]
        
        # Update next expected sequence to after the highest buffered
        if sorted_items:
            highest_seq = sorted_items[-1][0]
            self._next_seq[key] = highest_seq + 1
        
        # Clear buffer and oldest timestamp
        self._buffer[key].clear()
        self._oldest.pop(key, None)
        
        return result

    def get_buffer_stats(self) -> dict[str, Any]:
        """Return statistics about current buffer state."""
        total_buffered = sum(len(events) for events in self._buffer.values())
        entity_count = len([k for k, v in self._buffer.items() if v])
        
        return {
            "total_buffered_events": total_buffered,
            "entities_with_buffered_events": entity_count,
            "max_buffer_age_secs": (
                max(time.monotonic() - oldest for oldest in self._oldest.values())
                if self._oldest else 0.0
            ),
        }


# Module-level buffer instance (shared across webhook handlers)
_global_buffer: EventBuffer | None = None


def get_event_buffer(timeout_secs: float = 30.0, max_size: int = 100) -> EventBuffer:
    """Get or create the global event buffer instance."""
    global _global_buffer
    if _global_buffer is None:
        _global_buffer = EventBuffer(timeout_secs, max_size)
    return _global_buffer


def reset_event_buffer() -> None:
    """Reset the global buffer (used in tests)."""
    global _global_buffer
    _global_buffer = None
