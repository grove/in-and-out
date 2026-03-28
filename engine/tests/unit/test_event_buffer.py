"""Unit tests for out-of-order event buffer."""
from __future__ import annotations

import time

import pytest

from inandout.ingestion.event_buffer import EventBuffer, reset_event_buffer


@pytest.fixture(autouse=True)
def clear_buffer():
    reset_event_buffer()
    yield
    reset_event_buffer()


def test_in_order_events_pass_through():
    """Test that events arriving in order are immediately released."""
    buffer = EventBuffer()
    
    # Events in sequence should be released immediately
    result1 = buffer.buffer_event("conn", "dtype", "id1", 0, {"seq": 0, "data": "a"})
    assert len(result1) == 1
    assert result1[0]["data"] == "a"
    
    result2 = buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1, "data": "b"})
    assert len(result2) == 1
    assert result2[0]["data"] == "b"


def test_out_of_order_buffered():
    """Test that out-of-order events are buffered."""
    buffer = EventBuffer()
    
    # Event with seq=1 arrives before seq=0 - should be buffered
    result = buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1, "data": "b"})
    assert len(result) == 0  # Nothing ready yet


def test_gap_filled_releases_buffered():
    """Test that filling a gap releases buffered events."""
    buffer = EventBuffer()
    
    # Event 2 arrives first
    result1 = buffer.buffer_event("conn", "dtype", "id1", 2, {"seq": 2, "data": "c"})
    assert len(result1) == 0
    
    # Event 1 arrives
    result2 = buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1, "data": "b"})
    assert len(result2) == 0
    
    # Event 0 arrives and fills the gap - releases all 3
    result3 = buffer.buffer_event("conn", "dtype", "id1", 0, {"seq": 0, "data": "a"})
    assert len(result3) == 3
    assert result3[0]["data"] == "a"
    assert result3[1]["data"] == "b"
    assert result3[2]["data"] == "c"


def test_duplicate_event_discarded():
    """Test that duplicate events (old sequence numbers) are discarded."""
    buffer = EventBuffer()
    
    # Process events 0, 1, 2
    buffer.buffer_event("conn", "dtype", "id1", 0, {"seq": 0})
    buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1})
    buffer.buffer_event("conn", "dtype", "id1", 2, {"seq": 2})
    
    # Try to add event 1 again - should be discarded
    result = buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1, "data": "dup"})
    assert len(result) == 0


def test_timeout_releases_buffered():
    """Test that buffered events are released after timeout."""
    buffer = EventBuffer(timeout_secs=0.1, max_size=100)
    
    # Buffer event 2 (missing 0 and 1)
    result1 = buffer.buffer_event("conn", "dtype", "id1", 2, {"seq": 2, "data": "c"})
    assert len(result1) == 0
    
    # Wait for timeout
    time.sleep(0.15)
    
    # Next event triggers timeout check and releases buffered + new
    result2 = buffer.buffer_event("conn", "dtype", "id1", 5, {"seq": 5, "data": "f"})
    assert len(result2) == 2  # Both seq=2 and seq=5 released
    assert result2[0]["data"] == "c"
    assert result2[1]["data"] == "f"


def test_buffer_full_evicts_oldest():
    """Test that when buffer is full, oldest events are evicted."""
    buffer = EventBuffer(timeout_secs=30.0, max_size=3)
    
    # Fill buffer with events 1, 2, 3
    buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1})
    buffer.buffer_event("conn", "dtype", "id1", 2, {"seq": 2})
    buffer.buffer_event("conn", "dtype", "id1", 3, {"seq": 3})
    
    # Adding event 4 exceeds max_size - should flush and release all
    result = buffer.buffer_event("conn", "dtype", "id1", 4, {"seq": 4})
    assert len(result) == 4  # All 4 events released


def test_multiple_entities_isolated():
    """Test that buffering is isolated per entity."""
    buffer = EventBuffer()
    
    # Buffer event for entity id1
    result1 = buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1, "id": "id1"})
    assert len(result1) == 0
    
    # Event for different entity id2 should not be affected
    result2 = buffer.buffer_event("conn", "dtype", "id2", 0, {"seq": 0, "id": "id2"})
    assert len(result2) == 1
    assert result2[0]["id"] == "id2"
    
    # Entity id1 still buffered
    result3 = buffer.buffer_event("conn", "dtype", "id1", 0, {"seq": 0, "id": "id1"})
    assert len(result3) == 2
    assert result3[0]["id"] == "id1"
    assert result3[1]["id"] == "id1"


def test_buffer_stats():
    """Test buffer statistics reporting."""
    buffer = EventBuffer()
    
    # Buffer some events
    buffer.buffer_event("conn", "dtype", "id1", 2, {"seq": 2})
    buffer.buffer_event("conn", "dtype", "id1", 3, {"seq": 3})
    buffer.buffer_event("conn", "dtype", "id2", 5, {"seq": 5})
    
    stats = buffer.get_buffer_stats()
    assert stats["total_buffered_events"] == 3
    assert stats["entities_with_buffered_events"] == 2
    assert stats["max_buffer_age_secs"] >= 0.0


def test_partial_gap_fill():
    """Test that partially filling gaps releases only contiguous events."""
    buffer = EventBuffer()
    
    # Buffer events 2, 4, 5
    buffer.buffer_event("conn", "dtype", "id1", 2, {"seq": 2})
    buffer.buffer_event("conn", "dtype", "id1", 4, {"seq": 4})
    buffer.buffer_event("conn", "dtype", "id1", 5, {"seq": 5})
    
    # Event 0 arrives - releases 0, 1 is still missing
    result1 = buffer.buffer_event("conn", "dtype", "id1", 0, {"seq": 0})
    assert len(result1) == 1  # Only seq=0
    
    # Event 1 arrives - releases 1, 2 (but not 4, 5 because 3 is missing)
    result2 = buffer.buffer_event("conn", "dtype", "id1", 1, {"seq": 1})
    assert len(result2) == 2  # seq=1 and seq=2
    
    # Event 3 arrives - releases 3, 4, 5
    result3 = buffer.buffer_event("conn", "dtype", "id1", 3, {"seq": 3})
    assert len(result3) == 3  # seq=3, 4, 5


def test_non_sequential_start():
    """Test buffer when first event isn't sequence 0."""
    buffer = EventBuffer()
    
    # First event is seq=10 but buffer expects seq=0 - should be buffered
    result = buffer.buffer_event("conn", "dtype", "id1", 10, {"seq": 10})
    assert len(result) == 0  # Buffered, waiting for seq=0
    
    # Event seq=0 arrives - releases 0 but not 10 yet (missing 1-9)
    result2 = buffer.buffer_event("conn", "dtype", "id1", 0, {"seq": 0})
    assert len(result2) == 1  # Only seq=0
