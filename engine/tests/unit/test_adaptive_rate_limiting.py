"""Unit tests for adaptive rate limiting."""
from __future__ import annotations

import time

import pytest

from inandout.transport.rate_limiter import TokenBucket


@pytest.mark.anyio
async def test_apply_backoff_reduces_rate():
    """Test that apply_backoff temporarily reduces the refill rate."""
    bucket = TokenBucket(rate=10.0, capacity=10.0)
    
    # Apply backoff for 0.2 seconds
    bucket.apply_backoff(0.2)
    
    # Rate should be reduced to 50% (5.0)
    assert bucket._reduced_rate == 5.0
    assert bucket._rate_reduction_until is not None


@pytest.mark.anyio
async def test_apply_backoff_drains_tokens():
    """Test that apply_backoff drains tokens to force throttling."""
    bucket = TokenBucket(rate=10.0, capacity=10.0)
    
    # Bucket starts with full capacity (10 tokens)
    assert bucket._tokens == 10.0
    
    # Apply backoff
    bucket.apply_backoff(0.5)
    
    # Tokens should be drained to 10% of capacity
    assert bucket._tokens <= 1.0


@pytest.mark.anyio
async def test_backoff_recovery_after_duration():
    """Test that rate recovers to base rate after backoff duration."""
    bucket = TokenBucket(rate=10.0, capacity=10.0)
    
    # Apply very short backoff (0.05 seconds)
    bucket.apply_backoff(0.05)
    assert bucket._reduced_rate == 5.0
    
    # Wait for backoff to expire
    await anyio.sleep(0.1)
    
    # Force refill to trigger recovery check
    bucket._refill()
    
    # Rate should recover to base rate
    assert bucket._rate == 10.0
    assert bucket._reduced_rate is None
    assert bucket._rate_reduction_until is None


@pytest.mark.anyio
async def test_acquire_uses_reduced_rate_during_backoff():
    """Test that acquire() uses reduced rate during backoff period."""
    bucket = TokenBucket(rate=100.0, capacity=10.0)
    
    # Drain all tokens
    await bucket.acquire(10.0)
    assert bucket._tokens == 0.0
    
    # Apply backoff (reduced rate = 50.0)
    bucket.apply_backoff(1.0)
    
    # Measure time to acquire 5 tokens
    # At reduced rate of 50.0 tokens/sec, 5 tokens should take ~0.1 seconds
    start = time.monotonic()
    await bucket.acquire(5.0)
    elapsed = time.monotonic() - start
    
    # Allow some tolerance for timing
    assert 0.08 < elapsed < 0.15


@pytest.mark.anyio
async def test_multiple_backoffs_extend_duration():
    """Test that multiple backoff calls extend the reduction period."""
    bucket = TokenBucket(rate=10.0, capacity=10.0)
    
    # First backoff
    bucket.apply_backoff(0.1)
    first_until = bucket._rate_reduction_until
    
    # Second backoff (should extend the duration)
    await anyio.sleep(0.05)
    bucket.apply_backoff(0.1)
    second_until = bucket._rate_reduction_until
    
    assert second_until is not None
    assert second_until > first_until


@pytest.mark.anyio
async def test_zero_or_negative_backoff_ignored():
    """Test that zero or negative backoff values are ignored."""
    bucket = TokenBucket(rate=10.0, capacity=10.0)
    
    # Apply invalid backoff values
    bucket.apply_backoff(0.0)
    assert bucket._reduced_rate is None
    
    bucket.apply_backoff(-5.0)
    assert bucket._reduced_rate is None


@pytest.mark.anyio
async def test_backoff_with_normal_acquire():
    """Test that normal acquire() still works during backoff."""
    bucket = TokenBucket(rate=10.0, capacity=10.0)
    
    # Apply backoff
    bucket.apply_backoff(0.5)
    
    # Should still be able to acquire tokens (just slower)
    await bucket.acquire(0.5)
    assert bucket._tokens < 1.0  # Some tokens were consumed


import anyio
