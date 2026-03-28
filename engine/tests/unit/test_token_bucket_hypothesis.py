"""Property-based tests for TokenBucket using Hypothesis."""
from __future__ import annotations

import time
from unittest.mock import patch

from hypothesis import given, assume, settings
from hypothesis import strategies as st

import inandout.transport.rate_limiter as rl_mod
from inandout.transport.rate_limiter import TokenBucket


# --- _refill property: tokens never exceed capacity ---

@given(
    rate=st.floats(min_value=0.001, max_value=1000.0),
    capacity=st.floats(min_value=0.001, max_value=1000.0),
    elapsed=st.floats(min_value=0.0, max_value=3600.0),
    start_tokens=st.floats(min_value=0.0, max_value=1000.0),
)
def test_refill_never_exceeds_capacity(rate, capacity, elapsed, start_tokens):
    assume(start_tokens <= capacity)
    tb = TokenBucket(rate=rate, capacity=capacity)
    tb._tokens = start_tokens
    tb._last_refill = time.monotonic() - elapsed
    tb._refill()
    assert tb._tokens <= capacity + 1e-9  # tiny float tolerance


@given(
    rate=st.floats(min_value=0.001, max_value=1000.0),
    capacity=st.floats(min_value=0.001, max_value=1000.0),
    elapsed=st.floats(min_value=0.0, max_value=3600.0),
)
def test_refill_tokens_nonnegative(rate, capacity, elapsed):
    tb = TokenBucket(rate=rate, capacity=capacity)
    tb._tokens = 0.0
    tb._last_refill = time.monotonic() - elapsed
    tb._refill()
    assert tb._tokens >= 0.0


# --- acquire property: tokens never go below zero ---

@given(
    rate=st.floats(min_value=1.0, max_value=500.0),
    capacity=st.floats(min_value=1.0, max_value=500.0),
    acquire_amount=st.floats(min_value=0.01, max_value=10.0),
)
@settings(max_examples=100)
async def test_acquire_leaves_tokens_nonnegative(rate, capacity, acquire_amount):
    assume(acquire_amount <= capacity)

    sleep_calls: list[float] = []

    async def fast_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        # Simulate time passing: give the bucket back its capacity instantly
        _bucket._tokens = _bucket._capacity

    _bucket = TokenBucket(rate=rate, capacity=capacity)
    _bucket._tokens = capacity  # start full

    with patch.object(rl_mod.anyio, "sleep", side_effect=fast_sleep):
        await _bucket.acquire(acquire_amount)

    assert _bucket._tokens >= -1e-9  # allow tiny float rounding


# --- Constructor: rate/capacity preserved ---

@given(
    rate=st.floats(min_value=0.001, max_value=1e6),
    capacity=st.floats(min_value=0.001, max_value=1e6),
)
def test_token_bucket_stores_rate_and_capacity(rate, capacity):
    tb = TokenBucket(rate=rate, capacity=capacity)
    assert tb._rate == rate
    assert tb._capacity == capacity
    assert tb._tokens == capacity


# --- get_rate_limiter: same connector → same instance ---

@given(name=st.text(min_size=1, max_size=40))
@settings(max_examples=50)
def test_get_rate_limiter_idempotent(name):
    buckets: dict = {}
    with patch.object(rl_mod, "_buckets", buckets):
        b1 = rl_mod.get_rate_limiter(name, 1.0, 5.0)
        b2 = rl_mod.get_rate_limiter(name, 1.0, 5.0)
        assert b1 is b2
