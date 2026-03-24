"""Unit tests for TokenBucket and get_rate_limiter in transport/rate_limiter.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import inandout.transport.rate_limiter as rl_mod
from inandout.transport.rate_limiter import TokenBucket, get_rate_limiter


# --- Constructor validation ---

def test_invalid_rate_zero_raises():
    with pytest.raises(ValueError, match="rate"):
        TokenBucket(rate=0, capacity=10)


def test_invalid_rate_negative_raises():
    with pytest.raises(ValueError, match="rate"):
        TokenBucket(rate=-1, capacity=10)


def test_invalid_capacity_zero_raises():
    with pytest.raises(ValueError, match="capacity"):
        TokenBucket(rate=1, capacity=0)


def test_invalid_capacity_negative_raises():
    with pytest.raises(ValueError, match="capacity"):
        TokenBucket(rate=1, capacity=-5)


def test_valid_bucket_created():
    tb = TokenBucket(rate=2.0, capacity=10.0)
    assert tb._rate == 2.0
    assert tb._capacity == 10.0


def test_initial_tokens_equal_capacity():
    tb = TokenBucket(rate=1.0, capacity=5.0)
    assert tb._tokens == 5.0


# --- _refill ---

def test_refill_does_not_exceed_capacity():
    tb = TokenBucket(rate=100.0, capacity=5.0)
    tb._tokens = 0.0
    # Simulate a large elapsed time; tokens should be capped at capacity
    import time
    tb._last_refill = time.monotonic() - 10  # 10 secs ago
    tb._refill()
    assert tb._tokens == pytest.approx(5.0, abs=0.01)


def test_refill_adds_tokens_proportionally():
    import time
    tb = TokenBucket(rate=10.0, capacity=100.0)
    tb._tokens = 0.0
    tb._last_refill = time.monotonic() - 1.0  # 1 second ago → expect ~10 tokens
    tb._refill()
    assert tb._tokens == pytest.approx(10.0, abs=0.5)


# --- acquire ---

async def test_acquire_consumes_token():
    tb = TokenBucket(rate=100.0, capacity=10.0)
    initial = tb._tokens
    await tb.acquire(1.0)
    assert tb._tokens == pytest.approx(initial - 1.0, abs=0.01)


async def test_acquire_multiple_tokens():
    tb = TokenBucket(rate=100.0, capacity=10.0)
    await tb.acquire(5.0)
    assert tb._tokens <= 5.0


async def test_acquire_sleeps_when_tokens_insufficient():
    tb = TokenBucket(rate=2.0, capacity=1.0)
    tb._tokens = 0.1  # Not enough for 1 token

    sleep_calls: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        # Simulate time passing by refilling the bucket
        tb._tokens = tb._capacity

    with patch.object(rl_mod.anyio, "sleep", side_effect=fake_sleep):
        await tb.acquire(1.0)

    assert len(sleep_calls) >= 1
    assert sleep_calls[0] > 0


# --- get_rate_limiter ---

def test_get_rate_limiter_returns_token_bucket(monkeypatch):
    monkeypatch.setattr(rl_mod, "_buckets", {})
    bucket = get_rate_limiter("conn_a", 5.0, 10.0)
    assert isinstance(bucket, TokenBucket)


def test_get_rate_limiter_same_connector_returns_same_instance(monkeypatch):
    monkeypatch.setattr(rl_mod, "_buckets", {})
    b1 = get_rate_limiter("conn_x", 5.0, 10.0)
    b2 = get_rate_limiter("conn_x", 5.0, 10.0)
    assert b1 is b2


def test_get_rate_limiter_different_connectors_different_instances(monkeypatch):
    monkeypatch.setattr(rl_mod, "_buckets", {})
    b1 = get_rate_limiter("conn_a", 5.0, 10.0)
    b2 = get_rate_limiter("conn_b", 5.0, 10.0)
    assert b1 is not b2
