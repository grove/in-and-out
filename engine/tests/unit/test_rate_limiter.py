"""Unit tests for the token-bucket rate limiter."""
from __future__ import annotations

import pytest

from inandout.transport.rate_limiter import TokenBucket, get_rate_limiter, reset_all


@pytest.fixture(autouse=True)
def clear_buckets():
    reset_all()
    yield
    reset_all()


# ---------------------------------------------------------------------------
# acquire does not sleep when tokens are available
# ---------------------------------------------------------------------------

async def test_acquire_does_not_sleep_when_tokens_available(monkeypatch):
    sleep_called = False

    async def _mock_sleep(secs: float) -> None:
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr("inandout.transport.rate_limiter.anyio.sleep", _mock_sleep)

    bucket = TokenBucket(rate=10.0, capacity=10.0)
    await bucket.acquire()
    assert not sleep_called, "sleep should not be called when tokens are available"


# ---------------------------------------------------------------------------
# acquire sleeps when bucket is empty
# ---------------------------------------------------------------------------

async def test_acquire_sleeps_when_bucket_empty(monkeypatch):
    sleep_args: list[float] = []

    async def _mock_sleep(secs: float) -> None:
        sleep_args.append(secs)
        # After sleeping, simulate that enough time passed to refill the bucket.
        # We patch _last_refill so that the next _refill() call adds tokens.
        import time
        bucket._last_refill = time.monotonic() - (secs + 0.1)

    bucket = TokenBucket(rate=1.0, capacity=1.0)
    # Drain the bucket completely.
    bucket._tokens = 0.0

    monkeypatch.setattr("inandout.transport.rate_limiter.anyio.sleep", _mock_sleep)

    await bucket.acquire()
    assert len(sleep_args) >= 1, "sleep should be called at least once when bucket is empty"
    assert sleep_args[0] > 0, "sleep duration must be positive"


# ---------------------------------------------------------------------------
# burst allows N rapid requests
# ---------------------------------------------------------------------------

async def test_burst_allows_n_rapid_requests(monkeypatch):
    sleep_called = False

    async def _mock_sleep(secs: float) -> None:
        nonlocal sleep_called
        sleep_called = True

    monkeypatch.setattr("inandout.transport.rate_limiter.anyio.sleep", _mock_sleep)

    # rate=1 token/s but burst=5 means we can immediately consume 5 tokens.
    bucket = TokenBucket(rate=1.0, capacity=5.0)

    for _ in range(5):
        await bucket.acquire(1.0)

    assert not sleep_called, "burst capacity should allow 5 rapid requests without sleeping"
    # The 6th acquire must sleep (bucket is now empty).
    sleep_called = False

    # Drain: bucket has 0 tokens left
    async def _mock_sleep2(secs: float) -> None:
        nonlocal sleep_called
        sleep_called = True
        import time
        bucket._last_refill = time.monotonic() - (secs + 0.1)

    monkeypatch.setattr("inandout.transport.rate_limiter.anyio.sleep", _mock_sleep2)
    await bucket.acquire()
    assert sleep_called, "6th request should sleep"


# ---------------------------------------------------------------------------
# get_rate_limiter returns same instance
# ---------------------------------------------------------------------------

def test_get_rate_limiter_returns_same_instance():
    b1 = get_rate_limiter("connector_a", rate_per_second=5.0, burst=10.0)
    b2 = get_rate_limiter("connector_a", rate_per_second=5.0, burst=10.0)
    assert b1 is b2


def test_get_rate_limiter_different_connectors():
    b1 = get_rate_limiter("conn_x", rate_per_second=1.0, burst=2.0)
    b2 = get_rate_limiter("conn_y", rate_per_second=1.0, burst=2.0)
    assert b1 is not b2


# ---------------------------------------------------------------------------
# constructor validation
# ---------------------------------------------------------------------------

def test_token_bucket_invalid_rate():
    with pytest.raises(ValueError, match="rate"):
        TokenBucket(rate=0.0, capacity=10.0)


def test_token_bucket_invalid_capacity():
    with pytest.raises(ValueError, match="capacity"):
        TokenBucket(rate=1.0, capacity=0.0)
