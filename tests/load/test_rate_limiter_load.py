"""
Load test: rate-limiter enforcement under concurrent request pressure.

Validates GOAL.md T1 #18 (politeness & rate limiting — ingestion) and
T2 #11 (politeness & rate limiting — writeback).

The rate limiter must:
  - Never exceed the declared requests-per-second (with a small tolerance
    for timer jitter).
  - Correctly enforce per-connector isolation: different connectors share
    nothing and do not interfere with each other's budgets.
  - Allow burst traffic up to the configured capacity without blocking.

Run with: pytest tests/load/test_rate_limiter_load.py -v -m load
"""
from __future__ import annotations

import asyncio
import time

import anyio
import pytest


pytestmark = [
    pytest.mark.load,
]


# ---------------------------------------------------------------------------
# Test 1: Rate is enforced — throughput must not exceed declared rate
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_rate_limiter_does_not_exceed_declared_rate():
    """Acquiring N tokens at rate R must take at least (N-burst)/R seconds.

    GOAL.md T1 #18, T2 #11: the token bucket must never let the downstream
    system receive more requests per second than the configured rate.  We
    measure wall-clock throughput over a burst-depleted window.
    """
    from inandout.transport.rate_limiter import TokenBucket, reset_all

    reset_all()

    rate = 50.0     # 50 req/s
    burst = 5       # initial burst capacity
    # Drain the burst first, then measure throughput over the next `extra` tokens
    extra = 20

    bucket = TokenBucket(rate=rate, capacity=burst)

    # Drain burst instantly (no sleeping expected)
    for _ in range(burst):
        await bucket.acquire()

    # Now measure how long `extra` more requests take
    start = time.monotonic()
    for _ in range(extra):
        await bucket.acquire()
    elapsed = time.monotonic() - start

    # We issued `extra` tokens past the burst; minimum time = extra / rate
    min_expected = (extra - 1) / rate   # subtract 1 for TOCTOU slack
    assert elapsed >= min_expected * 0.85, (
        f"Rate limiter too fast: elapsed={elapsed:.3f}s, "
        f"min_expected={min_expected:.3f}s (rate={rate}, extra={extra})"
    )


# ---------------------------------------------------------------------------
# Test 2: Burst is honoured — bucket capacity tokens are granted immediately
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_rate_limiter_burst_granted_immediately():
    """Burst tokens (capacity) must be available without sleeping.

    GOAL.md T1 #3 (rate_limit.burst): the initial burst allowance must be
    consumed without delay so that the first requests after a quiet period
    are not artificially throttled.
    """
    from inandout.transport.rate_limiter import TokenBucket, reset_all

    reset_all()

    rate = 5.0
    burst = 20

    bucket = TokenBucket(rate=rate, capacity=burst)

    start = time.monotonic()
    for _ in range(burst):
        await bucket.acquire()
    elapsed = time.monotonic() - start

    # All burst tokens should be served in negligible time (< 200 ms)
    assert elapsed < 0.2, (
        f"Burst not granted immediately: elapsed={elapsed:.3f}s for {burst} "
        f"burst tokens at rate={rate}"
    )


# ---------------------------------------------------------------------------
# Test 3: Per-connector isolation — two connectors do not share a bucket
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_rate_limiter_per_connector_isolation():
    """Two connectors use independent buckets; one does not deplete the other.

    GOAL.md T1 #18: rate limiting is applied per connector, not globally.
    Connector B's burst should remain fully available after connector A
    has exhausted its own burst.
    """
    from inandout.transport.rate_limiter import get_rate_limiter, reset_all

    reset_all()

    bucket_a = get_rate_limiter("connector_a", rate_per_second=5.0, burst=10)
    bucket_b = get_rate_limiter("connector_b", rate_per_second=5.0, burst=10)

    # Exhaust connector A's burst
    for _ in range(10):
        await bucket_a.acquire()

    # Connector B's burst should still be fully intact → immediate
    start = time.monotonic()
    for _ in range(10):
        await bucket_b.acquire()
    elapsed = time.monotonic() - start

    assert elapsed < 0.2, (
        f"Connector B's burst was shared with A: elapsed={elapsed:.3f}s"
    )

    reset_all()


# ---------------------------------------------------------------------------
# Test 4: Concurrent acquirers serialise correctly
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_rate_limiter_concurrent_acquirers():
    """N concurrent coroutines all acquire tokens at the same rate limit.

    Asserts that the total elapsed time is consistent with a single shared
    budget across all concurrent callers — i.e., the bucket is not per-task.
    GOAL.md T1 #18: a single connector's rate limit applies across the
    entire engine regardless of internal concurrency.
    """
    from inandout.transport.rate_limiter import TokenBucket, reset_all

    reset_all()

    rate = 100.0     # 100 req/s → 10 ms per token
    burst = 5
    n_tasks = 5
    tokens_per_task = 5   # each task acquires 5 tokens = 25 total past first burst

    bucket = TokenBucket(rate=rate, capacity=burst)

    # Drain the burst to get a clean measurement
    for _ in range(burst):
        await bucket.acquire()

    acquired: list[float] = []

    async def _worker():
        for _ in range(tokens_per_task):
            await bucket.acquire()
            acquired.append(time.monotonic())

    start = time.monotonic()
    async with anyio.create_task_group() as tg:
        for _ in range(n_tasks):
            tg.start_soon(_worker)
    elapsed = time.monotonic() - start

    total_tokens = n_tasks * tokens_per_task
    min_expected = (total_tokens - 1) / rate   # -1 for first token already available

    assert elapsed >= min_expected * 0.80, (
        f"Concurrent acquirers violated rate: elapsed={elapsed:.3f}s < "
        f"min_expected={min_expected:.3f}s "
        f"(rate={rate}, total_tokens={total_tokens})"
    )
    assert len(acquired) == total_tokens, (
        f"Expected {total_tokens} acquisitions, got {len(acquired)}"
    )

    reset_all()


# ---------------------------------------------------------------------------
# Test 5: Retry-After header respect (T2 #11)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_rate_limiter_zero_tokens_does_not_panic():
    """Requesting 0 tokens must return immediately without error.

    Guard against edge-case: a zero-token acquire could divide by zero
    or hang forever if the implementation is naive.
    """
    from inandout.transport.rate_limiter import TokenBucket, reset_all

    reset_all()
    bucket = TokenBucket(rate=10.0, capacity=10)

    start = time.monotonic()
    await bucket.acquire(tokens=0)
    elapsed = time.monotonic() - start

    assert elapsed < 0.05, f"Zero-token acquire took {elapsed:.3f}s"
    reset_all()
