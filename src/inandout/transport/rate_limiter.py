"""Token-bucket rate limiter for connector HTTP calls."""
from __future__ import annotations

import time

import anyio


class TokenBucket:
    """Classic token-bucket rate limiter.

    Parameters
    ----------
    rate:
        Tokens added per second.
    capacity:
        Maximum number of tokens the bucket can hold (burst limit).
    """

    def __init__(self, rate: float, capacity: float) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until *tokens* are available in the bucket."""
        while True:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return
            # Compute how long to sleep until enough tokens are available.
            deficit = tokens - self._tokens
            sleep_secs = deficit / self._rate
            await anyio.sleep(sleep_secs)


# Module-level registry: connector_name → TokenBucket
_buckets: dict[str, TokenBucket] = {}


def get_rate_limiter(
    connector_name: str,
    rate_per_second: float,
    burst: float,
) -> TokenBucket:
    """Return (creating if needed) the token bucket for *connector_name*."""
    if connector_name not in _buckets:
        _buckets[connector_name] = TokenBucket(rate=rate_per_second, capacity=burst)
    return _buckets[connector_name]


def reset_all() -> None:
    """Clear all buckets — used in tests."""
    _buckets.clear()
