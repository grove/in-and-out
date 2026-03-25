"""Token-bucket rate limiter for connector HTTP calls with adaptive adjustment."""
from __future__ import annotations

import time

import anyio
import structlog

logger = structlog.get_logger(__name__)


class TokenBucket:
    """Token-bucket rate limiter with adaptive rate adjustment (T1 #18).

    Supports dynamic rate reduction in response to Retry-After headers from
    429 responses. When the API signals a rate limit, the bucket temporarily
    reduces its refill rate to honor the requested backoff period, then
    gradually recovers to the original configured rate.

    Parameters
    ----------
    rate:
        Tokens added per second (base rate).
    capacity:
        Maximum number of tokens the bucket can hold (burst limit).
    """

    def __init__(self, rate: float, capacity: float) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._base_rate = rate
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        # Adaptive rate limiting state
        self._rate_reduction_until: float | None = None  # monotonic timestamp
        self._reduced_rate: float | None = None

    def _refill(self) -> None:
        now = time.monotonic()
        # Check if rate reduction period has expired
        if self._rate_reduction_until is not None and now >= self._rate_reduction_until:
            self._rate = self._base_rate
            self._rate_reduction_until = None
            self._reduced_rate = None
            logger.info("rate_limit_recovery", rate=self._base_rate)
        
        # Use current (possibly reduced) rate
        effective_rate = self._reduced_rate if self._reduced_rate is not None else self._rate
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * effective_rate)
        self._last_refill = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until *tokens* are available in the bucket."""
        while True:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return
            # Compute how long to sleep until enough tokens are available.
            effective_rate = self._reduced_rate if self._reduced_rate is not None else self._rate
            deficit = tokens - self._tokens
            sleep_secs = deficit / effective_rate
            await anyio.sleep(sleep_secs)

    def apply_backoff(self, retry_after_secs: float) -> None:
        """Temporarily reduce rate in response to a Retry-After header.

        Reduces the refill rate to 50% of the base rate for the duration
        specified by retry_after_secs, then gradually recovers.

        Parameters
        ----------
        retry_after_secs:
            Duration (seconds) to apply reduced rate, typically from Retry-After header.
        """
        if retry_after_secs <= 0:
            return
        
        # Reduce rate to 50% of base rate during backoff period
        self._reduced_rate = self._base_rate * 0.5
        self._rate_reduction_until = time.monotonic() + retry_after_secs
        
        # Drain tokens to force immediate throttling
        self._tokens = min(self._tokens, self._capacity * 0.1)
        
        logger.info(
            "rate_limit_backoff_applied",
            base_rate=self._base_rate,
            reduced_rate=self._reduced_rate,
            duration_secs=retry_after_secs,
        )


# Module-level registry: connector_name → TokenBucket
_buckets: dict[str, TokenBucket] = {}

# Per-tenant rate limiters: (connector_name, account_id) → TokenBucket
_tenant_buckets: dict[tuple[str, str], TokenBucket] = {}


def get_rate_limiter(
    connector_name: str,
    rate_per_second: float,
    burst: float,
    account_id: str | None = None,
) -> TokenBucket:
    """Return (creating if needed) the token bucket for connector or tenant.
    
    If account_id is provided, returns a per-tenant rate limiter.
    Otherwise returns the connector-level rate limiter.
    """
    if account_id:
        key = (connector_name, account_id)
        if key not in _tenant_buckets:
            _tenant_buckets[key] = TokenBucket(rate=rate_per_second, capacity=burst)
        return _tenant_buckets[key]
    
    if connector_name not in _buckets:
        _buckets[connector_name] = TokenBucket(rate=rate_per_second, capacity=burst)
    return _buckets[connector_name]


def reset_all() -> None:
    """Clear all buckets — used in tests."""
    _buckets.clear()
    _tenant_buckets.clear()
