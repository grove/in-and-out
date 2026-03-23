"""Retry budget — rolling-window attempt throttle per connector."""
from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque


class RetryBudgetExhaustedError(Exception):
    """Raised when the retry budget for a connector is exhausted."""


class RetryBudget:
    """Rolling-window retry budget.

    Tracks attempt timestamps in a deque. Before consuming a slot the deque is
    pruned of entries older than ``window_secs``. If the remaining capacity is
    zero, ``consume()`` returns False.
    """

    def __init__(self, max_attempts: int, window_secs: float) -> None:
        self.max_attempts = max_attempts
        self.window_secs = window_secs
        self._timestamps: Deque[float] = deque()

    def _evict_old(self) -> None:
        cutoff = time.monotonic() - self.window_secs
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    async def consume(self) -> bool:
        """Try to consume one attempt slot. Returns True if allowed, False if exhausted."""
        self._evict_old()
        if len(self._timestamps) >= self.max_attempts:
            return False
        self._timestamps.append(time.monotonic())
        return True

    def remaining(self) -> int:
        """Attempts left in the current window."""
        self._evict_old()
        return max(0, self.max_attempts - len(self._timestamps))

    def reset_at(self) -> datetime:
        """When the oldest tracked attempt will age out, freeing a budget slot."""
        self._evict_old()
        if not self._timestamps:
            return datetime.now(timezone.utc)
        oldest = self._timestamps[0]
        reset_mono = oldest + self.window_secs
        delta = max(0.0, reset_mono - time.monotonic())
        import datetime as _dt
        return _dt.datetime.now(timezone.utc) + _dt.timedelta(seconds=delta)


# ---------------------------------------------------------------------------
# Module-level registry
# ---------------------------------------------------------------------------

_budgets: dict[str, RetryBudget] = {}


def get_retry_budget(
    connector_name: str,
    max_attempts: int,
    window_secs: float,
) -> RetryBudget:
    """Return (or create) the RetryBudget for a connector."""
    if connector_name not in _budgets:
        _budgets[connector_name] = RetryBudget(max_attempts, window_secs)
    return _budgets[connector_name]
