"""Unit tests for RateLimitMiddleware._check_rate in ingestion/webhook_server.py."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from inandout.ingestion.webhook_server import RateLimitMiddleware


def _make_middleware(limit: int = 3) -> RateLimitMiddleware:
    app = MagicMock()
    return RateLimitMiddleware(app, rate_limit_per_minute=limit)


def test_first_request_allowed():
    m = _make_middleware(limit=5)
    allowed, _ = m._check_rate("10.0.0.1")
    assert allowed is True


def test_requests_up_to_limit_allowed():
    m = _make_middleware(limit=3)
    for _ in range(3):
        allowed, _ = m._check_rate("10.0.0.2")
        assert allowed is True


def test_request_over_limit_rejected():
    m = _make_middleware(limit=2)
    m._check_rate("10.0.0.3")
    m._check_rate("10.0.0.3")
    allowed, _ = m._check_rate("10.0.0.3")
    assert allowed is False


def test_different_ips_tracked_independently():
    m = _make_middleware(limit=1)
    allowed_a, _ = m._check_rate("10.0.0.4")
    allowed_b, _ = m._check_rate("10.0.0.5")
    assert allowed_a is True
    assert allowed_b is True


def test_rejected_returns_retry_after_positive():
    m = _make_middleware(limit=1)
    m._check_rate("10.0.0.6")
    allowed, retry_after = m._check_rate("10.0.0.6")
    assert allowed is False
    assert retry_after >= 1


def test_window_prunes_old_entries():
    m = _make_middleware(limit=2)
    ip = "10.0.0.7"
    # Inject a timestamp that is older than 60 seconds
    old_ts = time.monotonic() - 120
    m._windows[ip] = [old_ts, old_ts]
    # After pruning, the window should be empty → request allowed
    allowed, _ = m._check_rate(ip)
    assert allowed is True


def test_limit_one_blocks_second_request():
    m = _make_middleware(limit=1)
    ip = "10.0.0.8"
    m._check_rate(ip)
    allowed, _ = m._check_rate(ip)
    assert allowed is False


def test_return_is_tuple():
    m = _make_middleware(limit=5)
    result = m._check_rate("10.0.0.9")
    assert isinstance(result, tuple)
    assert len(result) == 2
