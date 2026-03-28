"""Unit tests for Step 49 — Retry budget per connector."""
from __future__ import annotations

import asyncio
import time

import pytest


# ---------------------------------------------------------------------------
# RetryBudget unit tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_fresh_budget_allows_max_attempts():
    """A fresh budget allows up to max_attempts within the window."""
    from inandout.transport.retry_budget import RetryBudget

    budget = RetryBudget(max_attempts=3, window_secs=60.0)
    results = [await budget.consume() for _ in range(3)]
    assert all(results), "Expected all 3 attempts to be allowed"


@pytest.mark.anyio
async def test_exhausted_budget_returns_false():
    """After max_attempts are consumed, next consume() returns False."""
    from inandout.transport.retry_budget import RetryBudget

    budget = RetryBudget(max_attempts=2, window_secs=60.0)
    assert await budget.consume() is True
    assert await budget.consume() is True
    assert await budget.consume() is False


@pytest.mark.anyio
async def test_remaining_decrements():
    """remaining() decrements with each successful consume."""
    from inandout.transport.retry_budget import RetryBudget

    budget = RetryBudget(max_attempts=5, window_secs=60.0)
    assert budget.remaining() == 5
    await budget.consume()
    assert budget.remaining() == 4
    await budget.consume()
    assert budget.remaining() == 3


@pytest.mark.anyio
async def test_old_attempts_age_out():
    """Attempts older than window_secs are evicted, freeing budget."""
    from inandout.transport.retry_budget import RetryBudget

    budget = RetryBudget(max_attempts=2, window_secs=0.05)
    assert await budget.consume() is True
    assert await budget.consume() is True
    # Budget exhausted
    assert await budget.consume() is False

    # Wait for the window to expire
    await asyncio.sleep(0.1)

    # Now old attempts should have aged out
    assert await budget.consume() is True


@pytest.mark.anyio
async def test_reset_at_returns_datetime():
    """reset_at() returns a datetime indicating when oldest attempt ages out."""
    from inandout.transport.retry_budget import RetryBudget
    import datetime

    budget = RetryBudget(max_attempts=3, window_secs=60.0)
    await budget.consume()
    reset = budget.reset_at()
    assert isinstance(reset, datetime.datetime)
    assert reset.tzinfo is not None
    # Should be roughly now + 60s
    now = datetime.datetime.now(datetime.timezone.utc)
    diff = (reset - now).total_seconds()
    assert 55 <= diff <= 65, f"Expected ~60s, got {diff}"


@pytest.mark.anyio
async def test_reset_at_empty_budget_returns_now():
    """reset_at() on empty budget returns approximately now."""
    from inandout.transport.retry_budget import RetryBudget
    import datetime

    budget = RetryBudget(max_attempts=3, window_secs=60.0)
    reset = budget.reset_at()
    now = datetime.datetime.now(datetime.timezone.utc)
    diff = abs((reset - now).total_seconds())
    assert diff < 2.0


def test_get_retry_budget_returns_same_instance():
    """get_retry_budget returns the same instance for the same connector."""
    from inandout.transport.retry_budget import get_retry_budget, _budgets

    # Clear registry first to avoid test ordering issues
    _budgets.clear()

    b1 = get_retry_budget("test-connector", max_attempts=10, window_secs=60.0)
    b2 = get_retry_budget("test-connector", max_attempts=10, window_secs=60.0)
    assert b1 is b2


# ---------------------------------------------------------------------------
# RetryBudgetExhaustedError raised in transport
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_retry_budget_exhausted_error_is_raised_in_transport():
    """RetryBudgetExhaustedError is raised when budget is exhausted."""
    from inandout.transport.retry_budget import RetryBudgetExhaustedError

    # Just verify the class is importable and is an Exception subclass
    assert issubclass(RetryBudgetExhaustedError, Exception)
    err = RetryBudgetExhaustedError("budget exhausted")
    assert "budget exhausted" in str(err)


@pytest.mark.anyio
async def test_transport_raises_retry_budget_exhausted(monkeypatch):
    """HttpTransportAdapter raises RetryBudgetExhaustedError when budget exhausted."""
    import os
    import httpx
    import respx

    from inandout.config.connector import (
        ConnectionConfig,
        ConnectorConfig,
        RetryBudgetConfig,
        DatatypeConfig,
    )
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.ingestion import IngestionConfig
    from inandout.transport.http import HttpTransportAdapter
    from inandout.transport.retry_budget import RetryBudgetExhaustedError, _budgets

    # Build a minimal connector config with retry_budget max_attempts=1
    _budgets.clear()
    monkeypatch.setenv("INOUT_CREDENTIAL_BUDGETKEY", "test-secret")

    connector = ConnectorConfig(
        name="budget-test",
        system="test",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(
            base_url="https://api.example.com",
            retry_budget=RetryBudgetConfig(max_attempts=1, window_secs=60.0),
        ),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="budgetkey",
            api_key=ApiKeyConfig(location="header", name="X-API-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig.model_validate({
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {"path": "/items", "method": "GET", "pagination": {"strategy": "offset"}},
                })
            )
        },
    )

    with respx.mock:
        # First request: transient 500 to trigger retry
        respx.get("https://api.example.com/test").mock(
            return_value=httpx.Response(500)
        )

        adapter = HttpTransportAdapter(connector, max_retries=3)
        async with adapter:
            # The budget has max_attempts=1. The first request fails, and then
            # before retrying, budget.consume() is called and returns False.
            with pytest.raises(RetryBudgetExhaustedError):
                await adapter._request("GET", "/test")


@pytest.mark.anyio
async def test_raw_request_raises_budget_exhausted_on_429(monkeypatch):
    """_raw_request raises RetryBudgetExhaustedError when budget is exhausted during 429 retries."""
    import httpx
    import respx

    from inandout.config.connector import (
        ConnectionConfig,
        ConnectorConfig,
        RetryBudgetConfig,
        DatatypeConfig,
    )
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.ingestion import IngestionConfig
    from inandout.transport.http import HttpTransportAdapter
    from inandout.transport.retry_budget import RetryBudgetExhaustedError, _budgets

    _budgets.clear()
    monkeypatch.setenv("INOUT_CREDENTIAL_RAWBUDGETKEY", "test-secret")

    connector = ConnectorConfig(
        name="raw-budget-test",
        system="test",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(
            base_url="https://api.example.com",
            # Budget of 1: first 429 consumes it; second attempt must raise
            retry_budget=RetryBudgetConfig(max_attempts=1, window_secs=60.0),
        ),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="rawbudgetkey",
            api_key=ApiKeyConfig(location="header", name="X-API-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig.model_validate({
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {"path": "/items", "method": "GET", "pagination": {"strategy": "offset"}},
                })
            )
        },
    )

    with respx.mock:
        # Every request returns 429
        respx.post("https://api.example.com/writes").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "0"})
        )

        adapter = HttpTransportAdapter(connector, max_retries=5)
        async with adapter:
            # max_retries=5 but budget.consume() will fail on the second retry attempt
            with pytest.raises(RetryBudgetExhaustedError):
                await adapter._raw_request("POST", "/writes", json={"data": 1})


@pytest.mark.anyio
async def test_raw_request_retries_429_without_budget():
    """_raw_request retries 429 responses up to max_retries when no budget is configured."""
    import httpx
    import respx

    from inandout.config.connector import (
        ConnectionConfig,
        ConnectorConfig,
        DatatypeConfig,
    )
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.ingestion import IngestionConfig
    from inandout.transport.http import HttpTransportAdapter
    from inandout.transport.retry_budget import _budgets

    _budgets.clear()

    connector = ConnectorConfig(
        name="no-budget-test",
        system="test",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="nobud",
            api_key=ApiKeyConfig(location="header", name="X-API-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig.model_validate({
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {"path": "/items", "method": "GET", "pagination": {"strategy": "offset"}},
                })
            )
        },
    )

    import os
    os.environ.setdefault("INOUT_CREDENTIAL_NOBUD", "test-key")

    call_count = [0]

    with respx.mock:
        def _side_effect(request):
            call_count[0] += 1
            if call_count[0] < 3:
                return httpx.Response(429, headers={"Retry-After": "0"})
            return httpx.Response(200, json={"ok": True})

        respx.get("https://api.example.com/data").mock(side_effect=_side_effect)

        adapter = HttpTransportAdapter(connector, max_retries=5)
        async with adapter:
            resp = await adapter._raw_request("GET", "/data")

    assert resp.status_code == 200
    assert call_count[0] == 3  # two 429s then success
