"""Unit tests for Step 48 — API filtering and search."""
from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import FastAPI


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def make_app(pool: Any = None) -> FastAPI:
    from inandout.api import build_api_router
    from inandout.api.routes import _set_pool
    app = FastAPI()
    router = build_api_router(pool=pool)
    _set_pool(pool)
    app.include_router(router)
    return app


def make_mock_pool_with_rows(rows: list) -> Any:
    """Return a mock pool whose fetchall always returns the given rows."""
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=rows)
    mock_cursor.fetchone = AsyncMock(return_value=None)
    mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.commit = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)
    return mock_pool, mock_cursor


# ---------------------------------------------------------------------------
# test_connectors_filter_by_status
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_connectors_filter_by_status():
    """?status=healthy filters connectors by health score bracket."""
    now = datetime.datetime.now(datetime.timezone.utc)
    pool, cursor = make_mock_pool_with_rows([
        ("hubspot", "contacts", "completed", now),
    ])

    # Patch circuit breaker to return closed (healthy) state
    from inandout.transport.circuit_breaker import get_circuit_breaker
    cb = get_circuit_breaker("hubspot", "contacts")
    from inandout.transport.circuit_breaker import CircuitState
    cb._state = CircuitState.closed  # type: ignore[attr-defined]

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors?status=healthy")

    assert resp.status_code == 200
    data = resp.json()
    # With CB closed, score=1.0 → healthy
    assert any(c["name"] == "hubspot" for c in data)


@pytest.mark.anyio
async def test_connectors_filter_by_status_unhealthy():
    """?status=unhealthy filters out healthy connectors."""
    now = datetime.datetime.now(datetime.timezone.utc)
    pool, cursor = make_mock_pool_with_rows([
        ("hubspot", "contacts", "completed", now),
    ])

    from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
    cb = get_circuit_breaker("hubspot", "contacts")
    cb._state = CircuitState.closed  # healthy

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors?status=unhealthy")

    assert resp.status_code == 200
    data = resp.json()
    # hubspot is healthy, so should NOT appear in unhealthy filter
    assert not any(c["name"] == "hubspot" for c in data)


# ---------------------------------------------------------------------------
# test_connectors_filter_by_glob
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_connectors_filter_by_glob():
    """?connector=hub* matches hubspot but not salesforce."""
    now = datetime.datetime.now(datetime.timezone.utc)
    pool, cursor = make_mock_pool_with_rows([
        ("hubspot", "contacts", "completed", now),
        ("salesforce", "accounts", "completed", now),
    ])

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors?connector=hub*")

    assert resp.status_code == 200
    data = resp.json()
    names = [c["name"] for c in data]
    assert "hubspot" in names
    assert "salesforce" not in names


@pytest.mark.anyio
async def test_connectors_filter_by_glob_no_match():
    """?connector=stripe* returns empty list when no match."""
    now = datetime.datetime.now(datetime.timezone.utc)
    pool, cursor = make_mock_pool_with_rows([
        ("hubspot", "contacts", "completed", now),
    ])

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors?connector=stripe*")

    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# test_sync_runs_filter_by_status
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sync_runs_filter_by_status():
    """GET /sync-runs?status=failed returns only failed runs."""
    import uuid
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = str(uuid.uuid4())

    pool, cursor = make_mock_pool_with_rows([
        (run_id, "hubspot", "contacts", "full", "failed", now, None, 0, 0, 0, 5, "connection refused"),
    ])

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/sync-runs?status=failed")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["status"] == "failed"
    assert data[0]["connector"] == "hubspot"


@pytest.mark.anyio
async def test_sync_runs_no_pool():
    """GET /sync-runs without pool returns empty list."""
    app = make_app(pool=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/sync-runs")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.anyio
async def test_sync_runs_default_limit():
    """GET /sync-runs default limit is 20."""
    pool, cursor = make_mock_pool_with_rows([])
    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/sync-runs")

    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# test_dead_letter_limit
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_dead_letter_limit():
    """?limit=5 sends limit param to the query."""
    now = datetime.datetime.now(datetime.timezone.utc)
    rows = [
        (i, f"ext-{i}", "error", "data_error", now, 0)
        for i in range(1, 6)
    ]
    pool, cursor = make_mock_pool_with_rows(rows)

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/dead-letter/hubspot/contacts?limit=5")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 5


@pytest.mark.anyio
async def test_dead_letter_limit_max_100():
    """?limit=200 is rejected (exceeds max 100)."""
    pool, cursor = make_mock_pool_with_rows([])
    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/dead-letter/hubspot/contacts?limit=200")

    assert resp.status_code == 422


@pytest.mark.anyio
async def test_dead_letter_since_invalid():
    """?since=badval returns 422."""
    pool, cursor = make_mock_pool_with_rows([])
    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/dead-letter/hubspot/contacts?since=badval")

    assert resp.status_code == 422
