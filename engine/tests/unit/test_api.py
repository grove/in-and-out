"""Unit tests for the runtime management API."""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import FastAPI


def make_app(pool: Any = None) -> FastAPI:
    """Build a FastAPI app with the management API router for testing."""
    from inandout.api import build_api_router
    from inandout.api.routes import _set_pool
    app = FastAPI()
    router = build_api_router(pool=pool)
    # Ensure pool is explicitly set (None or mock) to prevent test bleed
    _set_pool(pool)
    app.include_router(router)
    return app


def make_mock_pool() -> Any:
    """Create a mock pool with a mock connection context manager."""
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_cursor.fetchone = AsyncMock(return_value=None)
    mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.commit = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    return mock_pool, mock_cursor, mock_conn


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_health_endpoint():
    app = make_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# GET /connectors returns list
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_list_connectors_empty_with_no_pool():
    """Without a pool, returns empty list."""
    app = make_app(pool=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.anyio
async def test_list_connectors_with_pool():
    """With a pool that returns sync rows, returns connector summaries."""
    pool, cursor, conn = make_mock_pool()

    # Simulate two rows from inout_ops_sync_run
    import datetime
    cursor.fetchall = AsyncMock(return_value=[
        ("hubspot", "contacts", "completed", datetime.datetime(2024, 1, 1, 10, 0, 0)),
        ("hubspot", "deals", "completed", datetime.datetime(2024, 1, 1, 10, 5, 0)),
    ])

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1  # grouped by connector
    assert data[0]["name"] == "hubspot"
    assert set(data[0]["datatypes"]) == {"contacts", "deals"}


# ---------------------------------------------------------------------------
# POST /connectors/{connector}/datatypes/{datatype}/force-sync inserts control command
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_force_sync_inserts_control_command():
    """POST force-sync inserts force_full_sync into control table."""
    pool, cursor, conn = make_mock_pool()

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.post("/connectors/hubspot/datatypes/contacts/force-sync")

    assert resp.status_code == 200
    data = resp.json()
    assert data["command"] == "force_full_sync"
    assert data["connector"] == "hubspot"
    assert data["datatype"] == "contacts"
    assert "id" in data

    # Verify DB insert was called
    conn.execute.assert_called()
    conn.commit.assert_called()


@pytest.mark.anyio
async def test_pause_inserts_control_command():
    pool, cursor, conn = make_mock_pool()

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.post("/connectors/salesforce/datatypes/accounts/pause")

    assert resp.status_code == 200
    data = resp.json()
    assert data["command"] == "pause_connector"
    assert data["connector"] == "salesforce"


@pytest.mark.anyio
async def test_resume_inserts_control_command():
    pool, cursor, conn = make_mock_pool()

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.post("/connectors/salesforce/datatypes/accounts/resume")

    assert resp.status_code == 200
    data = resp.json()
    assert data["command"] == "resume_connector"


# ---------------------------------------------------------------------------
# GET /dead-letter/{connector}/{datatype} returns rows
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_dead_letter_returns_rows():
    pool, cursor, conn = make_mock_pool()
    import datetime

    cursor.fetchall = AsyncMock(return_value=[
        (1, "ext-001", "could not extract primary key", "data_error",
         datetime.datetime(2024, 1, 1, 9, 0, 0), 0),
        (2, "ext-002", "http 500", "transient_error",
         datetime.datetime(2024, 1, 1, 9, 5, 0), 1),
    ])

    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/dead-letter/hubspot/contacts")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["id"] == 1
    assert data[0]["external_id"] == "ext-001"
    assert data[1]["requeue_count"] == 1


@pytest.mark.anyio
async def test_dead_letter_returns_empty_without_pool():
    app = make_app(pool=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/dead-letter/hubspot/contacts")
    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /connectors/{connector}/datatypes/{datatype}/status
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_datatype_status_no_pool():
    app = make_app(pool=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.get("/connectors/hubspot/datatypes/contacts/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["connector"] == "hubspot"
    assert data["datatype"] == "contacts"


# ---------------------------------------------------------------------------
# POST /dead-letter/{connector}/{datatype}/requeue
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_requeue_dead_letter():
    pool, cursor, conn = make_mock_pool()
    app = make_app(pool=pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resp = await client.post("/dead-letter/hubspot/contacts/requeue")
    assert resp.status_code == 200
    data = resp.json()
    assert data["command"] == "requeue_dead_letter"
    assert data["connector"] == "hubspot"
    assert data["datatype"] == "contacts"
