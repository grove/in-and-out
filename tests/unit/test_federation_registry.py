"""Unit tests for the federation registry module."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.federation.registry import (
    FederationRegistry,
    InstanceStatus,
    DEFAULT_STALE_AFTER_SECS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    instance_id: str = "inst-01",
    connector: str = "salesforce",
    datatype: str = "contacts",
    reported_at: datetime | None = None,
    health_score: float = 1.0,
    circuit_breaker_state: str = "closed",
    dead_letter_depth: int = 0,
) -> dict:
    if reported_at is None:
        reported_at = datetime.now(timezone.utc)
    return {
        "instance_id": instance_id,
        "namespace": "public",
        "connector": connector,
        "datatype": datatype,
        "health_score": health_score,
        "last_sync_at": None,
        "circuit_breaker_state": circuit_breaker_state,
        "dead_letter_depth": dead_letter_depth,
        "reported_at": reported_at,
    }


def _make_pool(rows: list[dict]) -> MagicMock:
    """Build a pool mock that returns the given rows via cursor."""
    col_names = list(rows[0].keys()) if rows else []
    tuples = [tuple(r.values()) for r in rows]

    cur = MagicMock()
    cur.description = [(name,) for name in col_names]
    cur.fetchall = AsyncMock(return_value=tuples)

    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=cur)

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


def _make_pool_empty() -> MagicMock:
    return _make_pool_rows([])


def _make_pool_rows(rows: list[dict]) -> MagicMock:
    if not rows:
        cur = MagicMock()
        cur.description = []
        cur.fetchall = AsyncMock(return_value=[])
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=cur)
        pool = MagicMock()
        pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
        return pool
    return _make_pool(rows)


# ---------------------------------------------------------------------------
# list_instances
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_instances_empty_returns_empty_list():
    pool = _make_pool_rows([])
    reg = FederationRegistry(pool)
    result = await reg.list_instances()
    assert result == []


@pytest.mark.anyio
async def test_list_instances_returns_instance_statuses():
    rows = [_make_row()]
    pool = _make_pool(rows)
    reg = FederationRegistry(pool, stale_after_secs=90)
    result = await reg.list_instances()
    assert len(result) == 1
    s = result[0]
    assert s.instance_id == "inst-01"
    assert s.connector == "salesforce"
    assert s.datatype == "contacts"
    assert s.health_score == 1.0
    assert s.circuit_breaker_state == "closed"
    assert s.is_alive is True


@pytest.mark.anyio
async def test_list_instances_stale_row_is_not_alive():
    old = datetime.now(timezone.utc) - timedelta(seconds=200)
    rows = [_make_row(reported_at=old)]
    pool = _make_pool(rows)
    reg = FederationRegistry(pool, stale_after_secs=90)
    result = await reg.list_instances()
    assert len(result) == 1
    assert result[0].is_alive is False


@pytest.mark.anyio
async def test_list_alive_instances_delegates_to_list_instances():
    rows = [_make_row()]
    pool = _make_pool(rows)
    reg = FederationRegistry(pool, stale_after_secs=90)
    result = await reg.list_alive_instances()
    # Should call list_instances with alive_only=True; result still processed
    assert len(result) == 1
    assert result[0].is_alive is True


@pytest.mark.anyio
async def test_list_instances_exception_returns_empty():
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("db gone")
    reg = FederationRegistry(pool)
    result = await reg.list_instances()
    assert result == []


# ---------------------------------------------------------------------------
# get_instance
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_instance_returns_matching_rows():
    rows = [
        _make_row(instance_id="inst-01", datatype="contacts"),
        _make_row(instance_id="inst-01", datatype="accounts"),
    ]
    pool = _make_pool(rows)
    reg = FederationRegistry(pool)
    result = await reg.get_instance("inst-01")
    assert len(result) == 2
    assert all(s.instance_id == "inst-01" for s in result)


@pytest.mark.anyio
async def test_get_instance_exception_returns_empty():
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("oops")
    reg = FederationRegistry(pool)
    result = await reg.get_instance("inst-dead")
    assert result == []


# ---------------------------------------------------------------------------
# evict_stale
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_evict_stale_returns_deleted_count():
    cur = MagicMock()
    cur.rowcount = 3

    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=cur)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    reg = FederationRegistry(pool, stale_after_secs=90)
    deleted = await reg.evict_stale()
    assert deleted == 3
    conn.commit.assert_called_once()


@pytest.mark.anyio
async def test_evict_stale_uses_3x_stale_window_by_default():
    executed_sqls: list[str] = []
    cur = MagicMock()
    cur.rowcount = 0

    conn = AsyncMock()

    async def capture_execute(sql, *args, **kwargs):
        executed_sqls.append(sql)
        return cur

    conn.execute = capture_execute
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    reg = FederationRegistry(pool, stale_after_secs=60)
    await reg.evict_stale()

    assert executed_sqls, "execute should have been called"
    # threshold = 60 * 3 = 180 seconds
    assert "180" in executed_sqls[0]


@pytest.mark.anyio
async def test_evict_stale_exception_returns_zero():
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("lost")
    reg = FederationRegistry(pool)
    result = await reg.evict_stale()
    assert result == 0


# ---------------------------------------------------------------------------
# DEFAULT_STALE_AFTER_SECS constant
# ---------------------------------------------------------------------------


def test_default_stale_after_secs():
    assert DEFAULT_STALE_AFTER_SECS == 90.0
