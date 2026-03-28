"""Unit tests for tombstone_missing exactly-50% threshold behaviour.

The circuit breaker must NOT trip at exactly 50% (ratio == 0.5).
Only ratios strictly > 0.5 must prevent tombstoning.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.ingestion.engine import IngestionEngine, SyncResult


# ---------------------------------------------------------------------------
# Helper (mirrors test_tombstone_missing.py's _make_pool_with_rows)
# ---------------------------------------------------------------------------

def _make_result() -> SyncResult:
    return SyncResult(uuid.uuid4(), "hubspot", "contacts", "full")


def _make_pool_with_rows(rows: list[str]) -> tuple[MagicMock, AsyncMock]:
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "COUNT(*)" in sql:
            cur.fetchone = AsyncMock(return_value=(len(rows),))
        elif "SELECT external_id" in sql:
            cur.fetchall = AsyncMock(return_value=[(r,) for r in rows])
        else:
            cur.fetchone = AsyncMock(return_value=None)
            cur.fetchall = AsyncMock(return_value=[])
            cur.rowcount = 1
        return cur

    txn_cm = AsyncMock()
    txn_cm.__aenter__ = AsyncMock(return_value=None)
    txn_cm.__aexit__ = AsyncMock(return_value=False)

    inner = AsyncMock()
    inner.execute = AsyncMock(side_effect=_execute)
    inner.commit = AsyncMock()
    inner.transaction = MagicMock(return_value=txn_cm)
    inner.__aenter__ = AsyncMock(return_value=inner)
    inner.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.connection = MagicMock(return_value=inner)
    return pool, inner


# ---------------------------------------------------------------------------
# Exactly 50% — should NOT trip
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_tombstone_exactly_50_percent_does_not_trip_circuit_breaker():
    """2 missing out of 4 existing = 50% → circuit breaker must NOT trip."""
    existing = ["id-1", "id-2", "id-3", "id-4"]
    seen = {"id-1", "id-2"}  # id-3 and id-4 missing → ratio = 0.5

    pool, conn = _make_pool_with_rows(existing)
    engine = IngestionEngine(pool)
    result = _make_result()
    log = MagicMock()

    await engine._tombstone_missing(
        "inout_src_hubspot_contacts", seen, result, log
    )

    update_calls = [
        c for c in conn.execute.call_args_list
        if "UPDATE" in str(c) and "_deleted_at" in str(c)
    ]
    assert len(update_calls) == 2
    assert result.records_deleted == 2
    log.warning.assert_not_called()


# ---------------------------------------------------------------------------
# Just above 50% — should trip
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_tombstone_above_50_percent_trips_circuit_breaker():
    """3 missing out of 4 existing = 75% → circuit breaker must trip."""
    existing = ["id-1", "id-2", "id-3", "id-4"]
    seen = {"id-1"}  # id-2, id-3, id-4 missing → ratio = 0.75

    pool, conn = _make_pool_with_rows(existing)
    engine = IngestionEngine(pool)
    result = _make_result()
    log = MagicMock()

    await engine._tombstone_missing(
        "inout_src_hubspot_contacts", seen, result, log
    )

    update_calls = [
        c for c in conn.execute.call_args_list
        if "UPDATE" in str(c) and "_deleted_at" in str(c)
    ]
    assert not update_calls
    assert result.records_deleted == 0
    log.warning.assert_called_once()


# ---------------------------------------------------------------------------
# Below 50% — should NOT trip
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_tombstone_below_50_percent_does_not_trip():
    """1 missing out of 4 existing = 25% → tombstone proceeds normally."""
    existing = ["id-1", "id-2", "id-3", "id-4"]
    seen = {"id-1", "id-2", "id-3"}  # id-4 missing → ratio = 0.25

    pool, conn = _make_pool_with_rows(existing)
    engine = IngestionEngine(pool)
    result = _make_result()
    log = MagicMock()

    await engine._tombstone_missing(
        "inout_src_hubspot_contacts", seen, result, log
    )

    update_calls = [
        c for c in conn.execute.call_args_list
        if "UPDATE" in str(c) and "_deleted_at" in str(c)
    ]
    assert len(update_calls) == 1
    assert result.records_deleted == 1
