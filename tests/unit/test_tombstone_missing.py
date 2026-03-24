"""Unit tests for _tombstone_missing soft-delete behaviour.

Covers:
- IDs absent from seen_ids get an UPDATE _deleted_at = NOW().
- IDs present in seen_ids are NOT updated.
- result.records_deleted equals the count of tombstoned records.
- When all existing IDs are in seen_ids (nothing missing), no UPDATE issued.
- Circuit-breaker: when >50% of records would be deleted, no UPDATE issued.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.ingestion.engine import IngestionEngine, SyncResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result() -> SyncResult:
    return SyncResult(uuid.uuid4(), "hubspot", "contacts", "full")


def _make_pool_with_rows(
    rows: list[str],
    count: int | None = None,
) -> MagicMock:
    """
    Build a pool whose connection returns `rows` for SELECT external_id
    and `count` (or len(rows)) for SELECT COUNT(*).
    """
    effective_count = count if count is not None else len(rows)

    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "COUNT(*)" in sql:
            cur.fetchone = AsyncMock(return_value=(effective_count,))
        elif "SELECT external_id" in sql:
            cur.fetchall = AsyncMock(return_value=[(r,) for r in rows])
        else:
            cur.fetchone = AsyncMock(return_value=None)
            cur.fetchall = AsyncMock(return_value=[])
            cur.rowcount = 1
        return cur

    # transaction() must return an async context manager
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
# Core tombstone tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_tombstone_missing_updates_absent_ids():
    """IDs not in seen_ids must receive UPDATE _deleted_at = NOW()."""
    # 5 existing, 3 seen → 2 missing → ratio = 2/5 = 0.4 < 0.5 (no circuit breaker)
    existing = ["id-1", "id-2", "id-3", "id-4", "id-5"]
    seen = {"id-1", "id-2", "id-3"}  # id-4 and id-5 are missing

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


@pytest.mark.anyio
async def test_tombstone_missing_result_records_deleted():
    """result.records_deleted must equal the number of tombstoned rows."""
    # 5 existing, 3 seen → 2 missing → ratio = 0.4 < 0.5
    existing = ["id-1", "id-2", "id-3", "id-4", "id-5"]
    seen = {"id-1", "id-2", "id-3"}

    pool, _ = _make_pool_with_rows(existing)
    engine = IngestionEngine(pool)
    result = _make_result()
    log = MagicMock()

    await engine._tombstone_missing(
        "inout_src_hubspot_contacts", seen, result, log
    )

    assert result.records_deleted == 2


@pytest.mark.anyio
async def test_tombstone_missing_no_update_when_all_present():
    """When seen_ids contains every existing row, no UPDATEs must be issued."""
    existing = ["id-1", "id-2"]
    seen = {"id-1", "id-2"}

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


@pytest.mark.anyio
async def test_tombstone_missing_circuit_breaker_on_mass_delete():
    """When >50% would be deleted, no UPDATE must be issued (circuit breaker)."""
    existing = ["id-1", "id-2", "id-3", "id-4"]
    seen = {"id-1"}  # 75% missing → circuit breaker trips

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


@pytest.mark.anyio
async def test_tombstone_missing_no_action_when_table_empty():
    """When COUNT(*) returns 0, no further queries must be issued."""
    pool, conn = _make_pool_with_rows([], count=0)
    engine = IngestionEngine(pool)
    result = _make_result()
    log = MagicMock()

    await engine._tombstone_missing(
        "inout_src_hubspot_contacts", set(), result, log
    )

    # Only the COUNT query should have been issued
    called_sqls = [str(c) for c in conn.execute.call_args_list]
    assert not any("UPDATE" in s for s in called_sqls)
    assert not any("SELECT external_id" in s for s in called_sqls)
