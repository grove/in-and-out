"""Tests for extended GDPR purge (steps 7 and 8 added to purge_by_external_id)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.privacy import purge_by_external_id
from inandout.ingestion.privacy import PurgeResult


def _make_pool() -> MagicMock:
    pool = MagicMock()
    conn = AsyncMock()
    cur = MagicMock()
    cur.rowcount = 1
    conn.execute = AsyncMock(return_value=cur)

    txn_cm = AsyncMock()
    txn_cm.__aenter__ = AsyncMock(return_value=None)
    txn_cm.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn_cm)

    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=conn_cm)
    return pool


async def test_writeback_dead_letter_table_purged():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("inout_dl_writeback_crm_contacts" in s for s in all_sql)


async def test_desired_state_table_tombstoned():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("inout_dst_crm_contacts" in s for s in all_sql)


async def test_purge_result_includes_writeback_dead_letter_key():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert "writeback_dead_letter" in result.tables_purged


async def test_purge_result_includes_desired_state_key():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert "desired_state" in result.tables_purged


async def test_namespace_applied_to_writeback_dl_table():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001", namespace="tenant_y")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("tenant_y.inout_dl_writeback_crm_contacts" in s for s in all_sql)


async def test_namespace_applied_to_desired_state_table():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001", namespace="tenant_y")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("tenant_y.inout_dst_crm_contacts" in s for s in all_sql)
