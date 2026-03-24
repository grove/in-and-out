"""Tests for desired-state housekeeping (new purge step added to run_housekeeping)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.housekeeping import run_housekeeping


def _make_pool() -> MagicMock:
    pool = MagicMock()
    conn = AsyncMock()
    cur = MagicMock()
    cur.rowcount = 3
    conn.execute = AsyncMock(return_value=cur)
    conn.commit = AsyncMock()

    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=conn_cm)
    return pool


def _make_housekeeping_cfg(desired_state_processed: str = "90d"):
    from inandout.config.tool import HousekeepingConfig, RetentionConfig

    retention = RetentionConfig(desired_state_processed=desired_state_processed)
    return HousekeepingConfig(retention=retention)


async def test_desired_state_table_purged():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    cfg = _make_housekeeping_cfg()

    await run_housekeeping(pool, cfg, [("crm", "contacts")])

    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("inout_dst_crm_contacts" in s for s in all_sql)


async def test_desired_state_purge_checks_processed_at():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    cfg = _make_housekeeping_cfg()

    await run_housekeeping(pool, cfg, [("crm", "contacts")])

    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    dst_sql = next((s for s in all_sql if "inout_dst_crm_contacts" in s), None)
    assert dst_sql is not None
    assert "_processed_at IS NOT NULL" in dst_sql


async def test_desired_state_totals_key_present():
    pool = _make_pool()
    cfg = _make_housekeeping_cfg()

    totals = await run_housekeeping(pool, cfg, [("crm", "contacts")])
    assert "dst_crm_contacts" in totals


async def test_desired_state_custom_retention_applied():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    cfg = _make_housekeeping_cfg(desired_state_processed="30d")

    await run_housekeeping(pool, cfg, [("crm", "contacts")])

    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    dst_sql = next((s for s in all_sql if "inout_dst_crm_contacts" in s), None)
    assert dst_sql is not None
    assert "30 days" in dst_sql


async def test_desired_state_purge_table_missing_does_not_raise():
    """Exception from missing table must be swallowed."""
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value

    call_count = [0]
    original = conn.execute.side_effect

    async def _raise_for_dst(*args, **kwargs):
        call_count[0] += 1
        if args and "inout_dst_crm_contacts" in str(args[0]):
            raise Exception("relation does not exist")
        cur = MagicMock()
        cur.rowcount = 0
        return cur

    conn.execute.side_effect = _raise_for_dst
    cfg = _make_housekeeping_cfg()

    # Must not raise
    totals = await run_housekeeping(pool, cfg, [("crm", "contacts")])
    # dst key should be absent since it raised
    assert "dst_crm_contacts" not in totals
