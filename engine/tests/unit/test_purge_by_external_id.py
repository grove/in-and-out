"""Unit tests for purge_by_external_id in privacy.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from inandout.privacy import purge_by_external_id
from inandout.ingestion.privacy import PurgeResult


def _make_cursor(rowcount: int = 1) -> AsyncMock:
    cur = MagicMock()
    cur.rowcount = rowcount
    return cur


def _make_pool() -> MagicMock:
    """Build a pool mock where conn.execute returns a cursor with rowcount=1."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=_make_cursor(1))

    # transaction context manager
    txn_cm = AsyncMock()
    txn_cm.__aenter__ = AsyncMock(return_value=None)
    txn_cm.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn_cm)

    # pool.connection context manager
    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=conn_cm)
    return pool


async def test_returns_purge_result():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert isinstance(result, PurgeResult)


async def test_result_has_connector():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert result.connector == "crm"


async def test_result_has_datatype():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert result.datatype == "contacts"


async def test_result_has_external_id():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert result.external_id == "ext-001"


async def test_tables_purged_is_dict():
    pool = _make_pool()
    result = await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    assert isinstance(result.tables_purged, dict)


async def test_source_table_name_in_execute_calls():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("inout_src_crm_contacts" in s for s in all_sql)


async def test_history_table_name_in_execute_calls():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("inout_src_crm_contacts_history" in s for s in all_sql)


async def test_dead_letter_table_name_in_execute_calls():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("inout_dl_ingestion_crm_contacts" in s for s in all_sql)


async def test_custom_namespace_prefixes_tables():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "ext-001", namespace="tenant_x")
    all_sql = [c.args[0] for c in conn.execute.await_args_list if c.args]
    assert any("tenant_x.inout_src_crm_contacts" in s for s in all_sql)


async def test_external_id_passed_as_param():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await purge_by_external_id(pool, "crm", "contacts", "unique-id-xyz")
    all_params = [
        c.args[1] for c in conn.execute.await_args_list if len(c.args) > 1
    ]
    assert any("unique-id-xyz" in p for p in all_params)
