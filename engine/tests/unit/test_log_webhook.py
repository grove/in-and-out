"""Unit tests for _log_webhook in ingestion/webhooks.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.ingestion.webhooks import _log_webhook


def _make_pool() -> MagicMock:
    pool = MagicMock()
    conn = AsyncMock()
    conn.commit = AsyncMock()
    conn.execute = AsyncMock()

    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=conn_cm)
    return pool


async def test_calls_execute():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", "contacts", "ext-001", "abc123", "create", "ok")
    conn.execute.assert_awaited_once()


async def test_sql_contains_webhook_log_table():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", "contacts", "ext-001", "abc123", "create", "ok")
    sql = conn.execute.await_args.args[0]
    assert "inout_ops_webhook_log" in sql


async def test_sql_inserts_correct_columns():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", "contacts", "ext-001", "abc123", "create", "ok")
    sql = conn.execute.await_args.args[0]
    assert "connector" in sql
    assert "datatype" in sql
    assert "status" in sql


async def test_commits_after_execute():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", "contacts", "ext-001", "abc123", "create", "ok")
    conn.commit.assert_awaited_once()


async def test_passes_all_six_params():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", "contacts", "ext-001", "hash-xyz", "update", "error")
    params = conn.execute.await_args.args[1]
    assert "crm" in params
    assert "contacts" in params
    assert "ext-001" in params
    assert "hash-xyz" in params
    assert "update" in params
    assert "error" in params


async def test_swallows_pool_exception_silently():
    """_log_webhook must never raise even when the pool throws."""
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("db down")
    # Should not raise
    await _log_webhook(pool, "crm", "contacts", None, "h", "create", "ok")


async def test_none_external_id_accepted():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", "contacts", None, "hash", "create", "ok")
    params = conn.execute.await_args.args[1]
    assert None in params


async def test_none_datatype_accepted():
    pool = _make_pool()
    conn = pool.connection.return_value.__aenter__.return_value
    await _log_webhook(pool, "crm", None, None, "hash", "create", "ok")
    params = conn.execute.await_args.args[1]
    assert None in params
