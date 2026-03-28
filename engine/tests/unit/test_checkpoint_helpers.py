"""Unit tests for save_checkpoint, load_checkpoint, clear_checkpoint."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.checkpoint import (
    clear_checkpoint,
    load_checkpoint,
    save_checkpoint,
)


def _make_pool(fetchone_return=None) -> MagicMock:
    """Build a mocked async pool."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.commit = AsyncMock()

    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=fetchone_return)
    conn.execute = AsyncMock(return_value=cursor)

    conn_cm = AsyncMock()
    conn_cm.__aenter__ = AsyncMock(return_value=conn)
    conn_cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=conn_cm)
    return pool


# --- save_checkpoint ---

async def test_save_checkpoint_calls_execute():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await save_checkpoint(pool, run_id, "crm", "contacts", 3, "cursor-v1", 100)
    conn.execute.assert_awaited_once()


async def test_save_checkpoint_sql_contains_upsert():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await save_checkpoint(pool, run_id, "crm", "contacts", 3, "cursor-v1", 100)
    sql = conn.execute.await_args.args[0]
    assert "INSERT" in sql
    assert "ON CONFLICT" in sql


async def test_save_checkpoint_passes_run_id_as_string():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await save_checkpoint(pool, run_id, "crm", "contacts", 0, None, 0)
    params = conn.execute.await_args.args[1]
    assert str(run_id) in params


async def test_save_checkpoint_commits():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await save_checkpoint(pool, run_id, "crm", "contacts", 0, None, 0)
    conn.commit.assert_awaited_once()


# --- load_checkpoint ---

async def test_load_checkpoint_returns_none_when_no_row():
    pool = _make_pool(fetchone_return=None)
    result = await load_checkpoint(pool, uuid.uuid4())
    assert result is None


async def test_load_checkpoint_returns_dict():
    run_id = uuid.uuid4()
    row = (str(run_id), "crm", "contacts", 2, "cursor-v1", 50, "2026-01-01")
    pool = _make_pool(fetchone_return=row)
    result = await load_checkpoint(pool, run_id)
    assert isinstance(result, dict)


async def test_load_checkpoint_dict_has_run_id():
    run_id = uuid.uuid4()
    row = (str(run_id), "crm", "contacts", 2, "cursor-v1", 50, "2026-01-01")
    pool = _make_pool(fetchone_return=row)
    result = await load_checkpoint(pool, run_id)
    assert result["run_id"] == str(run_id)


async def test_load_checkpoint_dict_has_page_number():
    run_id = uuid.uuid4()
    row = (str(run_id), "crm", "contacts", 7, "cursor-v2", 100, "2026-01-01")
    pool = _make_pool(fetchone_return=row)
    result = await load_checkpoint(pool, run_id)
    assert result["page_number"] == 7


async def test_load_checkpoint_dict_has_cursor_value():
    run_id = uuid.uuid4()
    row = (str(run_id), "crm", "contacts", 1, "my-cursor", 10, "2026-01-01")
    pool = _make_pool(fetchone_return=row)
    result = await load_checkpoint(pool, run_id)
    assert result["cursor_value"] == "my-cursor"


# --- clear_checkpoint ---

async def test_clear_checkpoint_calls_delete():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await clear_checkpoint(pool, run_id)
    sql = conn.execute.await_args.args[0]
    assert "DELETE" in sql


async def test_clear_checkpoint_passes_run_id():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await clear_checkpoint(pool, run_id)
    params = conn.execute.await_args.args[1]
    assert str(run_id) in params


async def test_clear_checkpoint_commits():
    pool = _make_pool()
    run_id = uuid.uuid4()
    conn = pool.connection.return_value.__aenter__.return_value
    await clear_checkpoint(pool, run_id)
    conn.commit.assert_awaited_once()
