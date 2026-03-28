"""Unit tests for fetch_dead_letter_rows in deadletter/inspect.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.deadletter.inspect import fetch_dead_letter_rows


def _make_pool(rows: list[tuple]) -> MagicMock:
    """Build a pool mock returning *rows* from conn.execute().fetchall()."""
    pool = MagicMock()
    conn = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=rows)
    conn.execute = AsyncMock(return_value=cursor)

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=cm)
    return pool


_SAMPLE_ROW = (1, "ext-001", '{"key": "val"}', "some error", "ValueError", "2026-01-01", 0)


async def test_returns_list():
    pool = _make_pool([_SAMPLE_ROW])
    result = await fetch_dead_letter_rows(pool, "crm", "contacts")
    assert isinstance(result, list)


async def test_single_row_converted_to_dict():
    pool = _make_pool([_SAMPLE_ROW])
    result = await fetch_dead_letter_rows(pool, "crm", "contacts")
    assert len(result) == 1
    row = result[0]
    assert row["id"] == 1
    assert row["external_id"] == "ext-001"
    assert row["error_message"] == "some error"
    assert row["error_class"] == "ValueError"
    assert row["requeue_count"] == 0


async def test_empty_result_returns_empty_list():
    pool = _make_pool([])
    result = await fetch_dead_letter_rows(pool, "crm", "contacts")
    assert result == []


async def test_multiple_rows():
    rows = [
        (1, "e1", "{}", "err1", "TypeError", "2026-01-01", 0),
        (2, "e2", "{}", "err2", "ValueError", "2026-01-02", 1),
    ]
    pool = _make_pool(rows)
    result = await fetch_dead_letter_rows(pool, "crm", "contacts")
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2


async def test_limit_passed_to_query():
    pool = _make_pool([])
    conn = pool.connection.return_value.__aenter__.return_value
    await fetch_dead_letter_rows(pool, "crm", "contacts", limit=5)
    # The limit parameter should appear in the execute call args
    call_args = conn.execute.await_args
    params = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("params")
    assert params == [5]


async def test_default_limit_is_20():
    pool = _make_pool([])
    conn = pool.connection.return_value.__aenter__.return_value
    await fetch_dead_letter_rows(pool, "crm", "contacts")
    call_args = conn.execute.await_args
    params = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("params")
    assert params == [20]


async def test_db_exception_returns_empty_list():
    """If pool.connection() raises, fetch_dead_letter_rows should return []."""
    pool = MagicMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(side_effect=Exception("connection refused"))
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=cm)
    result = await fetch_dead_letter_rows(pool, "crm", "contacts")
    assert result == []


async def test_result_dict_has_required_keys():
    pool = _make_pool([_SAMPLE_ROW])
    result = await fetch_dead_letter_rows(pool, "crm", "contacts")
    expected_keys = {"id", "external_id", "raw", "error_message", "error_class", "failed_at", "requeue_count"}
    assert set(result[0].keys()) == expected_keys
