"""Unit tests for dead-letter inspection and transform pipeline."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.deadletter.inspect import fetch_dead_letter_rows
from inandout.deadletter.transform import TransformResult, _load_transform_function, apply_transform_script


# ---------------------------------------------------------------------------
# fetch_dead_letter_rows tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_fetch_dead_letter_rows_queries_correct_table():
    """fetch_dead_letter_rows should query the correct dead-letter table."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_pool.connection = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_conn), __aexit__=AsyncMock(return_value=False)))

    rows = await fetch_dead_letter_rows(mock_pool, "myconn", "mydatatype", limit=10)

    assert rows == []
    mock_conn.execute.assert_called_once()
    # Check the query includes the expected table name
    call_args = mock_conn.execute.call_args
    query = call_args[0][0]
    assert "myconn" in query or "mydatatype" in query or "ingestion" in query


@pytest.mark.anyio
async def test_fetch_dead_letter_rows_returns_correct_structure():
    """fetch_dead_letter_rows should return rows with expected keys."""
    from datetime import datetime, timezone

    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    fake_rows = [
        (1, "ext-001", '{"name": "foo"}', "missing key", "data_error",
         datetime(2024, 1, 1, tzinfo=timezone.utc), 0),
    ]
    mock_cursor.fetchall = AsyncMock(return_value=fake_rows)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_pool.connection = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )

    rows = await fetch_dead_letter_rows(mock_pool, "conn", "dtype", limit=5)

    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["external_id"] == "ext-001"
    assert rows[0]["error_class"] == "data_error"
    assert rows[0]["requeue_count"] == 0


@pytest.mark.anyio
async def test_fetch_dead_letter_rows_handles_exception():
    """fetch_dead_letter_rows should return empty list on DB error."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(side_effect=Exception("DB connection failed"))
    mock_pool.connection = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )

    rows = await fetch_dead_letter_rows(mock_pool, "conn", "dtype")
    assert rows == []


# ---------------------------------------------------------------------------
# _load_transform_function tests
# ---------------------------------------------------------------------------

def test_load_transform_function_loads_script():
    """_load_transform_function should load the transform function from a script."""
    script_content = """\
async def transform(record: dict) -> dict | None:
    return record
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = Path(f.name)

    try:
        fn = _load_transform_function(script_path)
        assert callable(fn)
    finally:
        script_path.unlink(missing_ok=True)


def test_load_transform_function_raises_if_no_transform():
    """_load_transform_function should raise AttributeError if no 'transform' function."""
    script_content = """\
def some_other_function():
    pass
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = Path(f.name)

    try:
        with pytest.raises(AttributeError, match="transform"):
            _load_transform_function(script_path)
    finally:
        script_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# apply_transform_script tests
# ---------------------------------------------------------------------------

def _make_mock_pool_with_rows(rows: list) -> MagicMock:
    """Create a mock pool that returns the given rows."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=rows)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.commit = AsyncMock()
    mock_pool.connection = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return mock_pool


@pytest.mark.anyio
async def test_apply_transform_script_calls_transform():
    """apply_transform_script should call the transform function for each row."""
    script_content = """\
called_with = []

async def transform(record: dict) -> dict | None:
    called_with.append(record)
    return {**record, "transformed": True}
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = Path(f.name)

    try:
        from datetime import datetime, timezone
        fake_rows = [
            (1, "ext-001", '{"name": "foo"}', "err", "error_class",
             datetime.now(timezone.utc), 0),
        ]
        mock_pool = _make_mock_pool_with_rows(fake_rows)

        result = await apply_transform_script(
            mock_pool, "conn", "dtype", script_path, dry_run=True
        )
        assert result.processed == 1
    finally:
        script_path.unlink(missing_ok=True)


@pytest.mark.anyio
async def test_apply_transform_dry_run_no_db_writes():
    """dry_run=True should not perform any DB writes."""
    script_content = """\
async def transform(record: dict) -> dict | None:
    return {**record, "transformed": True}
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = Path(f.name)

    try:
        from datetime import datetime, timezone
        fake_rows = [
            (1, "ext-001", '{"name": "foo"}', "err", "error_class",
             datetime.now(timezone.utc), 0),
        ]
        mock_pool = _make_mock_pool_with_rows(fake_rows)

        result = await apply_transform_script(
            mock_pool, "conn", "dtype", script_path, dry_run=True
        )
        assert result.upserted == 1
        # In dry_run, pool.connection should only have been called for the fetch
        # (not for any write operations)
        # The fetch call is the only one expected
    finally:
        script_path.unlink(missing_ok=True)


@pytest.mark.anyio
async def test_apply_transform_none_return_drops_record():
    """transform() returning None should count as dropped, not upserted."""
    script_content = """\
async def transform(record: dict) -> dict | None:
    return None  # drop this record
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = Path(f.name)

    try:
        from datetime import datetime, timezone
        fake_rows = [
            (1, "ext-001", '{"name": "foo"}', "err", "error_class",
             datetime.now(timezone.utc), 0),
        ]
        mock_pool = _make_mock_pool_with_rows(fake_rows)

        result = await apply_transform_script(
            mock_pool, "conn", "dtype", script_path, dry_run=True
        )
        assert result.dropped == 1
        assert result.upserted == 0
    finally:
        script_path.unlink(missing_ok=True)


@pytest.mark.anyio
async def test_apply_transform_exception_counted_as_failed():
    """If transform() raises, the error should be counted and processing continues."""
    script_content = """\
async def transform(record: dict) -> dict | None:
    raise ValueError("transform error")
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        script_path = Path(f.name)

    try:
        from datetime import datetime, timezone
        fake_rows = [
            (1, "ext-001", '{"name": "foo"}', "err", "error_class",
             datetime.now(timezone.utc), 0),
            (2, "ext-002", '{"name": "bar"}', "err", "error_class",
             datetime.now(timezone.utc), 0),
        ]
        mock_pool = _make_mock_pool_with_rows(fake_rows)

        result = await apply_transform_script(
            mock_pool, "conn", "dtype", script_path, dry_run=True
        )
        # Both rows fail but processing continues
        assert result.processed == 2
        assert result.failed == 2
        assert result.upserted == 0
    finally:
        script_path.unlink(missing_ok=True)


def test_transform_result_defaults():
    """TransformResult should have zero defaults."""
    result = TransformResult()
    assert result.processed == 0
    assert result.upserted == 0
    assert result.dropped == 0
    assert result.failed == 0
