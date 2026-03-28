"""Unit tests for apply_transform_script (dry_run and drop paths)."""
from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.deadletter.transform import TransformResult, apply_transform_script


def _make_pool(rows: list[dict]) -> MagicMock:
    """Build a pool mock that returns *rows* from fetch_dead_letter_rows."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.commit = AsyncMock()

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=cm)
    return pool


def _write_script(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "t.py"
    p.write_text(textwrap.dedent(content))
    return p


@pytest.fixture
def passthrough_script(tmp_path):
    return _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            return record
        """,
    )


@pytest.fixture
def drop_script(tmp_path):
    return _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            return None
        """,
    )


SAMPLE_ROWS = [{"id": 1, "raw": {"key": "value"}}]
SAMPLE_ROWS_TWO = [
    {"id": 1, "raw": {"a": "1"}},
    {"id": 2, "raw": {"b": "2"}},
]


async def test_empty_rows_returns_zero_processed(passthrough_script):
    pool = _make_pool([])
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=[]),
    ):
        result = await apply_transform_script(pool, "crm", "contacts", passthrough_script)
    assert result.processed == 0
    assert result.upserted == 0
    assert result.dropped == 0
    assert result.failed == 0


async def test_dry_run_processes_but_no_db_write(passthrough_script):
    pool = _make_pool(SAMPLE_ROWS)
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=SAMPLE_ROWS),
    ):
        result = await apply_transform_script(
            pool, "crm", "contacts", passthrough_script, dry_run=True
        )
    assert result.processed == 1
    # In dry_run mode, no pool.connection() should be called for writing
    pool.connection.assert_not_called()


async def test_drop_script_increments_dropped(drop_script):
    pool = _make_pool(SAMPLE_ROWS)
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=SAMPLE_ROWS),
    ):
        result = await apply_transform_script(
            pool, "crm", "contacts", drop_script, dry_run=True
        )
    assert result.dropped == 1
    assert result.processed == 1


async def test_drop_script_dry_run_no_write(drop_script):
    pool = _make_pool(SAMPLE_ROWS)
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=SAMPLE_ROWS),
    ):
        await apply_transform_script(pool, "crm", "contacts", drop_script, dry_run=True)
    pool.connection.assert_not_called()


async def test_returns_transform_result_type(passthrough_script):
    pool = _make_pool([])
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=[]),
    ):
        result = await apply_transform_script(pool, "x", "y", passthrough_script)
    assert isinstance(result, TransformResult)


async def test_failed_transform_increments_failed(tmp_path):
    script = _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            raise ValueError("bad record")
        """,
    )
    pool = _make_pool(SAMPLE_ROWS)
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=SAMPLE_ROWS),
    ):
        result = await apply_transform_script(pool, "crm", "contacts", script, dry_run=True)
    assert result.failed == 1
    assert result.processed == 1


async def test_two_rows_dry_run(passthrough_script):
    pool = _make_pool(SAMPLE_ROWS_TWO)
    with patch(
        "inandout.deadletter.inspect.fetch_dead_letter_rows",
        new=AsyncMock(return_value=SAMPLE_ROWS_TWO),
    ):
        result = await apply_transform_script(
            pool, "crm", "contacts", passthrough_script, dry_run=True
        )
    assert result.processed == 2
