"""Unit tests for bulk_upsert_records (Step 64)."""
from __future__ import annotations

import hashlib
import uuid
from unittest.mock import AsyncMock, MagicMock, call, patch

import orjson
import pytest


def _make_hash(record: dict) -> str:
    return hashlib.sha256(
        orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


# ---------------------------------------------------------------------------
# _compute_raw_hash
# ---------------------------------------------------------------------------

def test_compute_raw_hash_is_deterministic():
    from inandout.postgres.bulk_upsert import _compute_raw_hash

    record = {"id": "1", "name": "Alice", "score": 42}
    h1 = _compute_raw_hash(record)
    h2 = _compute_raw_hash(record)
    assert h1 == h2


def test_compute_raw_hash_sorted_keys():
    """Hash must be key-order independent."""
    from inandout.postgres.bulk_upsert import _compute_raw_hash

    r1 = {"a": 1, "b": 2}
    r2 = {"b": 2, "a": 1}
    assert _compute_raw_hash(r1) == _compute_raw_hash(r2)


def test_compute_raw_hash_different_records():
    from inandout.postgres.bulk_upsert import _compute_raw_hash

    r1 = {"id": "1", "name": "Alice"}
    r2 = {"id": "2", "name": "Bob"}
    assert _compute_raw_hash(r1) != _compute_raw_hash(r2)


# ---------------------------------------------------------------------------
# bulk_upsert_records — insert path
# ---------------------------------------------------------------------------

async def test_bulk_upsert_inserts_new_records():
    """New records (not in DB) should result in INSERT calls."""
    from inandout.postgres.bulk_upsert import bulk_upsert_records

    run_id = uuid.uuid4()
    records = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]

    # Mock connection: fetchall returns empty (no existing hashes)
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    inserted, updated = await bulk_upsert_records(mock_conn, "test_table", records, "id", run_id)

    assert inserted == 2
    assert updated == 0
    assert mock_conn.execute.called


async def test_bulk_upsert_skips_noop_records():
    """Records whose hash matches existing should be skipped (no-op)."""
    from inandout.postgres.bulk_upsert import bulk_upsert_records, _compute_raw_hash

    run_id = uuid.uuid4()
    records = [{"id": "1", "name": "Alice"}]
    existing_hash = _compute_raw_hash(records[0])

    # Mock: existing hash matches
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[("1", existing_hash)])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    inserted, updated = await bulk_upsert_records(mock_conn, "test_table", records, "id", run_id)

    assert inserted == 0
    assert updated == 0


async def test_bulk_upsert_updates_changed_records():
    """Records whose hash differs from existing should result in UPDATE calls."""
    from inandout.postgres.bulk_upsert import bulk_upsert_records

    run_id = uuid.uuid4()
    records = [{"id": "1", "name": "Alice Changed"}]
    old_hash = "deadbeef" * 8  # does not match computed hash

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[("1", old_hash)])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    inserted, updated = await bulk_upsert_records(mock_conn, "test_table", records, "id", run_id)

    assert inserted == 0
    assert updated == 1


async def test_bulk_upsert_empty_list():
    """Empty record list returns (0, 0) without touching the DB."""
    from inandout.postgres.bulk_upsert import bulk_upsert_records

    mock_conn = AsyncMock()
    run_id = uuid.uuid4()

    inserted, updated = await bulk_upsert_records(mock_conn, "test_table", [], "id", run_id)

    assert inserted == 0
    assert updated == 0
    mock_conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Bulk batch size behaviour in IngestionConfig
# ---------------------------------------------------------------------------

def test_ingestion_config_default_bulk_batch_size_is_1():
    """Default bulk_upsert_batch_size must be 1 (single-record path)."""
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ScheduleConfig, ListConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig

    cfg = IngestionConfig(
        primary_key="id",
        history_mode=HistoryMode.overwrite,
        schedule=ScheduleConfig(interval="5m"),
        **{"list": ListConfig(
            path="/records",
            pagination=PaginationConfig(
                strategy=PaginationStrategy.cursor,
                cursor=CursorConfig(response_path="next", request_param="after"),
            ),
        )},
    )
    assert cfg.bulk_upsert_batch_size == 1


def test_ingestion_config_custom_bulk_batch_size():
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ScheduleConfig, ListConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig

    cfg = IngestionConfig(
        primary_key="id",
        history_mode=HistoryMode.overwrite,
        schedule=ScheduleConfig(interval="5m"),
        bulk_upsert_batch_size=100,
        **{"list": ListConfig(
            path="/records",
            pagination=PaginationConfig(
                strategy=PaginationStrategy.cursor,
                cursor=CursorConfig(response_path="next", request_param="after"),
            ),
        )},
    )
    assert cfg.bulk_upsert_batch_size == 100
