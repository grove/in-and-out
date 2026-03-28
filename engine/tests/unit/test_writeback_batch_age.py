"""Unit tests for T2 #33 — batch_max_age_secs enforcement in _fetch_delta_rows."""
from __future__ import annotations

import datetime
import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_engine():
    """Return a WritebackEngine instance with a mocked pool, bypassing __init__."""
    from inandout.writeback.engine import WritebackEngine

    engine = object.__new__(WritebackEngine)
    engine._pool = MagicMock()
    engine._log = MagicMock()
    return engine


def _make_result():
    from inandout.writeback.engine import WritebackResult

    return WritebackResult(connector="test", datatype="contacts", delta_table="delta_contacts")


def _make_rows(ts: datetime.datetime | None, count: int = 2) -> list[dict]:
    """Build minimal delta-table row dicts."""
    rows = []
    for i in range(count):
        row: dict = {"_action": "update", "external_id": f"id-{i}", "name": f"name-{i}"}
        if ts is not None:
            row["_queued_at"] = ts
        rows.append(row)
    return rows


def _patch_pool_with_rows(engine, rows: list[dict]):
    """Wire the engine's pool so _fetch_delta_rows returns *rows*."""
    mock_cursor = AsyncMock()
    mock_cursor.description = [(col,) for col in (rows[0].keys() if rows else [])]
    mock_cursor.fetchall = AsyncMock(
        return_value=[tuple(r.values()) for r in rows] if rows else []
    )
    mock_fetch_conn = AsyncMock()
    mock_fetch_conn.execute = AsyncMock(return_value=mock_cursor)

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_fetch_conn)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    engine._pool.connection = MagicMock(return_value=mock_ctx)


# ---------------------------------------------------------------------------
# Signature / source checks
# ---------------------------------------------------------------------------

def test_fetch_delta_rows_accepts_batch_max_age_secs():
    """_fetch_delta_rows must accept batch_max_age_secs as a keyword argument."""
    from inandout.writeback.engine import WritebackEngine

    sig = inspect.signature(WritebackEngine._fetch_delta_rows)
    assert "batch_max_age_secs" in sig.parameters


def test_run_writeback_cycle_passes_batch_max_age_secs():
    """_run_writeback_cycle_inner must forward batch_max_age_secs from config."""
    from inandout.writeback import engine as writeback_engine_module

    src = inspect.getsource(writeback_engine_module)
    assert "batch_max_age_secs" in src
    # The call-site must pass it, not just define it
    assert "batch_max_age_secs=getattr" in src or "batch_max_age_secs=" in src


# ---------------------------------------------------------------------------
# Stale-row warning emitted when age exceeds threshold
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_stale_rows_warning_emitted_when_age_exceeded():
    """When oldest row is older than batch_max_age_secs, log warning is emitted."""
    engine = _make_engine()
    result = _make_result()

    old_ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
    rows = _make_rows(old_ts)
    _patch_pool_with_rows(engine, rows)

    captured_events: list[str] = []

    original_warning = engine._log.warning if hasattr(engine._log, "warning") else None

    # Patch structlog so we can capture log events
    with patch("inandout.writeback.engine.logger") as mock_log:
        mock_log.warning = MagicMock()
        mock_log.info = MagicMock()

        fetched = await engine._fetch_delta_rows(
            "delta_contacts",
            mock_log,
            result,
            batch_max_age_secs=60.0,
        )

    assert fetched is not None
    assert len(fetched) == len(rows)

    # The stale warning must have been emitted
    warning_calls = mock_log.warning.call_args_list
    event_names = [c[0][0] for c in warning_calls]
    assert "writeback_batch_stale_rows" in event_names


@pytest.mark.anyio
async def test_stale_rows_warning_not_emitted_when_recent():
    """No stale warning when row is recent (within budget)."""
    engine = _make_engine()
    result = _make_result()

    recent_ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5)
    rows = _make_rows(recent_ts)
    _patch_pool_with_rows(engine, rows)

    with patch("inandout.writeback.engine.logger") as mock_log:
        mock_log.warning = MagicMock()
        mock_log.info = MagicMock()

        await engine._fetch_delta_rows(
            "delta_contacts",
            mock_log,
            result,
            batch_max_age_secs=60.0,
        )

    warning_calls = mock_log.warning.call_args_list
    event_names = [c[0][0] for c in warning_calls]
    assert "writeback_batch_stale_rows" not in event_names


@pytest.mark.anyio
async def test_no_stale_check_when_batch_max_age_is_none():
    """When batch_max_age_secs is None, stale checking is skipped entirely."""
    engine = _make_engine()
    result = _make_result()

    old_ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=9999)
    rows = _make_rows(old_ts)
    _patch_pool_with_rows(engine, rows)

    with patch("inandout.writeback.engine.logger") as mock_log:
        mock_log.warning = MagicMock()
        mock_log.info = MagicMock()

        await engine._fetch_delta_rows(
            "delta_contacts",
            mock_log,
            result,
            batch_max_age_secs=None,
        )

    warning_calls = mock_log.warning.call_args_list
    event_names = [c[0][0] for c in warning_calls]
    assert "writeback_batch_stale_rows" not in event_names


@pytest.mark.anyio
async def test_no_stale_check_for_empty_batch():
    """An empty batch with batch_max_age_secs should not crash or warn."""
    engine = _make_engine()
    result = _make_result()

    # Wire pool to return no rows
    mock_cursor = AsyncMock()
    mock_cursor.description = [("_action",)]
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_fetch_conn = AsyncMock()
    mock_fetch_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_fetch_conn)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    engine._pool.connection = MagicMock(return_value=mock_ctx)

    with patch("inandout.writeback.engine.logger") as mock_log:
        mock_log.warning = MagicMock()
        mock_log.info = MagicMock()

        fetched = await engine._fetch_delta_rows(
            "delta_contacts",
            mock_log,
            result,
            batch_max_age_secs=30.0,
        )

    assert fetched == []
    warning_calls = mock_log.warning.call_args_list
    event_names = [c[0][0] for c in warning_calls]
    assert "writeback_batch_stale_rows" not in event_names


@pytest.mark.anyio
async def test_stale_check_uses_isoformat_string_timestamp():
    """Stale detection should also accept ISO-format string timestamps."""
    engine = _make_engine()
    result = _make_result()

    old_ts_str = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=300)
    ).isoformat()

    rows = [
        {"_action": "update", "external_id": "x", "_queued_at": old_ts_str}
    ]
    _patch_pool_with_rows(engine, rows)

    with patch("inandout.writeback.engine.logger") as mock_log:
        mock_log.warning = MagicMock()
        mock_log.info = MagicMock()

        await engine._fetch_delta_rows(
            "delta_contacts",
            mock_log,
            result,
            batch_max_age_secs=60.0,
        )

    warning_calls = mock_log.warning.call_args_list
    event_names = [c[0][0] for c in warning_calls]
    assert "writeback_batch_stale_rows" in event_names
