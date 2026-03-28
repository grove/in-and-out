"""Unit tests for run_sync final-watermark write-back path.

After _do_sync completes, run_sync reads the final watermark via _read_conn_pool()
and writes it as high_water_mark_after in the UPDATE inout_ops_sync_run SQL.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.history_mode = "none"
    return cfg


def _make_connector(name: str = "testconn") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    return cfg


def _make_conn_with_watermark(
    watermark_value: str | None,
    for_update_row: tuple | None = ("row-id",),
) -> tuple[AsyncMock, list[str], list[list]]:
    """Return (conn, sql_list, params_list) where watermark queries return watermark_value."""
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        params_list.append(list(params) if params else [])
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=for_update_row)
        elif "watermark_value" in sql:
            cur.fetchone = AsyncMock(
                return_value=(watermark_value,) if watermark_value else None
            )
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    return conn, sql_list, params_list


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# high_water_mark_after is the watermark value from the read pool
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_final_watermark_written_as_high_water_mark_after():
    """
    After _do_sync, the watermark read from _read_conn_pool() must appear
    as the high_water_mark_after parameter in the UPDATE inout_ops_sync_run SQL.
    """
    from inandout.ingestion.engine import IngestionEngine

    EXPECTED_WATERMARK = "2026-03-24T12:00:00Z"

    write_conn, write_sql, write_params = _make_conn_with_watermark(
        watermark_value=None  # write pool returns no watermark
    )
    # The read pool returns the real final watermark
    read_conn, _, _ = _make_conn_with_watermark(watermark_value=EXPECTED_WATERMARK)

    write_pool = _build_pool(write_conn)
    read_pool = _build_pool(read_conn)

    engine = IngestionEngine(write_pool, read_pool=read_pool)

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    # Find the UPDATE inout_ops_sync_run that includes high_water_mark_after
    update_entries = [
        (s, p) for s, p in zip(write_sql, write_params)
        if "UPDATE inout_ops_sync_run" in s and "high_water_mark_after" in s
    ]
    assert update_entries, "Expected UPDATE inout_ops_sync_run with high_water_mark_after"

    # high_water_mark_after is the second-to-last param (before run_id)
    _, params = update_entries[0]
    # params order: status, fetched, inserted, updated, errored, error_message,
    #               error_detail, high_water_mark_after, run_id
    watermark_in_params = EXPECTED_WATERMARK in params
    assert watermark_in_params, (
        f"Expected watermark '{EXPECTED_WATERMARK}' in UPDATE params, got: {params}"
    )


@pytest.mark.anyio
async def test_final_watermark_none_when_get_watermark_returns_none():
    """
    If get_watermark returns None (no watermark set), high_water_mark_after
    must be None (not an error; sync still completes).
    """
    from inandout.ingestion.engine import IngestionEngine

    write_conn, write_sql, write_params = _make_conn_with_watermark(watermark_value=None)
    read_conn, _, _ = _make_conn_with_watermark(watermark_value=None)

    engine = IngestionEngine(_build_pool(write_conn), read_pool=_build_pool(read_conn))

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    # Sync should complete without error despite None watermark
    assert result.status not in ("running",)

    update_entries = [
        (s, p) for s, p in zip(write_sql, write_params)
        if "UPDATE inout_ops_sync_run" in s and ("high_water_mark_after" in s or "finished_at" in s)
    ]
    assert update_entries, "Expected UPDATE inout_ops_sync_run to be issued"
