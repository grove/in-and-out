"""Unit tests for run_sync error_detail JSON structure.

When _do_sync raises, run_sync captures the exception and builds an
orjson error_detail blob.  Verifies the UPDATE inout_ops_sync_run
receives a param that decodes to {"message": ..., "status": "failed"}.
"""
from __future__ import annotations

import json
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


def _make_conn(for_update_row: tuple | None = ("row-id",)) -> tuple[AsyncMock, list[str], list[list]]:
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        params_list.append(list(params) if params else [])
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=for_update_row)
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
# error_detail is structured JSON with message + status
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_error_detail_json_structure_on_do_sync_exception():
    """
    When _do_sync raises, the UPDATE inout_ops_sync_run must include an
    error_detail param that decodes to {"message": <str>, "status": "failed"}.
    """
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, params_list = _make_conn()
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_conn()[0])

    ERROR_MSG = "upstream API returned 503"

    async def _failing_sync(*args, **kwargs):
        raise RuntimeError(ERROR_MSG)

    with patch.object(engine, "_do_sync", side_effect=_failing_sync):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status == "failed"
    assert result.error_message == ERROR_MSG

    # Find UPDATE inout_ops_sync_run with error_detail column
    update_entries = [
        (s, p) for s, p in zip(sql_list, params_list)
        if "UPDATE inout_ops_sync_run" in s and "error_detail" in s
    ]
    assert update_entries, "Expected UPDATE inout_ops_sync_run with error_detail"

    _, params = update_entries[0]
    # error_detail is at index 6 (0-based):
    # status(0), fetched(1), inserted(2), updated(3), errored(4), error_message(5),
    # error_detail(6), high_water_mark_after(7), run_id(8)
    error_detail_json = next(
        (p for p in params if isinstance(p, str) and p.startswith("{") and "message" in p),
        None,
    )
    assert error_detail_json is not None, (
        f"Could not find error_detail JSON blob in params: {params}"
    )
    decoded = json.loads(error_detail_json)
    assert decoded.get("message") == ERROR_MSG
    assert decoded.get("status") == "failed"


@pytest.mark.anyio
async def test_error_detail_none_on_successful_sync():
    """On a successful sync, error_detail should be None (not a JSON blob)."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, params_list = _make_conn()
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_conn()[0])

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status not in ("running", "failed")

    update_entries = [
        (s, p) for s, p in zip(sql_list, params_list)
        if "UPDATE inout_ops_sync_run" in s and "error_detail" in s
    ]
    if update_entries:
        _, params = update_entries[0]
        # error_detail param should be None when no error occurred
        error_detail_param = next(
            (p for p in params if isinstance(p, str) and p.startswith("{") and "message" in p),
            None,
        )
        assert error_detail_param is None, (
            "error_detail should be None on successful sync"
        )
