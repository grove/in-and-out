"""Unit tests for connector_version upsert on full-sync completion (engine.py).

Covers:
- After status='completed' + mode='full', INSERT INTO inout_ops_connector_version
  is issued with (connector.name, connector.version).
- Not issued for incremental syncs.
- Not issued when _do_sync raises (failed sync).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ingestion_cfg(*, incremental: bool = False) -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    inc = MagicMock()
    inc.enabled = True
    cfg.list = MagicMock()
    cfg.list.incremental = inc if incremental else None
    cfg.history_mode = "none"
    return cfg


def _make_connector(name: str = "hubspot", version: str = "2.0.0") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = version
    cfg.datatypes = {}
    return cfg


def _make_conn(
    for_update_row: tuple | None = ("row-id",),
    watermark_value: str | None = None,
) -> tuple[AsyncMock, list[str], list[list]]:
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
# Upserted on completed full sync
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_connector_version_upserted_on_completed_full_sync():
    """INSERT INTO inout_ops_connector_version must be issued for completed full syncs."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, params_list = _make_conn()
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_conn()[0])

    connector = _make_connector(name="hubspot", version="2.0.0")

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(connector, "contacts", _make_ingestion_cfg())

    version_inserts = [
        (s, p) for s, p in zip(sql_list, params_list)
        if "inout_ops_connector_version" in s and "INSERT" in s
    ]
    assert version_inserts, "Expected INSERT INTO inout_ops_connector_version"
    _, params = version_inserts[0]
    assert "hubspot" in params
    assert "2.0.0" in params


# ---------------------------------------------------------------------------
# NOT upserted on incremental sync
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_connector_version_not_upserted_on_incremental_sync():
    """Connector version INSERT must NOT be issued for incremental syncs."""
    from inandout.ingestion.engine import IngestionEngine

    # Supply a watermark so the mode resolves to 'incremental'
    conn, sql_list, params_list = _make_conn(watermark_value="2026-01-01")
    engine = IngestionEngine(_build_pool(conn))
    # Read pool also returns the watermark
    read_conn, _, _ = _make_conn(watermark_value="2026-01-01")
    engine._read_pool = _build_pool(read_conn)

    connector = _make_connector()

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(connector, "contacts", _make_ingestion_cfg(incremental=True))

    version_inserts = [
        s for s in sql_list
        if "inout_ops_connector_version" in s and "INSERT" in s
    ]
    assert not version_inserts, (
        "Expected no INSERT INTO inout_ops_connector_version for incremental syncs"
    )


# ---------------------------------------------------------------------------
# NOT upserted on failed sync
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_connector_version_not_upserted_on_failed_sync():
    """Connector version INSERT must NOT be issued when _do_sync raises."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, params_list = _make_conn()
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_conn()[0])

    connector = _make_connector()

    async def _failing_sync(*args, **kwargs):
        raise RuntimeError("upstream 500")

    with patch.object(engine, "_do_sync", side_effect=_failing_sync):
        result = await engine.run_sync(connector, "contacts", _make_ingestion_cfg())

    assert result.status == "failed"

    version_inserts = [
        s for s in sql_list
        if "inout_ops_connector_version" in s and "INSERT" in s
    ]
    assert not version_inserts, (
        "Expected no INSERT INTO inout_ops_connector_version for failed syncs"
    )
