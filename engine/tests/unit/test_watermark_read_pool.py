"""Unit test — watermark is read via the read pool, not the write pool (item 6).

Verifies that run_sync calls get_watermark on a connection from _read_conn_pool()
(which returns the read pool when set) rather than the primary write pool.
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


def _make_named_conn(label: str, used_labels: list[str]) -> AsyncMock:
    """Return a connection whose execute() records *label* so callers can see which pool was used."""
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=("row-id",))
        elif "watermark_value" in sql:
            used_labels.append(label)
            cur.fetchone = AsyncMock(return_value=None)
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    return conn


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# Test: watermark is read from the read pool
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_run_sync_reads_watermark_from_read_pool():
    """
    When a dedicated read_pool is supplied, get_watermark must be called on a
    connection from that pool, not from the primary write pool.
    """
    from inandout.ingestion.engine import IngestionEngine

    used_labels: list[str] = []

    write_conn = _make_named_conn("write", used_labels)
    read_conn  = _make_named_conn("read",  used_labels)

    write_pool = _build_pool(write_conn)
    read_pool  = _build_pool(read_conn)

    engine = IngestionEngine(write_pool, read_pool=read_pool)

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    watermark_reads = used_labels
    assert watermark_reads, "get_watermark was never called"
    assert all(label == "read" for label in watermark_reads), (
        f"Expected all watermark reads via 'read' pool, got: {watermark_reads}"
    )


@pytest.mark.anyio
async def test_run_sync_falls_back_to_write_pool_when_no_read_pool():
    """
    When no dedicated read_pool is passed, get_watermark falls back to the
    primary write pool (both 'write' and 'read' labels come from the same conn).
    """
    from inandout.ingestion.engine import IngestionEngine

    used_labels: list[str] = []
    write_conn = _make_named_conn("write", used_labels)
    write_pool = _build_pool(write_conn)

    engine = IngestionEngine(write_pool)  # no read_pool

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert used_labels, "get_watermark was never called"
    # All watermark reads must come through the write pool (label == 'write')
    assert all(label == "write" for label in used_labels), (
        f"Without a read pool, watermark should use write pool; got: {used_labels}"
    )
