"""Unit tests for run_sync mode-detection logic.

Covers:
- mode='full' when no watermark exists.
- mode='incremental' when watermark exists AND incremental config is enabled.
- mode='full' when watermark exists but incremental config is None.
- mode='full' when watermark exists but incremental.enabled = False.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ingestion_cfg(
    *,
    incremental_enabled: bool | None = None,
) -> MagicMock:
    """Build an ingestion config.

    incremental_enabled=None  → incremental field is None (no incremental config)
    incremental_enabled=True  → incremental.enabled = True
    incremental_enabled=False → incremental.enabled = False
    """
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.list = MagicMock()
    if incremental_enabled is None:
        cfg.list.incremental = None
    else:
        inc = MagicMock()
        inc.enabled = incremental_enabled
        cfg.list.incremental = inc
    cfg.history_mode = "none"
    return cfg


def _make_connector(name: str = "testconn") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    return cfg


def _make_conn(
    for_update_row: tuple | None = ("row-id",),
) -> AsyncMock:
    async def _execute(sql: str, params=None):
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
    return conn


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


async def _run(watermark: str | None, ingestion_cfg: MagicMock) -> str:
    """Run engine.run_sync with a controlled watermark and return the result mode."""
    from inandout.ingestion.engine import IngestionEngine

    engine = IngestionEngine(_build_pool(_make_conn()))
    engine._read_pool = _build_pool(_make_conn())

    # Patch get_watermark to return the desired value
    with patch("inandout.ingestion.engine.get_watermark", return_value=watermark), \
         patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(_make_connector(), "contacts", ingestion_cfg)

    return result.mode


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_mode_full_when_no_watermark():
    mode = await _run(watermark=None, ingestion_cfg=_make_ingestion_cfg(incremental_enabled=True))
    assert mode == "full"


@pytest.mark.anyio
async def test_mode_incremental_when_watermark_exists_and_incremental_enabled():
    mode = await _run(
        watermark="2026-01-01T00:00:00Z",
        ingestion_cfg=_make_ingestion_cfg(incremental_enabled=True),
    )
    assert mode == "incremental"


@pytest.mark.anyio
async def test_mode_full_when_watermark_exists_but_incremental_config_is_none():
    mode = await _run(
        watermark="2026-01-01T00:00:00Z",
        ingestion_cfg=_make_ingestion_cfg(incremental_enabled=None),
    )
    assert mode == "full"


@pytest.mark.anyio
async def test_mode_full_when_watermark_exists_but_incremental_disabled():
    mode = await _run(
        watermark="2026-01-01T00:00:00Z",
        ingestion_cfg=_make_ingestion_cfg(incremental_enabled=False),
    )
    assert mode == "full"
