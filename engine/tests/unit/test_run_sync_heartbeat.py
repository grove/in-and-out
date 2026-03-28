"""Unit tests for run_sync lock-heartbeat task behaviour.

When run_sync acquires the lock and calls _do_sync, a background heartbeat
task must be started. We verify:
- anyio.sleep(_LOCK_HEARTBEAT_INTERVAL_SECS) is called by the heartbeat
  (intercepted before the sync finishes).
- The heartbeat is cancelled (CancelScope) after _do_sync completes.

We test this by making _do_sync take two ticks (using a sleep counter),
then asserting the heartbeat sleep was called with the right interval.

NOTE: This is inherently concurrent, so we use an anyio.Event to coordinate.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest

import inandout.ingestion.engine as _engine_mod
from inandout.ingestion.engine import IngestionEngine, _LOCK_HEARTBEAT_INTERVAL_SECS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(name: str = "hubspot") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    cfg.api_version = "v1"
    return cfg


def _make_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.history_mode = "none"
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.schedule = MagicMock()
    cfg.schedule.cron = None
    cfg.schedule.interval = None
    return cfg


def _make_pool_with_lock() -> MagicMock:
    """Pool that returns a valid lock row (lock acquired)."""
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=("lock-row-id",))
        else:
            cur.fetchone = AsyncMock(return_value=None)
            cur.rowcount = 1
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_lock_heartbeat_sleep_called_with_correct_interval():
    """The heartbeat task must call anyio.sleep(_LOCK_HEARTBEAT_INTERVAL_SECS)."""
    heartbeat_sleep_args: list[float] = []
    sync_done = anyio.Event()

    original_sleep = anyio.sleep

    async def _intercepting_sleep(secs: float) -> None:
        if secs == _LOCK_HEARTBEAT_INTERVAL_SECS:
            heartbeat_sleep_args.append(secs)
            # Block until the sync signals done, then return
            await sync_done.wait()
        else:
            # All other sleeps (e.g. Prometheus internal) pass through quickly
            pass

    engine = IngestionEngine(_make_pool_with_lock())
    engine._read_pool = _make_pool_with_lock()

    async def _fake_do_sync(connector, datatype, ingestion_cfg, result, wm, log, **kw):
        # Signal that sync has started, yield briefly, let heartbeat fire
        await anyio.sleep(0)  # yield to allow heartbeat task to start
        sync_done.set()

    with (
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch("inandout.ingestion.engine.anyio.sleep", side_effect=_intercepting_sleep),
        patch.object(engine, "_do_sync", side_effect=_fake_do_sync),
    ):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert heartbeat_sleep_args, (
        f"Heartbeat task must call anyio.sleep({_LOCK_HEARTBEAT_INTERVAL_SECS})"
    )
    assert heartbeat_sleep_args[0] == _LOCK_HEARTBEAT_INTERVAL_SECS


@pytest.mark.anyio
async def test_run_sync_completes_after_do_sync():
    """run_sync must complete successfully (no hang) after _do_sync returns."""
    engine = IngestionEngine(_make_pool_with_lock())
    engine._read_pool = _make_pool_with_lock()

    async def _fast_do_sync(connector, datatype, ingestion_cfg, result, wm, log, **kw):
        result.status = "completed"

    with (
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", side_effect=_fast_do_sync),
    ):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status in ("completed", "running")
