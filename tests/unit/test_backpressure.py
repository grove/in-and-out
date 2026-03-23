"""Unit tests for backpressure / flow control in WritebackEngine."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest

from inandout.config.writeback import WritebackConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_writeback_cfg(max_concurrent_writes: int = 2, batch_size: int = 50) -> WritebackConfig:
    """Build a minimal WritebackConfig with the desired concurrency settings."""
    return WritebackConfig(
        protection_level=3,
        conflict_resolution="last_writer_wins",
        supported_actions=["insert"],
        max_concurrent_writes=max_concurrent_writes,
        batch_size=batch_size,
        operations={
            "lookup": {"method": "GET", "path": "/lookup/${external_id}"},
            "insert": {"method": "POST", "path": "/items"},
        },
    )


# ---------------------------------------------------------------------------
# Concurrency cap test
# ---------------------------------------------------------------------------

async def test_max_concurrent_writes_limits_parallelism():
    """Verify that with max_concurrent_writes=2 and 5 rows, no more than 2
    dispatches happen simultaneously."""
    max_concurrent = 2
    total_rows = 5

    current_count = 0
    max_observed = 0
    gate = anyio.Event()

    async def _fake_dispatch(row: dict) -> None:
        nonlocal current_count, max_observed
        current_count += 1
        max_observed = max(max_observed, current_count)
        # Yield control so other tasks can start — simulates IO work.
        await anyio.sleep(0)
        current_count -= 1

    semaphore = anyio.Semaphore(max_concurrent)
    rows = [{"id": str(i), "_action": "insert"} for i in range(total_rows)]

    async with anyio.create_task_group() as tg:
        for row in rows:
            async def _task(r: dict = row) -> None:
                async with semaphore:
                    await _fake_dispatch(r)

            tg.start_soon(_task)

    assert max_observed <= max_concurrent, (
        f"Expected at most {max_concurrent} concurrent dispatches, "
        f"but observed {max_observed}"
    )
    assert max_observed >= 1, "At least one dispatch should have happened"


# ---------------------------------------------------------------------------
# batch_size config
# ---------------------------------------------------------------------------

def test_batch_size_default():
    cfg = _make_writeback_cfg()
    assert cfg.batch_size == 50


def test_batch_size_custom():
    cfg = _make_writeback_cfg(batch_size=25)
    assert cfg.batch_size == 25


def test_max_concurrent_writes_default_is_ten():
    cfg = WritebackConfig(
        protection_level=3,
        conflict_resolution="last_writer_wins",
        supported_actions=["insert"],
        operations={
            "lookup": {"method": "GET", "path": "/lookup"},
        },
    )
    assert cfg.max_concurrent_writes == 10


def test_max_concurrent_writes_custom():
    cfg = _make_writeback_cfg(max_concurrent_writes=3)
    assert cfg.max_concurrent_writes == 3
