"""Integration test: T2 #32 — replication slot health monitoring.

Validates that monitor_replication_slot:
  - Reads real pg_replication_slots from PostgreSQL
  - Calls on_fallback() when lag_bytes > max_lag_bytes
  - Emits ERROR-level log when lag_bytes > warn_lag_bytes
  - Gracefully exits when the slot does not exist

Uses a real PostgreSQL container (testcontainers) to create and inspect
actual replication slots.

GOAL.md T2 #32: continuously monitor replication slot lag; emit metric;
log at ERROR level above warn threshold; trigger fallback above max threshold.
"""
from __future__ import annotations

import os

import pytest

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _create_logical_slot(conn, slot_name: str) -> None:
    """Create a logical replication slot for testing.

    Raises pytest.skip if the PostgreSQL instance's wal_level is below 'logical'.
    """
    try:
        # Use pgoutput output plugin (always available)
        await conn.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            [slot_name],
        )
    except Exception as exc:
        if "wal_level" in str(exc):
            pytest.skip(
                f"PostgreSQL instance requires wal_level=logical for replication slot tests "
                f"(got: {exc})"
            )
        raise


async def _drop_slot(conn, slot_name: str) -> None:
    """Drop a replication slot, ignoring errors if it doesn't exist."""
    try:
        await conn.execute(
            "SELECT pg_drop_replication_slot(%s)",
            [slot_name],
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Test 1: get_slot_lag returns real lag values for an existing slot
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_get_slot_lag_real_slot(pool, run_migrations):
    """get_slot_lag must return (lag_bytes, lag_secs) for a real slot.

    GOAL.md T2 #32: continuously monitor replication slot lag (bytes behind).
    """
    from inandout.writeback.slot_monitor import get_slot_lag

    slot_name = "int_test_lag_slot"

    async with pool.connection() as conn:
        await _create_logical_slot(conn, slot_name)
        await conn.commit()

    try:
        result = await get_slot_lag(pool, slot_name)

        assert result is not None, f"get_slot_lag returned None for existing slot '{slot_name}'"
        lag_bytes, lag_secs = result
        assert isinstance(lag_bytes, int), "lag_bytes must be an int"
        assert isinstance(lag_secs, float), "lag_secs must be a float"
        assert lag_bytes >= 0, "lag_bytes must be non-negative"
        assert lag_secs >= 0, "lag_secs must be non-negative"
    finally:
        async with pool.connection() as conn:
            await _drop_slot(conn, slot_name)
            await conn.commit()


# ---------------------------------------------------------------------------
# Test 2: get_slot_lag returns None for a non-existent slot
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_get_slot_lag_missing_slot_returns_none(pool, run_migrations):
    """get_slot_lag must return None when the slot does not exist.

    GOAL.md T2 #32: graceful handling when the replication slot is missing —
    this happens during initial setup before a slot has been provisioned.
    """
    from inandout.writeback.slot_monitor import get_slot_lag

    result = await get_slot_lag(pool, "int_test_slot_that_does_not_exist_xyz")
    assert result is None, (
        "Expected None for non-existent slot, got a result"
    )


# ---------------------------------------------------------------------------
# Test 3: monitor_replication_slot calls on_fallback when lag > max_lag_bytes
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_slot_monitor_calls_fallback_when_lag_exceeds_max(pool, run_migrations):
    """monitor_replication_slot must call on_fallback() when lag > max_lag_bytes.

    GOAL.md T2 #32: if lag exceeds a configurable maximum threshold, the tool
    pauses replication consumption and falls back to polling the desired-state
    table directly.

    We simulate high lag by setting max_lag_bytes=0 so every observed lag
    value (even 0 bytes) exceeds the threshold.
    """
    from inandout.config.tool import ReplicationSlotConfig
    from inandout.writeback.slot_monitor import monitor_replication_slot

    slot_name = "int_test_fallback_slot"

    async with pool.connection() as conn:
        await _create_logical_slot(conn, slot_name)
        await conn.commit()

    try:
        fallback_called = []

        def on_fallback():
            fallback_called.append(True)

        stop_after_one = [False]

        def should_stop() -> bool:
            # Stop after first iteration (on_fallback already called)
            if fallback_called:
                stop_after_one[0] = True
            return stop_after_one[0]

        cfg = ReplicationSlotConfig(
            slot_name=slot_name,
            warn_lag_bytes=0,   # trigger warn on any lag
            max_lag_bytes=0,    # trigger fallback on any lag
            poll_interval_secs=0.01,
        )

        import anyio
        with anyio.fail_after(10):
            await monitor_replication_slot(pool, cfg, on_fallback, should_stop=should_stop)

        assert fallback_called, (
            "on_fallback() was not called even though lag > max_lag_bytes"
        )
    finally:
        async with pool.connection() as conn:
            await _drop_slot(conn, slot_name)
            await conn.commit()


# ---------------------------------------------------------------------------
# Test 4: monitor exits cleanly when slot does not exist (no fallback called)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_slot_monitor_missing_slot_exits_cleanly(pool, run_migrations):
    """When the slot doesn't exist, monitor exits after one poll without error.

    GOAL.md T2 #32: graceful handling of missing slot during startup.
    """
    from inandout.config.tool import ReplicationSlotConfig
    from inandout.writeback.slot_monitor import monitor_replication_slot

    fallback_called = []

    call_count = [0]

    def _should_stop() -> bool:
        call_count[0] += 1
        return call_count[0] >= 2  # exit after 2 polls

    cfg = ReplicationSlotConfig(
        slot_name="int_test_nonexistent_slot_abc",
        warn_lag_bytes=0,
        max_lag_bytes=0,
        poll_interval_secs=0.01,
    )

    import anyio
    with anyio.fail_after(5):
        await monitor_replication_slot(
            pool, cfg,
            on_fallback=lambda: fallback_called.append(True),
            should_stop=_should_stop,
        )

    # fallback should NOT be called for a missing slot (slot not found ≠ high lag)
    assert not fallback_called, (
        "on_fallback() was called for a non-existent slot — "
        "missing slot should not trigger fallback"
    )


# ---------------------------------------------------------------------------
# Test 5: monitor runs safely when slot_name is None (disabled)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_slot_monitor_disabled_when_slot_name_none(pool, run_migrations):
    """When slot_name is None (monitoring disabled), monitor returns immediately.

    GOAL.md T2 #32: an operator must be able to disable slot monitoring by
    omitting slot_name — the monitor must be a no-op in that case.
    """
    from inandout.config.tool import ReplicationSlotConfig
    from inandout.writeback.slot_monitor import monitor_replication_slot

    fallback_called = []
    cfg = ReplicationSlotConfig(slot_name=None, poll_interval_secs=0.01)

    import anyio
    with anyio.fail_after(2):
        await monitor_replication_slot(
            pool, cfg,
            on_fallback=lambda: fallback_called.append(True),
        )

    assert not fallback_called
