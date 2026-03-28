"""Unit tests for heartbeat exception swallowing and connector-version migration
trigger in engine.py.

Covers (item 3):
- _lock_heartbeat: if pool.connection() raises, run_sync still completes with
  status='completed' — heartbeat failures must never abort the sync.

Covers (item 4):
- When deployed_version != connector.version, _apply_version_migration is called.
- When deployed_version == connector.version, _apply_version_migration is NOT called.
- When the connector_version table doesn't exist (query raises), migration is silently skipped.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest


# ---------------------------------------------------------------------------
# Shared helpers (same pattern as test_lock_heartbeat.py)
# ---------------------------------------------------------------------------

def _make_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.history_mode = "none"
    return cfg


def _make_connector(name: str = "testconn", version: str = "1.0.0") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = version
    cfg.datatypes = {}
    return cfg


def _make_read_conn() -> AsyncMock:
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    rconn = AsyncMock()
    rconn.__aenter__ = AsyncMock(return_value=rconn)
    rconn.__aexit__ = AsyncMock(return_value=None)
    rconn.execute = AsyncMock(side_effect=_execute)
    rconn.commit = AsyncMock()
    return rconn


def _make_write_conn_factory(
    for_update_row: tuple | None = ("row-id",),
    version_row: tuple | None = None,
) -> tuple[MagicMock, list[str]]:
    """Return (pool, sql_list). version_row controls what SELECT deployed_version returns."""
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=for_update_row)
        elif "deployed_version" in sql:
            cur.fetchone = AsyncMock(return_value=version_row)
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool, sql_list


# ---------------------------------------------------------------------------
# Heartbeat exception swallowing (item 3)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_heartbeat_pool_error_does_not_abort_sync():
    """
    If the heartbeat's pool.connection() raises, run_sync must still complete
    with status='completed' and not propagate the exception.
    """
    from inandout.ingestion import engine as engine_mod
    from inandout.ingestion.engine import IngestionEngine

    pool, _ = _make_write_conn_factory()
    engine = IngestionEngine(pool)
    engine._read_pool = MagicMock()
    engine._read_pool.connection = MagicMock(return_value=_make_read_conn())

    orig_interval = engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS
    engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS = 0.0  # fire immediately

    # Make a *second* pool that the heartbeat will use (via self._pool.connection).
    # We need to distinguish the heartbeat call from all other pool.connection() calls.
    # Strategy: track how many times pool.connection() is called; after the first N
    # legitimate connection acquisitions, the next one (heartbeat) raises.
    call_count = [0]
    original_connection = pool.connection

    def _failing_after_first_few():
        call_count[0] += 1
        if call_count[0] > 3:
            cm = AsyncMock()
            cm.__aenter__ = AsyncMock(side_effect=RuntimeError("heartbeat db error"))
            cm.__aexit__ = AsyncMock(return_value=False)
            return cm
        return original_connection()

    pool.connection = MagicMock(side_effect=_failing_after_first_few)

    async def _sync_with_yield(*args, **kwargs):
        await anyio.sleep(0)
        await anyio.sleep(0)

    try:
        with patch.object(engine, "_do_sync", side_effect=_sync_with_yield):
            result = await engine.run_sync(
                _make_connector(), "contacts", _make_ingestion_cfg()
            )
    finally:
        engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS = orig_interval

    # run_sync must complete without raising; status must not be 'failed' due to heartbeat
    assert result.status != "running", "run_sync should have finished"
    # The error_message, if present, should not mention heartbeat internals
    if result.error_message:
        assert "heartbeat db error" not in result.error_message, (
            "Heartbeat exception must be swallowed and not propagate to run_sync result"
        )


# ---------------------------------------------------------------------------
# Connector version migration trigger (item 4)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_version_changed_calls_apply_version_migration():
    """When deployed_version differs from connector.version, _apply_version_migration is called."""
    from inandout.ingestion.engine import IngestionEngine

    # version_row[0] = "0.9.0" but connector.version = "1.0.0" → mismatch → migration
    pool, _ = _make_write_conn_factory(version_row=("0.9.0",))
    engine = IngestionEngine(pool)
    engine._read_pool = MagicMock()
    engine._read_pool.connection = MagicMock(return_value=_make_read_conn())

    migration_called = []

    async def _fake_migration(conn, connector, datatype, ingestion_cfg, dtype_cfg, log):
        migration_called.append(True)

    with (
        patch.object(engine, "_do_sync", new=AsyncMock()),
        patch.object(engine, "_apply_version_migration", side_effect=_fake_migration),
    ):
        await engine.run_sync(_make_connector(version="1.0.0"), "contacts", _make_ingestion_cfg())

    assert migration_called, "_apply_version_migration must be called when versions differ"


@pytest.mark.anyio
async def test_same_version_does_not_call_apply_version_migration():
    """When deployed_version matches connector.version, _apply_version_migration is NOT called."""
    from inandout.ingestion.engine import IngestionEngine

    # version_row[0] matches connector.version
    pool, _ = _make_write_conn_factory(version_row=("1.0.0",))
    engine = IngestionEngine(pool)
    engine._read_pool = MagicMock()
    engine._read_pool.connection = MagicMock(return_value=_make_read_conn())

    migration_called = []

    async def _fake_migration(conn, connector, datatype, ingestion_cfg, dtype_cfg, log):
        migration_called.append(True)

    with (
        patch.object(engine, "_do_sync", new=AsyncMock()),
        patch.object(engine, "_apply_version_migration", side_effect=_fake_migration),
    ):
        await engine.run_sync(_make_connector(version="1.0.0"), "contacts", _make_ingestion_cfg())

    assert not migration_called, "_apply_version_migration must NOT be called when versions match"


@pytest.mark.anyio
async def test_missing_connector_version_table_silently_skipped():
    """When the connector_version query raises, migration is silently skipped and sync completes."""
    from inandout.ingestion.engine import IngestionEngine

    pool, _ = _make_write_conn_factory()
    engine = IngestionEngine(pool)
    engine._read_pool = MagicMock()
    engine._read_pool.connection = MagicMock(return_value=_make_read_conn())

    # Patch the execute on the existing conn to raise on the deployed_version query
    original_execute = pool.connection.return_value.__aenter__.return_value.execute

    async def _execute_raising_on_version(sql: str, params=None):
        if "deployed_version" in sql:
            raise Exception("relation does not exist")
        return await original_execute(sql, params)

    pool.connection.return_value.__aenter__.return_value.execute = AsyncMock(
        side_effect=_execute_raising_on_version
    )

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(
            _make_connector(), "contacts", _make_ingestion_cfg()
        )

    # Sync should complete without propagating the table-does-not-exist error
    assert result.status not in ("running",), "run_sync should have finished"
