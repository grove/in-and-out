"""Unit tests for _webhook_dedup_cleanup_loop TTL deletion.

Covers:
- Loop exits cleanly when _draining is set.
- DELETE FROM inout_ops_webhook_seen with correct interval is issued when
  connector has dedup_ttl configured.
- Connectors without webhook config are skipped (no DELETE issued).
- DB exceptions are swallowed per connector (loop continues).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod
from inandout.ingestion.daemon import _webhook_dedup_cleanup_loop


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector_cfg(
    name: str = "hubspot",
    has_webhooks: bool = True,
    dedup_ttl: str = "24h",
    event_id_field: str | None = "event_id",
) -> MagicMock:
    connector = MagicMock()
    connector.name = name
    if has_webhooks and event_id_field:
        wh = MagicMock()
        wh.event_id_field = event_id_field
        wh.dedup_ttl = dedup_ttl
        connector.webhooks = wh
    else:
        connector.webhooks = None
    file_cfg = MagicMock()
    file_cfg.connector = connector
    return file_cfg


def _make_pool_capturing() -> tuple[MagicMock, list[str]]:
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        cur = AsyncMock()
        cur.rowcount = 5
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
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_webhook_dedup_loop_drains_cleanly():
    """Loop must exit cleanly when _draining is set after first sleep."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        _daemon_mod._draining = True

    pool, _ = _make_pool_capturing()

    try:
        with patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep):
            await _webhook_dedup_cleanup_loop(pool, [], interval_secs=3600.0)
    finally:
        _daemon_mod._draining = original_draining

    assert ticks >= 1


@pytest.mark.anyio
async def test_webhook_dedup_loop_issues_delete_with_ttl():
    """Connector with dedup_ttl must result in DELETE FROM inout_ops_webhook_seen."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    cfg = _make_connector_cfg(name="hubspot", dedup_ttl="24h")
    pool, sql_list = _make_pool_capturing()

    try:
        with patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep):
            await _webhook_dedup_cleanup_loop(pool, [cfg], interval_secs=3600.0)
    finally:
        _daemon_mod._draining = original_draining

    delete_sqls = [s for s in sql_list if "DELETE" in s and "webhook_seen" in s]
    assert delete_sqls, f"Expected DELETE on webhook_seen, got: {sql_list}"


@pytest.mark.anyio
async def test_webhook_dedup_loop_skips_connector_without_webhooks():
    """Connector with webhooks=None must not trigger any SQL."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    cfg = _make_connector_cfg(has_webhooks=False, event_id_field=None)
    pool, sql_list = _make_pool_capturing()

    try:
        with patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep):
            await _webhook_dedup_cleanup_loop(pool, [cfg], interval_secs=3600.0)
    finally:
        _daemon_mod._draining = original_draining

    assert not sql_list, f"Expected no SQL for connector without webhooks, got: {sql_list}"


@pytest.mark.anyio
async def test_webhook_dedup_loop_skips_connector_without_event_id_field():
    """Connector with event_id_field=None must be skipped."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    cfg = _make_connector_cfg(event_id_field=None)
    pool, sql_list = _make_pool_capturing()

    try:
        with patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep):
            await _webhook_dedup_cleanup_loop(pool, [cfg], interval_secs=3600.0)
    finally:
        _daemon_mod._draining = original_draining

    assert not sql_list


@pytest.mark.anyio
async def test_webhook_dedup_loop_swallows_db_exception():
    """DB exception during cleanup must not propagate."""
    ticks = 0
    original_draining = _daemon_mod._draining

    async def _fake_sleep(secs: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            _daemon_mod._draining = True

    async def _failing_execute(sql: str, params=None):
        raise RuntimeError("relation does not exist")

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_failing_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)

    cfg = _make_connector_cfg(name="hubspot", dedup_ttl="24h")

    try:
        with patch("inandout.ingestion.daemon.anyio.sleep", side_effect=_fake_sleep):
            # Must not raise
            await _webhook_dedup_cleanup_loop(pool, [cfg], interval_secs=3600.0)
    finally:
        _daemon_mod._draining = original_draining
