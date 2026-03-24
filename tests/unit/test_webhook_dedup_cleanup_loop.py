"""Unit tests for _webhook_dedup_cleanup_loop in daemon.py.

Covers:
- DELETE FROM inout_ops_webhook_seen is issued for connectors with event_id_field set.
- DELETE is skipped for connectors without event_id_field.
- When multiple connectors are present, only those with event_id_field get a DELETE.
- Invalid dedup_ttl string (e.g. "forever") falls back gracefully to 86400.0 s (24 h).
- Valid dedup_ttl string is parsed and used as-is.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.daemon as _daemon_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector_file_cfg(
    name: str,
    *,
    event_id_field: str | None = None,
    dedup_ttl: str = "24h",
) -> MagicMock:
    """Return a minimal connector_file_cfg stub."""
    wh_cfg: MagicMock | None = None
    if event_id_field is not None:
        wh_cfg = MagicMock()
        wh_cfg.event_id_field = event_id_field
        wh_cfg.dedup_ttl = dedup_ttl

    connector_cfg = MagicMock()
    connector_cfg.name = name
    connector_cfg.webhooks = wh_cfg

    file_cfg = MagicMock()
    file_cfg.connector = connector_cfg
    return file_cfg


def _make_pool_with_sql_log() -> tuple[MagicMock, list[str], list[list]]:
    """Return (pool, sql_log, params_log) where every execute call is recorded."""
    sql_log: list[str] = []
    params_log: list[list] = []

    async def _execute(sql: str, params=None):
        sql_log.append(sql)
        params_log.append(list(params) if params else [])
        cur = AsyncMock()
        cur.rowcount = 3
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool, sql_log, params_log


async def _run_cleanup_loop_one_pass(
    pool: MagicMock,
    connectors: list,
) -> None:
    """
    Drive _webhook_dedup_cleanup_loop through exactly one iteration then exit.

    Strategy: anyio.sleep is patched to a no-op on the first call (the sleep
    before processing), then sets _draining=True on the second call (the sleep
    at the top of the second iteration) so the loop exits cleanly.
    """
    orig_draining = _daemon_mod._draining
    _daemon_mod._draining = False
    sleep_count = 0

    async def _fake_sleep(secs: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 2:
            _daemon_mod._draining = True

    try:
        with patch("anyio.sleep", side_effect=_fake_sleep):
            await _daemon_mod._webhook_dedup_cleanup_loop(pool, connectors, interval_secs=0.0)
    finally:
        _daemon_mod._draining = orig_draining


# ---------------------------------------------------------------------------
# DELETE issued / skipped
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cleanup_loop_issues_delete_for_connector_with_event_id_field():
    """DELETE FROM inout_ops_webhook_seen is issued when connector has event_id_field set."""
    pool, sql_log, _ = _make_pool_with_sql_log()
    connector = _make_connector_file_cfg("hubspot", event_id_field="event_id")

    await _run_cleanup_loop_one_pass(pool, [connector])

    delete_sqls = [s for s in sql_log if "DELETE FROM inout_ops_webhook_seen" in s]
    assert delete_sqls, "Expected DELETE FROM inout_ops_webhook_seen to be issued"
    assert all("connector = %s" in s for s in delete_sqls)
    assert all("received_at <" in s for s in delete_sqls)


@pytest.mark.anyio
async def test_cleanup_loop_skips_delete_for_connector_without_event_id_field():
    """No DELETE should be issued when connector has webhooks=None or event_id_field=None."""
    pool, sql_log, _ = _make_pool_with_sql_log()
    connector = _make_connector_file_cfg("salesforce", event_id_field=None)

    await _run_cleanup_loop_one_pass(pool, [connector])

    delete_sqls = [s for s in sql_log if "DELETE FROM inout_ops_webhook_seen" in s]
    assert not delete_sqls, "Expected no DELETE for connector without event_id_field"


@pytest.mark.anyio
async def test_cleanup_loop_mixed_connectors_only_deletes_webhook_ones():
    """With mixed connectors, only those with event_id_field receive a DELETE."""
    pool, sql_log, _ = _make_pool_with_sql_log()
    with_wh = _make_connector_file_cfg("hubspot", event_id_field="event_id")
    without_wh = _make_connector_file_cfg("salesforce", event_id_field=None)

    await _run_cleanup_loop_one_pass(pool, [with_wh, without_wh])

    delete_sqls = [s for s in sql_log if "DELETE FROM inout_ops_webhook_seen" in s]
    assert len(delete_sqls) == 1, (
        f"Expected exactly one DELETE (for hubspot), got {len(delete_sqls)}"
    )


# ---------------------------------------------------------------------------
# dedup_ttl parse error resilience (item 5)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cleanup_loop_invalid_dedup_ttl_falls_back_to_24h():
    """Invalid dedup_ttl (e.g. 'forever') must fall back to 86400.0 s (24 h)."""
    pool, sql_log, params_log = _make_pool_with_sql_log()
    connector = _make_connector_file_cfg("hubspot", event_id_field="event_id", dedup_ttl="forever")

    await _run_cleanup_loop_one_pass(pool, [connector])

    # params for DELETE: [connector_name, ttl_secs] — ttl_secs is params[1]
    delete_params = [
        p for s, p in zip(sql_log, params_log)
        if "DELETE FROM inout_ops_webhook_seen" in s
    ]
    assert delete_params, "Expected DELETE to be called even with an invalid dedup_ttl"
    assert delete_params[0][1] == 86400.0, (
        f"Expected fallback TTL of 86400.0 s, got {delete_params[0][1]}"
    )


@pytest.mark.anyio
async def test_cleanup_loop_valid_dedup_ttl_is_used_directly():
    """Valid dedup_ttl '1h' should be parsed to 3600.0 s and used in the DELETE."""
    pool, sql_log, params_log = _make_pool_with_sql_log()
    connector = _make_connector_file_cfg("hubspot", event_id_field="event_id", dedup_ttl="1h")

    await _run_cleanup_loop_one_pass(pool, [connector])

    delete_params = [
        p for s, p in zip(sql_log, params_log)
        if "DELETE FROM inout_ops_webhook_seen" in s
    ]
    assert delete_params, "Expected DELETE to be issued"
    assert delete_params[0][1] == 3600.0, (
        f"Expected TTL of 3600.0 s for '1h', got {delete_params[0][1]}"
    )
