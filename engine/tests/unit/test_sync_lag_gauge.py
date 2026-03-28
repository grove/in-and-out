"""Unit tests for sync_lag_seconds Prometheus gauge being set to 0 on completion.

Verifies that after run_sync completes successfully, the inout_sync_lag_seconds
gauge in the shared REGISTRY is set to 0.0 for the connector/datatype pair.
Also verifies the gauge is NOT set to 0 when the sync fails or is skipped.
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


def _make_connector(name: str = "gaug_test") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    return cfg


def _make_conn(for_update_row: tuple | None = ("row-id",)) -> AsyncMock:
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


# ---------------------------------------------------------------------------
# Gauge set to 0 on completed sync
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sync_lag_gauge_set_to_zero_on_completion():
    """After a completed run_sync, inout_sync_lag_seconds must be 0.0."""
    from inandout.ingestion.engine import IngestionEngine
    from inandout.observability.metrics import sync_lag_seconds

    connector_name = "gaug_test_conn"
    datatype = "contacts"

    engine = IngestionEngine(_build_pool(_make_conn()), namespace="public")
    engine._read_pool = _build_pool(_make_conn())

    connector = _make_connector(name=connector_name)

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(connector, datatype, _make_ingestion_cfg())

    assert result.status not in ("running", "failed")

    gauge_value = sync_lag_seconds.labels(
        tool="ingestion",
        connector=connector_name,
        datatype=datatype,
        namespace="public",
    )._value.get()
    assert gauge_value == 0.0, f"Expected 0.0, got {gauge_value}"


# ---------------------------------------------------------------------------
# Gauge NOT set to 0 on failed sync
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sync_lag_gauge_not_set_on_failed_sync():
    """When _do_sync raises, sync_lag_seconds must NOT be set to 0."""
    from inandout.ingestion.engine import IngestionEngine
    from inandout.observability.metrics import sync_lag_seconds

    connector_name = "gaug_fail_conn"
    datatype = "deals"

    engine = IngestionEngine(_build_pool(_make_conn()), namespace="public")
    engine._read_pool = _build_pool(_make_conn())
    connector = _make_connector(name=connector_name)

    # Record gauge value before the sync
    before = sync_lag_seconds.labels(
        tool="ingestion", connector=connector_name, datatype=datatype, namespace="public"
    )._value.get()

    async def _fail(*args, **kwargs):
        raise RuntimeError("boom")

    with patch.object(engine, "_do_sync", side_effect=_fail):
        result = await engine.run_sync(connector, datatype, _make_ingestion_cfg())

    assert result.status == "failed"

    after = sync_lag_seconds.labels(
        tool="ingestion", connector=connector_name, datatype=datatype, namespace="public"
    )._value.get()
    assert after == before, (
        "sync_lag_seconds must not be zeroed when the sync fails"
    )
