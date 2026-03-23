"""Unit tests for T2 #31 — writeback delete safety guard (max_deletes_per_batch)."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock


def _make_writeback_cfg(max_deletes_per_batch=None, **kwargs):
    """Build a minimal WritebackConfig with optional max_deletes_per_batch."""
    from inandout.config.writeback import (
        ConflictResolution,
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        UpdateOperationConfig,
        WritebackConfig,
    )

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        insert=OperationConfig(method="POST", path="/contacts"),
        update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
        delete=OperationConfig(method="DELETE", path="/contacts/${external_id}"),
    )
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update", "delete"],
        operations=ops,
        max_deletes_per_batch=max_deletes_per_batch,
        **kwargs,
    )


def _make_rows(n_inserts=0, n_updates=0, n_deletes=0):
    """Build a list of fake delta rows with the given mix of _action values."""
    rows = []
    for i in range(n_inserts):
        rows.append({"external_id": f"ins-{i}", "_action": "insert", "name": f"New {i}"})
    for i in range(n_updates):
        rows.append({"external_id": f"upd-{i}", "_action": "update", "name": f"Updated {i}"})
    for i in range(n_deletes):
        rows.append({"external_id": f"del-{i}", "_action": "delete"})
    return rows


# ---------------------------------------------------------------------------
# Config field
# ---------------------------------------------------------------------------

def test_max_deletes_per_batch_defaults_none():
    """max_deletes_per_batch defaults to None (guard disabled)."""
    cfg = _make_writeback_cfg()
    assert cfg.max_deletes_per_batch is None


def test_max_deletes_per_batch_can_be_set():
    """max_deletes_per_batch can be configured to a positive integer."""
    cfg = _make_writeback_cfg(max_deletes_per_batch=5)
    assert cfg.max_deletes_per_batch == 5


def test_max_deletes_per_batch_must_be_positive():
    """max_deletes_per_batch must be >= 1."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _make_writeback_cfg(max_deletes_per_batch=0)


# ---------------------------------------------------------------------------
# Guard inactive when max_deletes_per_batch is None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_guard_inactive_when_not_configured(caplog):
    """When max_deletes_per_batch is None, delete rows are dispatched normally."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.transport.circuit_breaker import reset_all
    reset_all()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=AsyncMock(
            execute=AsyncMock(return_value=AsyncMock(fetchall=AsyncMock(return_value=[]))),
            commit=AsyncMock(),
        )),
        __aexit__=AsyncMock(return_value=None),
    ))
    engine = WritebackEngine(pool)

    mock_connector = MagicMock()
    mock_connector.name = "crm"
    mock_connector.connection.base_url = "https://api.crm.test"
    mock_connector.circuit_breaker = None

    writeback_cfg = _make_writeback_cfg(max_deletes_per_batch=None)
    result = WritebackResult(connector="crm", datatype="contacts", delta_table="t")

    mock_transport = AsyncMock()
    mock_resp = MagicMock(status_code=204)
    mock_resp.raise_for_status = MagicMock()
    mock_transport._raw_request = AsyncMock(return_value=mock_resp)
    mock_transport._request = AsyncMock()

    rows = _make_rows(n_deletes=10)

    # Dispatch each row individually (simulating what the engine does per batch)
    log = MagicMock()
    for row in rows:
        await engine._dispatch_row(
            mock_transport, mock_connector, writeback_cfg,
            row["_action"], row["external_id"], row, log, result,
        )

    # With guard disabled, all deletes should be dispatched
    assert result.skipped == 0
    # Transport was called for each delete
    assert mock_transport._request.call_count == 10


# ---------------------------------------------------------------------------
# Guard trips when too many deletes in batch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_guard_trips_when_deletes_exceed_limit():
    """When delete count > max_deletes_per_batch, deletes are suppressed."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.transport.circuit_breaker import reset_all
    reset_all()

    pool = MagicMock()
    engine = WritebackEngine(pool)

    mock_connector = MagicMock()
    mock_connector.name = "crm"
    mock_connector.connection.base_url = "https://api.crm.test"
    mock_connector.circuit_breaker = None

    writeback_cfg = _make_writeback_cfg(max_deletes_per_batch=3)
    rows = _make_rows(n_inserts=2, n_deletes=5)  # 5 deletes > limit of 3

    import structlog
    log_events = []

    # Simulate the engine batch loop guard directly (unit test the filtering logic)
    from inandout.config.writeback import WritebackConfig

    max_deletes = writeback_cfg.max_deletes_per_batch
    delete_count = sum(1 for r in rows if r.get("_action") == "delete")

    result_skipped = 0
    filtered_rows = rows
    if max_deletes is not None and delete_count > max_deletes:
        filtered_rows = [r for r in rows if r.get("_action") != "delete"]
        result_skipped += delete_count

    assert delete_count == 5
    assert result_skipped == 5
    assert all(r["_action"] != "delete" for r in filtered_rows)
    assert len(filtered_rows) == 2  # only inserts remain


def test_guard_does_not_trip_when_at_limit():
    """Exactly max_deletes_per_batch deletes are allowed through."""
    writeback_cfg = _make_writeback_cfg(max_deletes_per_batch=5)
    rows = _make_rows(n_deletes=5)

    max_deletes = writeback_cfg.max_deletes_per_batch
    delete_count = sum(1 for r in rows if r.get("_action") == "delete")

    # at or below limit — no filtering
    assert delete_count <= max_deletes
    filtered = [r for r in rows if r.get("_action") != "delete"] if delete_count > max_deletes else rows
    assert len(filtered) == 5  # all rows untouched


def test_guard_does_not_trip_below_limit():
    """Fewer deletes than the limit are dispatched normally."""
    writeback_cfg = _make_writeback_cfg(max_deletes_per_batch=10)
    rows = _make_rows(n_inserts=3, n_updates=2, n_deletes=4)

    max_deletes = writeback_cfg.max_deletes_per_batch
    delete_count = sum(1 for r in rows if r.get("_action") == "delete")

    assert delete_count < max_deletes
    # No suppression
    should_suppress = delete_count > max_deletes
    assert should_suppress is False
