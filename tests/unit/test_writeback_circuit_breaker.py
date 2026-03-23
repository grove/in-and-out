"""Unit tests for T2 #25 — circuit breaker integration in writeback _dispatch_row."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from inandout.transport.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    reset_all,
)


@pytest.fixture(autouse=True)
def clear_cb_registry():
    reset_all()
    yield
    reset_all()


def _make_writeback_cfg(**kwargs):
    from inandout.config.writeback import (
        ConflictResolution,
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        UpdateOperationConfig,
        WritebackConfig,
    )

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/items/${external_id}"),
        insert=OperationConfig(method="POST", path="/items"),
        update=UpdateOperationConfig(method="PATCH", path="/items/${external_id}"),
        delete=OperationConfig(method="DELETE", path="/items/${external_id}"),
    )
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update", "delete"],
        operations=ops,
        **kwargs,
    )


def _make_connector(failure_threshold=5, recovery_timeout=60.0):
    """Build a mock ConnectorConfig with circuit_breaker config."""
    mock = MagicMock()
    mock.name = "testconn"
    mock.connection.base_url = "https://api.test"
    mock.circuit_breaker = {
        "failure_threshold": failure_threshold,
        "recovery_timeout": recovery_timeout,
    }
    return mock


# ---------------------------------------------------------------------------
# Open CB → row is skipped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_cb_causes_row_skip():
    """When the circuit breaker is OPEN, _dispatch_row skips the row."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.transport.circuit_breaker import get_circuit_breaker

    pool = MagicMock()
    engine = WritebackEngine(pool)

    connector = _make_connector(failure_threshold=1)
    writeback_cfg = _make_writeback_cfg()
    result = WritebackResult(connector="testconn", datatype="items", delta_table="t")

    # Pre-trip the CB by recording a failure manually
    cb = get_circuit_breaker("testconn", "items", failure_threshold=1, recovery_timeout=9999)
    cb.record_failure()
    assert cb.state == CircuitState.open

    mock_transport = AsyncMock()
    mock_transport._request = AsyncMock()
    mock_transport._raw_request = AsyncMock()

    await engine._dispatch_row(
        mock_transport, connector, writeback_cfg,
        "insert", "ext-1", {"name": "Alice"}, MagicMock(), result,
    )

    # Should skip without making any HTTP calls
    assert result.skipped == 1
    mock_transport._request.assert_not_called()
    mock_transport._raw_request.assert_not_called()


# ---------------------------------------------------------------------------
# HTTP error → CB records failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_error_trips_cb_after_threshold():
    """Consecutive HTTP errors should trip the CB to OPEN state."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.transport.circuit_breaker import get_circuit_breaker

    pool = MagicMock()
    engine = WritebackEngine(pool)

    connector = _make_connector(failure_threshold=2)
    writeback_cfg = _make_writeback_cfg()

    mock_transport = AsyncMock()
    mock_request = MagicMock()
    mock_request.url = "https://api.test/items"
    mock_transport._request = AsyncMock(
        side_effect=httpx.HTTPStatusError("Server Error", request=mock_request, response=MagicMock(status_code=500))
    )
    mock_transport._raw_request = AsyncMock()

    cb = get_circuit_breaker("testconn", "items", failure_threshold=2)
    assert cb.state == CircuitState.closed

    # First failure
    result1 = WritebackResult(connector="testconn", datatype="items", delta_table="t")
    await engine._dispatch_row(
        mock_transport, connector, writeback_cfg,
        "delete", "ext-1", {}, MagicMock(), result1,
    )
    assert result1.failed == 1
    assert cb.state == CircuitState.closed  # still closed after 1

    # Second failure → trips open
    result2 = WritebackResult(connector="testconn", datatype="items", delta_table="t")
    await engine._dispatch_row(
        mock_transport, connector, writeback_cfg,
        "delete", "ext-2", {}, MagicMock(), result2,
    )
    assert result2.failed == 1
    assert cb.state == CircuitState.open  # now open


# ---------------------------------------------------------------------------
# Successful write → CB records success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_write_records_cb_success():
    """A successful HTTP write should call record_success on the CB."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.transport.circuit_breaker import get_circuit_breaker

    pool = MagicMock()
    engine = WritebackEngine(pool)

    connector = _make_connector(failure_threshold=3)
    writeback_cfg = _make_writeback_cfg()
    result = WritebackResult(connector="testconn", datatype="items", delta_table="t")

    # Pre-set a failure to make sure success clears it
    cb = get_circuit_breaker("testconn", "items", failure_threshold=3)
    cb.record_failure()
    assert cb._consecutive_failures == 1

    mock_transport = AsyncMock()
    mock_transport._request = AsyncMock()
    mock_transport._raw_request = AsyncMock()

    await engine._dispatch_row(
        mock_transport, connector, writeback_cfg,
        "delete", "ext-1", {}, MagicMock(), result,
    )

    # Delete uses _request → success
    assert result.processed == 1
    assert result.failed == 0
    # CB failure counter reset by record_success
    assert cb._consecutive_failures == 0
    assert cb.state == CircuitState.closed


# ---------------------------------------------------------------------------
# Half-open probe: success closes CB
# ---------------------------------------------------------------------------

def test_half_open_success_closes_cb():
    """After transitioning to HALF_OPEN, a recorded success closes the CB."""
    cb = CircuitBreaker("c", "d", failure_threshold=1, recovery_timeout=0.001)
    cb.record_failure()
    assert cb.state == CircuitState.open

    import time
    time.sleep(0.01)  # Wait for recovery_timeout to elapse

    # Transition to HALF_OPEN by querying state
    assert cb.state == CircuitState.half_open
    assert cb.allow_request() is True

    cb.record_success()
    assert cb.state == CircuitState.closed


# ---------------------------------------------------------------------------
# Half-open probe: failure keeps CB open
# ---------------------------------------------------------------------------

def test_half_open_failure_reopens_cb():
    """After transitioning to HALF_OPEN, a recorded failure re-opens the CB."""
    cb = CircuitBreaker("c", "d", failure_threshold=1, recovery_timeout=0.001)
    cb.record_failure()
    assert cb.state == CircuitState.open

    import time
    time.sleep(0.01)

    assert cb.state == CircuitState.half_open
    cb.record_failure()
    assert cb.state == CircuitState.open


# ---------------------------------------------------------------------------
# Control dispatcher: reset-circuit-breaker
# ---------------------------------------------------------------------------

def test_control_reset_circuit_breaker():
    """reset-circuit-breaker control command resets the CB to CLOSED."""
    from inandout.transport.circuit_breaker import get_circuit_breaker

    cb = get_circuit_breaker("myconn", "contacts", failure_threshold=2)
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.open

    cb.reset()
    assert cb.state == CircuitState.closed
    assert cb._consecutive_failures == 0
