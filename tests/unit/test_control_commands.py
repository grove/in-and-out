"""Unit tests for control table schema and new commands (Priority 4 — Phase 2)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Migration 011 structure tests
# ---------------------------------------------------------------------------


def test_migration_011_exists():
    """Migration 011_control_columns.py should exist and be importable."""
    import importlib
    mod = importlib.import_module(
        "migrations.versions.011_20260323_control_columns"
    )
    assert hasattr(mod, "upgrade")
    assert hasattr(mod, "downgrade")
    assert mod.revision == "011_20260323"
    assert mod.down_revision == "010_20260323"


def test_operational_tables_ddl_has_issued_by():
    """OPERATIONAL_TABLES_DDL should include issued_by in inout_ops_control."""
    from inandout.postgres.schema import OPERATIONAL_TABLES_DDL

    assert "issued_by" in OPERATIONAL_TABLES_DDL


# ---------------------------------------------------------------------------
# CircuitBreaker.reset() tests
# ---------------------------------------------------------------------------


def test_circuit_breaker_reset_from_open():
    """CircuitBreaker.reset() should set state back to CLOSED."""
    from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState

    cb = CircuitBreaker("testconn", "contacts", failure_threshold=1)
    cb.record_failure()  # trips to OPEN with threshold=1
    assert cb.state == CircuitState.open

    cb.reset()
    assert cb.state == CircuitState.closed
    assert cb._consecutive_failures == 0


def test_circuit_breaker_reset_from_closed():
    """CircuitBreaker.reset() should be a no-op when already CLOSED."""
    from inandout.transport.circuit_breaker import CircuitBreaker, CircuitState

    cb = CircuitBreaker("testconn", "contacts")
    assert cb.state == CircuitState.closed
    cb.reset()
    assert cb.state == CircuitState.closed


def test_circuit_breaker_reset_clears_failure_count():
    """CircuitBreaker.reset() should clear consecutive_failures."""
    from inandout.transport.circuit_breaker import CircuitBreaker

    cb = CircuitBreaker("testconn", "contacts", failure_threshold=10)
    cb._consecutive_failures = 9
    cb.reset()
    assert cb._consecutive_failures == 0


# ---------------------------------------------------------------------------
# ControlDispatcher new commands tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_control_dispatcher_reset_circuit_breaker_command():
    """reset-circuit-breaker command should call circuit_breaker.reset()."""
    from inandout.engine.control import ControlDispatcher
    from inandout.transport.circuit_breaker import _registry, CircuitBreaker, CircuitState

    # Plant a tripped circuit breaker in the registry
    cb = CircuitBreaker("cbconn", "orders", failure_threshold=1)
    cb.record_failure()  # trip it
    assert cb.state == CircuitState.open
    _registry[("cbconn", "orders")] = cb

    mock_pool = MagicMock()
    dispatcher = ControlDispatcher(pool=mock_pool, paused_connectors=set())

    result = dispatcher._cmd_reset_circuit_breaker("cbconn", "orders")
    assert "reset" in result
    assert cb.state == CircuitState.closed

    # Cleanup
    _registry.pop(("cbconn", "orders"), None)


@pytest.mark.anyio
async def test_control_dispatcher_reload_config_command():
    """reload-config command should return reload_requested without raising."""
    from inandout.engine.control import ControlDispatcher

    mock_pool = MagicMock()
    dispatcher = ControlDispatcher(pool=mock_pool, paused_connectors=set())

    result = dispatcher._cmd_reload_config("myconn", "contacts", {})
    assert "reload_requested" in result
    assert "myconn" in result["reload_requested"]


@pytest.mark.anyio
async def test_control_dispatcher_reset_watermark_alias():
    """reset-watermark should execute the same as force_full_sync (clear watermark)."""
    from inandout.engine.control import ControlDispatcher

    executed_sqls: list[str] = []

    mock_cursor = MagicMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_cursor.fetchone = AsyncMock(return_value=None)

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.commit = AsyncMock()

    async def _execute(sql, params=None):
        executed_sqls.append(sql)
        return mock_cursor

    mock_conn.execute = AsyncMock(side_effect=_execute)
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    dispatcher = ControlDispatcher(pool=mock_pool, paused_connectors=set())
    result = await dispatcher._execute(
        "reset-watermark", "myconn", "contacts", {}, None
    )

    assert "cleared" in result
    # Should have executed a DELETE on inout_ops_watermark
    delete_sqls = [s for s in executed_sqls if "DELETE" in s and "watermark" in s]
    assert delete_sqls
