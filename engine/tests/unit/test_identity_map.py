"""Unit tests for identity map (Priority 5 — Phase 2)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Migration 012 structure tests
# ---------------------------------------------------------------------------


def test_migration_012_exists():
    """Migration 012_identity_map.py should exist and be importable."""
    import importlib
    mod = importlib.import_module(
        "migrations.versions.012_20260323_identity_map"
    )
    assert hasattr(mod, "upgrade")
    assert hasattr(mod, "downgrade")
    assert mod.revision == "012_20260323"
    assert mod.down_revision == "011_20260323"


def test_migration_012_creates_identity_map_table():
    """Migration 012 upgrade SQL should create inout_ops_identity_map."""
    import inspect
    import importlib
    mod = importlib.import_module(
        "migrations.versions.012_20260323_identity_map"
    )
    source = inspect.getsource(mod.upgrade)
    assert "inout_ops_identity_map" in source
    assert "external_id" in source
    assert "internal_id" in source


# ---------------------------------------------------------------------------
# WritebackEngine._record_identity_map tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_record_identity_map_upserts_row():
    """_record_identity_map should UPSERT into inout_ops_identity_map."""
    from inandout.writeback.engine import WritebackEngine

    executed_sqls: list[str] = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.commit = AsyncMock()

    async def _execute(sql, params=None):
        executed_sqls.append(sql)
        return MagicMock()

    mock_conn.execute = AsyncMock(side_effect=_execute)
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = WritebackEngine(pool=mock_pool)
    await engine._record_identity_map(
        connector="myconn",
        datatype="contacts",
        external_id="ext-1",
        internal_id="int-abc",
    )

    insert_sqls = [s for s in executed_sqls if "inout_ops_identity_map" in s]
    assert insert_sqls, "Expected INSERT into inout_ops_identity_map"
    assert "external_id" in insert_sqls[0]
    assert "internal_id" in insert_sqls[0]


@pytest.mark.anyio
async def test_record_identity_map_swallows_undefined_table_error():
    """_record_identity_map should silently ignore UndefinedTable errors."""
    import psycopg
    from inandout.writeback.engine import WritebackEngine

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(
        side_effect=psycopg.errors.UndefinedTable("table not found")
    )

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = WritebackEngine(pool=mock_pool)
    # Should not raise
    await engine._record_identity_map(
        connector="myconn",
        datatype="contacts",
        external_id="ext-2",
        internal_id="int-xyz",
    )
