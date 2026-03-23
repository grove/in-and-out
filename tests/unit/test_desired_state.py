"""Unit tests for desired-state table contract (Priority 6 — Phase 2)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# DDL / naming tests
# ---------------------------------------------------------------------------


def test_desired_state_table_name():
    """desired_state_table_name should follow inout_dst_{connector}_{datatype} pattern."""
    from inandout.postgres.desired_state import desired_state_table_name

    assert desired_state_table_name("myconn", "contacts") == "inout_dst_myconn_contacts"


def test_lwstate_table_name():
    """lwstate_table_name should follow inout_dst_{connector}_{datatype}_lwstate pattern."""
    from inandout.postgres.desired_state import lwstate_table_name

    assert lwstate_table_name("myconn", "contacts") == "inout_dst_myconn_contacts_lwstate"


def test_desired_state_table_name_with_namespace():
    """desired_state_table_name with namespace should use schema-qualified name."""
    from inandout.postgres.desired_state import desired_state_table_name

    result = desired_state_table_name("myconn", "contacts", namespace="tenant1")
    assert result == "tenant1.inout_dst_myconn_contacts"


def test_desired_state_table_ddl_has_required_columns():
    """desired_state_table_ddl should include all required columns."""
    from inandout.postgres.desired_state import desired_state_table_ddl

    ddl = desired_state_table_ddl("myconn", "contacts")
    for col in ("external_id", "data", "_action", "_schema_version",
                "_created_at", "_updated_at"):
        assert col in ddl, f"Missing column: {col}"
    assert "JSONB" in ddl


def test_desired_state_table_ddl_has_valid_action_check():
    """desired_state_table_ddl should include CHECK constraint on _action."""
    from inandout.postgres.desired_state import desired_state_table_ddl

    ddl = desired_state_table_ddl("myconn", "contacts")
    assert "CHECK" in ddl
    assert "insert" in ddl
    assert "update" in ddl
    assert "delete" in ddl


def test_lwstate_table_ddl_has_required_columns():
    """lwstate_table_ddl should include external_id, data, _written_at."""
    from inandout.postgres.desired_state import lwstate_table_ddl

    ddl = lwstate_table_ddl("myconn", "contacts")
    for col in ("external_id", "data", "_written_at"):
        assert col in ddl, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# WritebackConfig.use_desired_state_table tests
# ---------------------------------------------------------------------------


def test_writeback_config_has_use_desired_state_table():
    """WritebackConfig should have use_desired_state_table defaulting to False."""
    from inandout.config.writeback import WritebackConfig

    cfg = WritebackConfig(
        protection_level=3,
        conflict_resolution="last_writer_wins",
        supported_actions=["insert", "update"],
        operations={
            "lookup": {"method": "GET", "path": "/contacts/${external_id}"},
        },
    )
    assert hasattr(cfg, "use_desired_state_table")
    assert cfg.use_desired_state_table is False


def test_writeback_config_use_desired_state_table_can_be_enabled():
    """WritebackConfig with use_desired_state_table=True should be valid."""
    from inandout.config.writeback import WritebackConfig

    cfg = WritebackConfig(
        protection_level=3,
        conflict_resolution="last_writer_wins",
        supported_actions=["insert", "update"],
        operations={
            "lookup": {"method": "GET", "path": "/contacts/${external_id}"},
        },
        use_desired_state_table=True,
    )
    assert cfg.use_desired_state_table is True


# ---------------------------------------------------------------------------
# upsert_desired_state / get_lwstate helper tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_upsert_desired_state_executes_upsert():
    """upsert_desired_state should execute an INSERT ... ON CONFLICT DO UPDATE."""
    from inandout.postgres.desired_state import upsert_desired_state

    executed_sqls: list[str] = []

    mock_conn = AsyncMock()

    async def _execute(sql, params=None):
        executed_sqls.append(sql)
        return MagicMock()

    mock_conn.execute = AsyncMock(side_effect=_execute)

    await upsert_desired_state(
        conn=mock_conn,
        connector="myconn",
        datatype="contacts",
        external_id="ext-1",
        data={"name": "Alice"},
        action="update",
    )

    assert executed_sqls, "Expected at least one SQL execution"
    assert "inout_dst_myconn_contacts" in executed_sqls[0]
    assert "ON CONFLICT" in executed_sqls[0]


@pytest.mark.anyio
async def test_get_lwstate_returns_none_when_not_found():
    """get_lwstate should return None when no row exists."""
    from inandout.postgres.desired_state import get_lwstate

    mock_cursor = MagicMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    result = await get_lwstate(
        conn=mock_conn,
        connector="myconn",
        datatype="contacts",
        external_id="nonexistent",
    )
    assert result is None


@pytest.mark.anyio
async def test_get_lwstate_returns_dict_when_found():
    """get_lwstate should return the data dict when a row exists."""
    import orjson
    from inandout.postgres.desired_state import get_lwstate

    expected = {"name": "Bob", "email": "bob@example.com"}

    mock_cursor = MagicMock()
    mock_cursor.fetchone = AsyncMock(return_value=(expected,))

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    result = await get_lwstate(
        conn=mock_conn,
        connector="myconn",
        datatype="contacts",
        external_id="ext-1",
    )
    assert result == expected
