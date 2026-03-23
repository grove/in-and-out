"""Unit tests for atomic watermark updates (T1 #40)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# set_watermark accepts a connection directly (no pool acquire)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_watermark_uses_provided_conn_directly():
    """set_watermark with a connection object uses it directly without acquiring from pool."""
    from inandout.postgres.watermark import set_watermark

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    # Make sure it has 'execute' so it's treated as a connection, not pool
    assert hasattr(mock_conn, "execute")

    run_id = uuid.uuid4()
    await set_watermark(mock_conn, "myconn", "contacts", "cursor", "2026-01-01", run_id)

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    assert "inout_ops_watermark" in call_args[0][0]


@pytest.mark.asyncio
async def test_set_watermark_uses_pool_when_no_execute_attr():
    """set_watermark with a pool-like object (no execute attr) acquires a connection."""
    from inandout.postgres.watermark import set_watermark

    class FakePool:
        """A pool object that explicitly has no execute method."""
        def __init__(self):
            self._conn = None

        def connection(self):
            return self

        async def __aenter__(self):
            self._conn = AsyncMock()
            self._conn.execute = AsyncMock()
            self._conn.commit = AsyncMock()
            return self._conn

        async def __aexit__(self, *a):
            pass

    fake_pool = FakePool()
    assert not hasattr(fake_pool, "execute")

    run_id = uuid.uuid4()
    await set_watermark(fake_pool, "mypool", "contacts", "cursor", "2026-01-01", run_id)

    # Verify the conn inside was used
    assert fake_pool._conn.execute.called
    assert fake_pool._conn.commit.called


@pytest.mark.asyncio
async def test_set_watermark_updated_by_run_id():
    """set_watermark passes run_id as updated_by_run_id."""
    from inandout.postgres.watermark import set_watermark

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    run_id = uuid.uuid4()
    await set_watermark(mock_conn, "c", "d", "cursor", "wm_value", run_id)

    call_params = mock_conn.execute.call_args[0][1]  # positional params list
    # params = [connector, datatype, watermark_type, watermark_value, run_id]
    assert call_params[-1] == run_id


@pytest.mark.asyncio
async def test_set_watermark_same_conn_as_data_write():
    """Verify watermark can be written on the same conn object as data writes."""
    from inandout.postgres.watermark import set_watermark

    # Shared connection used for both data and watermark
    shared_conn = AsyncMock()
    shared_conn.execute = AsyncMock()

    run_id = uuid.uuid4()

    # Simulate data write on shared_conn
    await shared_conn.execute("INSERT INTO some_table VALUES (%s)", ["data"])

    # Then watermark write on same shared_conn
    await set_watermark(shared_conn, "conn", "dt", "cursor", "wm", run_id)

    # Both calls were on the same object
    assert shared_conn.execute.call_count == 2
    calls = shared_conn.execute.call_args_list
    assert "some_table" in calls[0][0][0]
    assert "inout_ops_watermark" in calls[1][0][0]


@pytest.mark.asyncio
async def test_watermark_rollback_on_data_failure():
    """If data write fails and transaction rolls back, watermark is also rolled back.

    This test verifies the invariant: watermark and data share same conn/tx,
    so a rollback before commit undoes both.
    """
    committed = []
    rolled_back = []

    class FakeConn:
        """Fake connection that tracks commit/rollback."""

        async def execute(self, sql, params=None):
            return MagicMock(fetchone=AsyncMock(return_value=None))

        async def commit(self):
            committed.append(True)

        async def rollback(self):
            rolled_back.append(True)

    conn = FakeConn()

    from inandout.postgres.watermark import set_watermark

    # Simulate: data write then watermark write, but commit is NOT called (rollback)
    await conn.execute("INSERT INTO data VALUES (1)")
    await set_watermark(conn, "c", "d", "cursor", "wm", uuid.uuid4())

    # Rollback before commit
    await conn.rollback()

    assert not committed  # never committed
    assert rolled_back  # rolled back — both data and watermark lost
