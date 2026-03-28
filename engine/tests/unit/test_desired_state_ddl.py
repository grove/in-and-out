"""Unit tests for REPLICA IDENTITY FULL on desired-state tables (B3)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest


@pytest.mark.asyncio
async def test_ensure_desired_state_table_sets_replica_identity() -> None:
    """ensure_desired_state_table should execute REPLICA IDENTITY FULL."""
    from inandout.postgres.desired_state import ensure_desired_state_table

    conn = AsyncMock()
    conn.execute = AsyncMock()

    await ensure_desired_state_table(conn, "myconn", "contacts", "public")

    # Find the REPLICA IDENTITY FULL call
    calls_str = [str(c) for c in conn.execute.call_args_list]
    replica_calls = [c for c in calls_str if "REPLICA IDENTITY FULL" in c]
    assert len(replica_calls) >= 1, (
        f"Expected REPLICA IDENTITY FULL call, got: {calls_str}"
    )


@pytest.mark.asyncio
async def test_ensure_lwstate_table_sets_replica_identity() -> None:
    """ensure_lwstate_table should also execute REPLICA IDENTITY FULL."""
    from inandout.postgres.desired_state import ensure_lwstate_table

    conn = AsyncMock()
    conn.execute = AsyncMock()

    await ensure_lwstate_table(conn, "myconn", "contacts", "public")

    calls_str = [str(c) for c in conn.execute.call_args_list]
    replica_calls = [c for c in calls_str if "REPLICA IDENTITY FULL" in c]
    assert len(replica_calls) >= 1, (
        f"Expected REPLICA IDENTITY FULL call, got: {calls_str}"
    )
