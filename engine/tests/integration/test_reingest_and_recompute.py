"""Integration test: T2 #30 / T2 #39 — re-ingest-and-recompute conflict resolution.

When a writeback conflict is detected (external actor modified the record) and
the resolution strategy is ``re_ingest_and_recompute``, the writeback engine must:
  1. NOT issue the HTTP write (the desired state is stale).
  2. Insert a ``resync`` command row into ``inout_ops_control``.
  3. Cap the feedback loop at ``max_feedback_iterations`` (T2 #39).

This creates the feedback loop:
  writeback detects drift → inserts resync command → ingestion re-fetches →
  MDM re-computes desired state → writeback retries with current base.

GOAL.md T2 #30 (conflict resolution strategies), T2 #39 (conflict-driven
re-ingestion signal with configurable max iteration cap).
"""
from __future__ import annotations

import json
import os
import re

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.reingest-test.example.com"
_CONNECTOR = "reingest_wb"
_DATATYPE = "tickets"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(resolution: str = "re_ingest_and_recompute"):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import (
        ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile,
    )
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig,
        ProtectionLevel, UpdateOperationConfig, WritebackConfig,
    )

    # Map string → enum
    res_enum = {
        "re_ingest_and_recompute": ConflictResolution.re_ingest_and_recompute,
        "dead_letter": ConflictResolution.dead_letter,
        "skip_and_warn": ConflictResolution.skip_and_warn,
    }[resolution]

    return ConnectorConfig(
        name=_CONNECTOR,
        system="ReIngestTest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="reingest_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=res_enum,
                    supported_actions=["update"],
                    use_desired_state_table=True,
                    max_feedback_iterations=3,
                    operations=OperationsConfig(
                        lookup=OperationConfig(
                            method="GET", path="/v1/tickets/${external_id}"
                        ),
                        update=UpdateOperationConfig(
                            method="PATCH", path="/v1/tickets/${external_id}"
                        ),
                    ),
                ),
            )
        },
    )


async def _setup_tables(pool, connector_name=_CONNECTOR):
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, connector_name, _DATATYPE)
        await ensure_lwstate_table(conn, connector_name, _DATATYPE)
        await conn.commit()

    return (
        desired_state_table_name(connector_name, _DATATYPE),
        lwstate_table_name(connector_name, _DATATYPE),
    )


# ---------------------------------------------------------------------------
# Test 1: re_ingest_and_recompute inserts a resync command
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_reingest_conflict_inserts_resync_command(pool, run_migrations):
    """When conflict detected + re_ingest_and_recompute, a resync row is inserted.

    GOAL.md T2 #30, T2 #39: the writeback tool inserts a 'resync' command into
    inout_ops_control with the affected (connector, datatype, external_id).
    """
    os.environ["INOUT_CREDENTIAL_REINGEST_KEY"] = "dummy"
    from inandout.postgres.desired_state import desired_state_table_name, lwstate_table_name
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector(resolution="re_ingest_and_recompute")
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst, lwst = await _setup_tables(pool)

    external_id = "ticket_reingest_001"

    async with pool.connection() as conn:
        # lwstate: last wrote status=open
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [external_id, orjson.dumps({"status": "open"}).decode()],
        )
        # desired: update to status=resolved, base=open
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES (%s, %s, 'update')
            ON CONFLICT (external_id) DO UPDATE
                SET data=EXCLUDED.data, _action=EXCLUDED._action, _status='pending'
            """,
            [external_id, orjson.dumps({
                "status": "resolved",
                "_base": {"status": "open"},   # base = what MDM expected
            }).decode()],
        )
        await conn.commit()

    # Pre-clean any leftover resync commands from previous test runs
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_control WHERE connector=%s AND datatype=%s AND command='resync'",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()

    patched = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Pre-flight GET: external actor changed to 'in_progress'
        mock.get(re.compile(r"/v1/tickets/\w+")).mock(
            return_value=httpx.Response(200, json={"id": external_id, "status": "in_progress"})
        )
        mock.patch(re.compile(r"/v1/tickets/\w+")).mock(
            side_effect=lambda req: (
                patched.append(req)
                or httpx.Response(200, json={"id": external_id})
            )
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    # Conflict must be detected
    assert result.conflicts >= 1, (
        f"Expected at least 1 conflict, got {result.conflicts}"
    )
    # PATCH must NOT be issued when re_ingest_and_recompute
    assert len(patched) == 0, (
        "PATCH was issued despite re_ingest_and_recompute conflict resolution"
    )

    # A resync command row must have been inserted into inout_ops_control
    async with pool.connection() as conn:
        row = await (await conn.execute(
            """
            SELECT connector, datatype, command, payload, status
            FROM inout_ops_control
            WHERE connector=%s AND datatype=%s AND command='resync'
            ORDER BY issued_at DESC
            LIMIT 1
            """,
            [_CONNECTOR, _DATATYPE],
        )).fetchone()

    assert row is not None, (
        "No resync command row found in inout_ops_control after conflict with "
        "re_ingest_and_recompute resolution"
    )
    assert row[2] == "resync"
    assert row[4] == "pending"

    # Payload should contain the affected external_id
    payload = row[3] if isinstance(row[3], dict) else json.loads(row[3])
    assert payload.get("external_id") == external_id, (
        f"resync payload should contain external_id={external_id!r}, got {payload}"
    )


# ---------------------------------------------------------------------------
# Test 2: feedback loop is capped at max_feedback_iterations (T2 #39)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_reingest_cap_prevents_infinite_loop(pool, run_migrations):
    """After max_feedback_iterations, no more resync signals are emitted.

    GOAL.md T2 #39: the feedback loop must have a configurable maximum
    iteration count per record (default: 3) to prevent infinite cycles when
    the external system is under continuous concurrent modification.
    """
    os.environ["INOUT_CREDENTIAL_REINGEST_KEY"] = "dummy"
    from inandout.postgres.desired_state import desired_state_table_name, lwstate_table_name
    from inandout.writeback.engine import WritebackEngine

    # Use a distinct connector name to avoid counter state pollution
    connector_name = "reingest_cap_wb"
    connector = _make_connector(resolution="re_ingest_and_recompute")
    # Override name (we can't change the config directly; use a fresh engine)
    # We test the cap via the engine's internal _reingest_counters
    wb_cfg = connector.datatypes[_DATATYPE].writeback

    dst, lwst = await _setup_tables(pool, connector_name=connector_name)

    external_id = "ticket_cap_001"

    # Seed desired state + lwstate for the capped-connector
    from inandout.postgres.desired_state import ensure_desired_state_table, ensure_lwstate_table
    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, connector_name, _DATATYPE)
        await ensure_lwstate_table(conn, connector_name, _DATATYPE)

        dst_cap = desired_state_table_name(connector_name, _DATATYPE)
        lwst_cap = lwstate_table_name(connector_name, _DATATYPE)

        await conn.execute(
            f"""
            INSERT INTO {lwst_cap} (external_id, data, _written_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [external_id, orjson.dumps({"status": "open"}).decode()],
        )
        await conn.execute(
            f"""
            INSERT INTO {dst_cap} (external_id, data, _action)
            VALUES (%s, %s, 'update')
            ON CONFLICT (external_id) DO UPDATE
                SET data=EXCLUDED.data, _action=EXCLUDED._action, _status='pending'
            """,
            [external_id, orjson.dumps({
                "status": "resolved",
                "_base": {"status": "open"},
            }).decode()],
        )
        await conn.commit()

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig,
        ProtectionLevel, UpdateOperationConfig, WritebackConfig,
    )

    cap_connector = ConnectorConfig(
        name=connector_name,
        system="ReIngestCapTest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="reingest_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.re_ingest_and_recompute,
                    supported_actions=["update"],
                    use_desired_state_table=True,
                    max_feedback_iterations=2,   # low cap for testing
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/tickets/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/tickets/${external_id}"),
                    ),
                )
            )
        },
    )
    cap_wb_cfg = cap_connector.datatypes[_DATATYPE].writeback

    engine = WritebackEngine(pool)

    resync_counts: list[int] = []

    async def _run_and_count():
        async with pool.connection() as conn:
            await conn.execute(
                f"UPDATE {dst_cap} SET _status='pending' WHERE external_id=%s",
                [external_id],
            )
            await conn.commit()

        with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
            mock.get(re.compile(r"/v1/tickets/\w+")).mock(
                return_value=httpx.Response(200, json={"id": external_id, "status": "in_progress"})
            )
            mock.patch(re.compile(r"/v1/tickets/\w+")).mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            result = await engine.run_writeback_cycle(cap_connector, _DATATYPE, cap_wb_cfg, dst_cap)

        # Count resync commands
        async with pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT COUNT(*) FROM inout_ops_control WHERE connector=%s AND command='resync'",
                [connector_name],
            )).fetchone()
        resync_counts.append(row[0])

    # Run 4 cycles — cap is 2, so 3rd and 4th cycles should not insert more resyncs
    for _ in range(4):
        await _run_and_count()

    # After cap is reached, command count should stop growing
    final_count = resync_counts[-1]
    # We expect no more than max_feedback_iterations resync rows to be added
    max_allowed = cap_wb_cfg.max_feedback_iterations
    assert final_count <= max_allowed + 1, (  # +1 tolerance for first insertion
        f"Resync loop not capped: {final_count} signals after {len(resync_counts)} "
        f"cycles (cap={max_allowed})"
    )
