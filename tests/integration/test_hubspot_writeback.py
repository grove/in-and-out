"""Integration test: HubSpot writeback via simulator against real PostgreSQL."""
from __future__ import annotations

import os

import pytest

from inandout.simulators import GenericSimulator, make_hubspot_connector_config, make_hubspot_sim_config
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig, UpdateOperationConfig,
)
from inandout.writeback.engine import WritebackEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")


def _make_hubspot_writeback_cfg() -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.optimistic,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/crm/v3/objects/contacts/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/crm/v3/objects/contacts/${external_id}"),
        ),
    )


async def _create_hubspot_delta_table(pool, table_name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                external_id TEXT,
                firstname   TEXT,
                email       TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.commit()


@pytest.mark.anyio
async def test_hubspot_writeback_dispatches_patch(pool, run_migrations):
    """Writeback engine sends PATCH requests to the HubSpot simulator."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy_token"

    delta_table = "_delta_hubspot_contacts_wb_test"
    await _create_hubspot_delta_table(pool, delta_table)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, firstname, email, _action) VALUES (%s, %s, %s, 'update')",
            ["1", "Alice Updated", "alice.new@example.com"],
        )
        await conn.commit()

    connector = make_hubspot_connector_config()
    writeback_cfg = _make_hubspot_writeback_cfg()

    with GenericSimulator(connector, make_hubspot_sim_config()):
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0


@pytest.mark.anyio
async def test_hubspot_writeback_not_found_counts_as_failure(pool, run_migrations):
    """PATCH to a non-existent contact results in a failed count."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy_token"

    delta_table = "_delta_hubspot_contacts_notfound"
    await _create_hubspot_delta_table(pool, delta_table)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, firstname, _action) VALUES (%s, %s, 'update')",
            ["999", "Ghost Contact"],
        )
        await conn.commit()

    connector = make_hubspot_connector_config()
    writeback_cfg = _make_hubspot_writeback_cfg()

    with GenericSimulator(connector, make_hubspot_sim_config()):
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.failed == 1
    assert result.processed == 0


@pytest.mark.anyio
async def test_hubspot_writeback_feedback_written(pool, run_migrations):
    """Feedback rows are written to inout_ops_writeback_result after successful dispatch."""
    os.environ["INOUT_CREDENTIAL_HUBSPOT_OAUTH"] = "dummy_token"

    delta_table = "_delta_hubspot_contacts_feedback_wb"
    await _create_hubspot_delta_table(pool, delta_table)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {delta_table} (external_id, firstname, _action) VALUES (%s, %s, 'update')",
            ["2", "Bob Updated"],
        )
        await conn.commit()

    connector = make_hubspot_connector_config()
    writeback_cfg = _make_hubspot_writeback_cfg()

    with GenericSimulator(connector, make_hubspot_sim_config()):
        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, "contacts", writeback_cfg, delta_table)

    assert result.processed == 1

    async with pool.connection() as conn:
        row = await (await conn.execute(
            """SELECT connector, action, external_id, status
               FROM inout_ops_writeback_result
               WHERE connector = 'hubspot' AND external_id = '2'
               ORDER BY processed_at DESC LIMIT 1"""
        )).fetchone()

    assert row is not None
    assert row[0] == "hubspot"
    assert row[1] == "update"
    assert row[2] == "2"
    assert row[3] == "ok"
