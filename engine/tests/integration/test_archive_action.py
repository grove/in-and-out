"""Integration tests for the archive action type (T2 #20)."""
from __future__ import annotations

import os

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
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


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available",
)

_CONNECTOR = "arc_test"
_DATATYPE = "deals"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
_BASE_URL = "https://api.arc-test.example.com"
_SRC_TABLE = f"inout_src_{_CONNECTOR}_{_DATATYPE}"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_ARC_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_ARC_TEST_KEY", None)


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="ArcTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="arc_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={_DATATYPE: DatatypeConfig(writeback=_make_writeback_cfg_with_archive())},
    )


def _make_writeback_cfg_with_archive() -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update", "delete", "archive"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/deals/${external_id}"),
            insert=OperationConfig(method="POST", path="/v1/deals"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/deals/${external_id}"),
            delete=OperationConfig(method="DELETE", path="/v1/deals/${external_id}"),
            archive=OperationConfig(method="POST", path="/v1/deals/${external_id}/archive"),
        ),
    )


def _make_writeback_cfg_no_archive() -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update", "delete"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/deals/${external_id}"),
            insert=OperationConfig(method="POST", path="/v1/deals"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/deals/${external_id}"),
            delete=OperationConfig(method="DELETE", path="/v1/deals/${external_id}"),
        ),
    )


async def _setup_delta_table(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_DELTA_TABLE} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update',
                _cluster_id TEXT
            )
        """)
        await conn.commit()


async def _setup_source_table(pool) -> None:
    """Create source table with _deleted column for superseded-guard tests."""
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_SRC_TABLE} (
                external_id TEXT PRIMARY KEY,
                name        TEXT,
                _deleted    BOOLEAN DEFAULT FALSE,
                _ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.commit()


@pytest.mark.anyio
async def test_archive_action_calls_archive_endpoint(pool):
    """Delta row with _action='archive' triggers POST to the archive endpoint."""
    await _setup_delta_table(pool)
    await _setup_source_table(pool)

    # Insert source record that is NOT deleted (archive is valid)
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_SRC_TABLE} (external_id, name, _deleted) VALUES (%s, %s, %s)",
            ["deal-001", "Deal One", False],
        )
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["deal-001", "Deal One", "archive"],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg_with_archive()
    engine = WritebackEngine(pool)

    with respx.mock(base_url=_BASE_URL) as mock:
        archive_route = mock.post("/v1/deals/deal-001/archive").mock(
            return_value=httpx.Response(200, json={"status": "archived"})
        )
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    assert archive_route.called
    assert result.processed >= 1
    assert result.skipped == 0


@pytest.mark.anyio
async def test_archive_skipped_when_source_record_deleted(pool):
    """Archive action is skipped when source record has _deleted=TRUE (superseded guard)."""
    await _setup_delta_table(pool)
    await _setup_source_table(pool)

    # Insert source record that IS deleted (archive should be skipped)
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_SRC_TABLE} (external_id, name, _deleted) VALUES (%s, %s, %s)",
            ["deal-del-001", "Deleted Deal", True],
        )
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["deal-del-001", "Deleted Deal", "archive"],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg_with_archive()
    engine = WritebackEngine(pool)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        archive_route = mock.post("/v1/deals/deal-del-001/archive").mock(
            return_value=httpx.Response(200)
        )
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    # Archive must NOT have been called — the record was superseded/deleted
    assert not archive_route.called
    assert result.skipped >= 1
    assert result.processed == 0


@pytest.mark.anyio
async def test_archive_skipped_when_no_archive_operation_configured(pool):
    """Archive row is skipped (result.skipped += 1) when operations.archive is None."""
    await _setup_delta_table(pool)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["deal-noarchive-001", "Some Deal", "archive"],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = _make_writeback_cfg_no_archive()
    engine = WritebackEngine(pool)

    with respx.mock(base_url=_BASE_URL):
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    assert result.processed == 0
    assert result.skipped >= 1
    assert result.failed == 0
