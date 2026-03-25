"""Integration tests for write batch dependency ordering (T2 #26)."""
from __future__ import annotations

import os

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig, UpdateOperationConfig, WriteDependency,
)
from inandout.writeback.engine import WritebackEngine
from inandout.writeback.ordering import detect_dependency_cycle, topological_sort_rows


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

_CONNECTOR = "dep_ord_test"
_DATATYPE = "records"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
_BASE_URL = "https://api.dep-ord-test.example.com"


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_DEP_ORD_TEST_KEY"] = "dummy"
    yield
    os.environ.pop("INOUT_CREDENTIAL_DEP_ORD_TEST_KEY", None)


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DepOrdSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="dep_ord_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/records/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/records/${external_id}"),
                    ),
                ),
            ),
        },
    )


async def _setup_delta_table(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {_DELTA_TABLE}")
        await conn.execute(f"""
            CREATE TABLE {_DELTA_TABLE} (
                external_id  TEXT,
                name         TEXT,
                parent_id    TEXT,
                _action      TEXT NOT NULL DEFAULT 'update',
                _cluster_id  TEXT,
                _group_id    TEXT
            )
        """)
        await conn.commit()


# ---------------------------------------------------------------------------
# Unit-level tests for the ordering module (no DB needed beyond schema)
# ---------------------------------------------------------------------------

def test_topological_sort_orders_parents_before_children():
    """Parents must appear before their children after topological sort."""
    deps = [WriteDependency(parent_datatype="account", join_field="parent_id")]
    rows = [
        {"external_id": "child-1", "parent_id": "parent-1", "_group_id": "grp1"},
        {"external_id": "parent-1", "parent_id": None, "_group_id": "grp1"},
    ]
    sorted_rows = topological_sort_rows(rows, deps)
    ids = [r["external_id"] for r in sorted_rows if not r.get("_cycle_error")]
    assert ids.index("parent-1") < ids.index("child-1")


def test_cycle_detection_returns_true_for_cycle():
    """detect_dependency_cycle returns True when rows form a circular reference."""
    deps = [WriteDependency(parent_datatype="records", join_field="parent_id")]
    rows = [
        {"external_id": "A", "parent_id": "B", "_group_id": "grp-cycle"},
        {"external_id": "B", "parent_id": "A", "_group_id": "grp-cycle"},
    ]
    assert detect_dependency_cycle(rows, deps) is True


def test_cycle_detection_returns_false_for_no_cycle():
    """detect_dependency_cycle returns False when there is no circular reference."""
    deps = [WriteDependency(parent_datatype="records", join_field="parent_id")]
    rows = [
        {"external_id": "A", "parent_id": None},
        {"external_id": "B", "parent_id": "A"},
        {"external_id": "C", "parent_id": "B"},
    ]
    assert detect_dependency_cycle(rows, deps) is False


def test_topological_sort_marks_cycle_rows():
    """Rows that form a cycle get _cycle_error=True set by topological_sort_rows."""
    deps = [WriteDependency(parent_datatype="records", join_field="parent_id")]
    rows = [
        {"external_id": "X", "parent_id": "Y", "_group_id": "grp-err"},
        {"external_id": "Y", "parent_id": "X", "_group_id": "grp-err"},
    ]
    result = topological_sort_rows(rows, deps)
    cycle_rows = [r for r in result if r.get("_cycle_error")]
    assert len(cycle_rows) == 2


# ---------------------------------------------------------------------------
# Integration test: cycle rows counted as failed by the engine
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cycle_rows_counted_as_failed_by_engine(pool):
    """Engine counts cycle-errored rows as result.failed and does not send HTTP calls."""
    await _setup_delta_table(pool)

    # Two rows that reference each other in the same group → cycle
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, parent_id, _action, _group_id) VALUES (%s, %s, %s, %s, %s)",
            ["cy-A", "Node A", "cy-B", "update", "cycle-grp"],
        )
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, parent_id, _action, _group_id) VALUES (%s, %s, %s, %s, %s)",
            ["cy-B", "Node B", "cy-A", "update", "cycle-grp"],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        write_dependencies=[WriteDependency(parent_datatype=_DATATYPE, join_field="parent_id")],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/records/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/records/${external_id}"),
        ),
    )
    engine = WritebackEngine(pool)

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        update_route = mock.patch(url__regex=r"/v1/records/cy-[AB]").mock(
            return_value=httpx.Response(200)
        )
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    # Cycle rows should be dead-lettered as failed; no HTTP calls
    assert result.failed >= 2
    assert not update_route.called


@pytest.mark.anyio
async def test_no_dependency_rows_sent_in_insertion_order(pool):
    """When write_dependencies is empty, rows are processed in the order they appear."""
    await _setup_delta_table(pool)

    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["no-dep-1", "First", "update"],
        )
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, %s)",
            ["no-dep-2", "Second", "update"],
        )
        await conn.commit()

    connector = _make_connector()
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        # No write_dependencies
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/records/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/records/${external_id}"),
        ),
    )
    engine = WritebackEngine(pool)

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.patch(url__regex=r"/v1/records/no-dep-\d").mock(
            return_value=httpx.Response(200)
        )
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    assert result.processed == 2
    assert result.failed == 0
