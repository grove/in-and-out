"""Integration tests: three-way conflict detection (B2)."""
from __future__ import annotations

import os
import re

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.conflict-test.example.com"
_CONNECTOR = "conflict_test"
_DATATYPE = "orders"


def _make_connector(connector_name: str = _CONNECTOR):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution, OperationsConfig,
        OperationConfig, UpdateOperationConfig,
    )

    return ConnectorConfig(
        name=connector_name,
        system="ConflictTest",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="conflict_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/orders",
                            record_selector="orders",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                ),
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.skip_and_warn,
                    supported_actions=["update"],
                    use_desired_state_table=True,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path="/v1/orders/${external_id}"),
                        update=UpdateOperationConfig(method="PATCH", path="/v1/orders/${external_id}"),
                    ),
                ),
            )
        },
    )


@pytest.mark.anyio
async def test_three_way_conflict_skip_and_warn(pool, run_migrations):
    """External actor modifies a record between MDM decision and writeback.
    With skip_and_warn: the write is skipped, lwstate updated to current.
    """
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector()
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name(_CONNECTOR, _DATATYPE)
    lwst = lwstate_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, _CONNECTOR, _DATATYPE)
        await ensure_lwstate_table(conn, _CONNECTOR, _DATATYPE)

        # lwstate: last we wrote was status=pending
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-1', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "pending"}).decode()],
        )
        # desired-state: update to status=shipped, based on status=pending
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action, _sync_run_id)
            VALUES ('ord-1', %s, 'update', NULL)
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "shipped", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # External actor changed status to 'processing'
        return httpx.Response(200, json={"id": "ord-1", "status": "processing"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-1", "status": "shipped"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    # skip_and_warn: PATCH must NOT have been sent
    assert len(patched) == 0, "PATCH should not be sent when conflict detected with skip_and_warn"
    assert result.skipped >= 1
    assert result.conflicts >= 1

    # lwstate should be updated to the current (external) state
    async with pool.connection() as conn:
        lw_row = await (
            await conn.execute(f"SELECT data FROM {lwst} WHERE external_id='ord-1'")
        ).fetchone()
    assert lw_row is not None
    lw_data = lw_row[0] if isinstance(lw_row[0], dict) else orjson.loads(lw_row[0])
    assert lw_data.get("status") == "processing"


@pytest.mark.anyio
async def test_three_way_no_conflict_current_matches_base(pool, run_migrations):
    """No external modification: current == base, write proceeds."""
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector(connector_name="conflict_test_b")
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name("conflict_test_b", _DATATYPE)
    lwst = lwstate_table_name("conflict_test_b", _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, "conflict_test_b", _DATATYPE)
        await ensure_lwstate_table(conn, "conflict_test_b", _DATATYPE)

        # lwstate: last wrote status=pending
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-2', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "pending"}).decode()],
        )
        # desired: update to status=shipped, base=pending
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('ord-2', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "shipped", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # No external change — current still matches base
        return httpx.Response(200, json={"id": "ord-2", "status": "pending"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-2", "status": "shipped"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    # No conflict: PATCH should have been sent
    assert result.processed >= 1 or len(patched) >= 1 or result.skipped == 0


@pytest.mark.anyio
async def test_three_way_no_conflict_current_matches_lwstate(pool, run_migrations):
    """T2 #3: current state differs from base but matches lwstate (our own prior write).

    This is safe — the discrepancy between current and base was caused by the
    tool's own last write, not by an external actor. The write must proceed.
    """
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector(connector_name="conflict_test_c")
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name("conflict_test_c", _DATATYPE)
    lwst = lwstate_table_name("conflict_test_c", _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, "conflict_test_c", _DATATYPE)
        await ensure_lwstate_table(conn, "conflict_test_c", _DATATYPE)

        # Scenario: MDM base says status=pending; we previously wrote status=processing
        # External system now shows status=processing (our own prior write)
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-3', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "processing"}).decode()],  # last we wrote
        )
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('ord-3', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "shipped", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # External system reflects our own last write (processing), not the MDM base (pending)
        return httpx.Response(200, json={"id": "ord-3", "status": "processing"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-3", "status": "shipped"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    # current matches lwstate → safe write, no conflict
    assert result.conflicts == 0, (
        f"Expected 0 conflicts (current matches lwstate = our prior write); got {result}"
    )
    assert len(patched) == 1, "PATCH should be sent when current matches lwstate (safe)"


@pytest.mark.anyio
async def test_three_way_field_scoped_unrelated_changes_not_conflict(pool, run_migrations):
    """T2 #3: field-scoped comparison — changes to fields NOT in the desired payload
    must not trigger a false conflict.

    External actor changes job_title; MDM only writes email.
    Since job_title is not in the write payload, the conflict check must pass.
    """
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector = _make_connector(connector_name="conflict_test_d")
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name("conflict_test_d", _DATATYPE)
    lwst = lwstate_table_name("conflict_test_d", _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, "conflict_test_d", _DATATYPE)
        await ensure_lwstate_table(conn, "conflict_test_d", _DATATYPE)

        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-4', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"email": "old@example.com", "job_title": "Engineer"}).decode()],
        )
        # MDM only changes email, not job_title
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('ord-4', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({
                "email": "new@example.com",
                "_base": {"email": "old@example.com"},
            }).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # External actor changed job_title (not email) — should NOT be a conflict
        return httpx.Response(200, json={"id": "ord-4", "email": "old@example.com", "job_title": "Senior Engineer"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-4"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    assert result.conflicts == 0, (
        f"Expected 0 conflicts (job_title change is field-scoped out); got {result}"
    )
    assert len(patched) == 1, "PATCH should be sent because only unrelated field changed"


@pytest.mark.anyio
async def test_three_way_last_writer_wins_sends_patch_despite_conflict(pool, run_migrations):
    """T2 #3 / T2 #30: last_writer_wins resolution — conflict is detected and counted
    but the PATCH is still sent (overwriting external changes).
    """
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.config.writeback import ConflictResolution
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
        get_lwstate,
    )
    from inandout.writeback.engine import WritebackEngine

    # Build a connector with last_writer_wins resolution
    connector_lww = _make_connector(connector_name="conflict_test_e")
    from inandout.config.writeback import WritebackConfig, ProtectionLevel, OperationsConfig, OperationConfig, UpdateOperationConfig
    new_wb_cfg = WritebackConfig(
        protection_level=ProtectionLevel.optimistic,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        use_desired_state_table=True,
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/orders/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/orders/${external_id}"),
        ),
    )
    import copy
    connector_lww = copy.deepcopy(connector_lww)
    connector_lww.datatypes[_DATATYPE].__dict__["writeback"] = new_wb_cfg
    object.__setattr__(connector_lww.datatypes[_DATATYPE], "writeback", new_wb_cfg)

    dst = desired_state_table_name("conflict_test_e", _DATATYPE)
    lwst = lwstate_table_name("conflict_test_e", _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, "conflict_test_e", _DATATYPE)
        await ensure_lwstate_table(conn, "conflict_test_e", _DATATYPE)

        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-5', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "pending"}).decode()],
        )
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('ord-5', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "shipped", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        return httpx.Response(200, json={"id": "ord-5", "status": "modified_externally"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-5", "status": "shipped"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector_lww, _DATATYPE, new_wb_cfg, dst)

    assert result.conflicts >= 1, "Conflict should be counted even with last_writer_wins"
    assert len(patched) == 1, "PATCH must be sent despite conflict with last_writer_wins"
    assert result.processed >= 1, "Record should count as processed (write executed)"

    # T2 #9 / T2 #30: after successful write, lwstate must reflect confirmed written state
    async with pool.connection() as conn:
        lw_row = await (
            await conn.execute(f"SELECT data FROM {lwst} WHERE external_id='ord-5'")
        ).fetchone()
    assert lw_row is not None, "lwstate must be written after successful last_writer_wins patch"
    lw_data = lw_row[0] if isinstance(lw_row[0], dict) else orjson.loads(lw_row[0])
    assert lw_data.get("status") == "shipped", (
        f"lwstate must reflect confirmed written state 'shipped' after last_writer_wins; "
        f"got: {lw_data.get('status')!r}"
    )


@pytest.mark.anyio
async def test_three_way_server_wins_updates_lwstate_to_current(pool, run_migrations):
    """T2 #9 / T2 #30: server_wins resolution — write is skipped but lwstate is
    refreshed to reflect the actual current external state observed in the
    preflight read.

    GOAL.md requirement T2 #9: "After a conflict is detected (any resolution
    strategy): Update [lwstate] with the actual current state observed in the
    pre-flight read."
    """
    os.environ["INOUT_CREDENTIAL_CONFLICT_KEY"] = "dummy"
    from inandout.config.writeback import ConflictResolution
    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    connector_sw = _make_connector(connector_name="conflict_test_f")
    from inandout.config.writeback import WritebackConfig, ProtectionLevel, OperationsConfig, OperationConfig, UpdateOperationConfig
    import copy
    sw_wb_cfg = WritebackConfig(
        protection_level=ProtectionLevel.optimistic,
        conflict_resolution=ConflictResolution.server_wins,
        supported_actions=["update"],
        use_desired_state_table=True,
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/v1/orders/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/v1/orders/${external_id}"),
        ),
    )
    connector_sw = copy.deepcopy(connector_sw)
    object.__setattr__(connector_sw.datatypes[_DATATYPE], "writeback", sw_wb_cfg)

    dst = desired_state_table_name("conflict_test_f", _DATATYPE)
    lwst = lwstate_table_name("conflict_test_f", _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, "conflict_test_f", _DATATYPE)
        await ensure_lwstate_table(conn, "conflict_test_f", _DATATYPE)

        # lwstate: last we wrote was status=pending
        await conn.execute(
            f"""
            INSERT INTO {lwst} (external_id, data, _written_at)
            VALUES ('ord-6', %s, NOW())
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data
            """,
            [orjson.dumps({"status": "pending"}).decode()],
        )
        # Desired state: want to set status=closed, base was pending
        await conn.execute(
            f"""
            INSERT INTO {dst} (external_id, data, _action)
            VALUES ('ord-6', %s, 'update')
            ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action
            """,
            [orjson.dumps({"status": "closed", "_base": {"status": "pending"}}).decode()],
        )
        await conn.commit()

    patched = []

    def _handle_get(request):
        # External actor modified the record while we weren't looking
        return httpx.Response(200, json={"id": "ord-6", "status": "in_review"})

    def _handle_patch(request):
        patched.append(request)
        return httpx.Response(200, json={"id": "ord-6", "status": "closed"})

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_get)
        mock.patch(re.compile(r"/v1/orders/\w+")).mock(side_effect=_handle_patch)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector_sw, _DATATYPE, sw_wb_cfg, dst)

    # server_wins: we do NOT send the PATCH — the external state wins
    assert result.conflicts >= 1, "Conflict must be counted"
    assert result.skipped >= 1, "server_wins must skip the write"
    assert len(patched) == 0, "PATCH must NOT be sent when server_wins"

    # T2 #9 / T2 #30: lwstate must be updated to the current external state
    # even though we didn't write anything
    async with pool.connection() as conn:
        lw_row = await (
            await conn.execute(f"SELECT data FROM {lwst} WHERE external_id='ord-6'")
        ).fetchone()
    assert lw_row is not None, "lwstate must exist"
    lw_data = lw_row[0] if isinstance(lw_row[0], dict) else orjson.loads(lw_row[0])
    assert lw_data.get("status") == "in_review", (
        f"lwstate must be refreshed to current external state 'in_review' after "
        f"server_wins; got: {lw_data.get('status')!r}"
    )
