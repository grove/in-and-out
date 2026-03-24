"""Engine-level unit tests for T2 #21 -- group atomicity.

Source-inspection tests verify the engine has group-abort logic. Functional
tests verify that failing group members cause remaining group members to be
aborted (not dispatched).
"""
from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.writeback.engine import WritebackEngine, WritebackResult


def test_engine_has_aborted_groups_tracking() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "_aborted_groups" in source


def test_engine_logs_writeback_group_member_aborted() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "writeback_group_member_aborted" in source


def test_engine_reads_group_id_from_row() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "_group_id" in source


def test_engine_moves_aborted_member_to_dead_letter() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "move_to_dead_letter" in source
    assert "group_partial_failure" in source


def test_engine_tracks_failed_entries_for_aborted_members() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "_failed_entries" in source
    assert "group_partial_failure" in source


def test_engine_skips_dispatch_for_aborted_group_members() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "continue" in source
    assert "gid and gid in _aborted_groups" in source


def test_engine_marks_group_aborted_when_member_dispatch_fails() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "_failed_before" in source
    assert "result.failed > _failed_before" in source


def test_engine_abort_uses_group_id_as_key() -> None:
    source = inspect.getsource(WritebackEngine._run_writeback_cycle_inner)
    assert "_aborted_groups[gid]" in source


def _make_minimal_wb_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.protection_level = 0
    cfg.conflict_resolution = "last_writer_wins"
    cfg.supported_actions = ["update"]
    cfg.diff_fields = False
    cfg.use_desired_state_table = False
    cfg.batch_size = 50
    cfg.batch_max_bytes = None
    cfg.max_concurrent_writes = 10
    cfg.enable_crash_recovery = False
    cfg.write_dependencies = []
    cfg.max_deletes_per_batch = None
    cfg.crdt_type = None
    cfg.required_fields = []
    cfg.idempotency_key_field = None
    cfg.idempotency_key_header = None
    cfg.etag_header = "ETag"
    cfg.if_match_header = "If-Match"
    cfg.auto_dead_letter_max_retries = 3
    cfg.writeback_result_enabled = True
    cfg.delta_table_prefix = "inout_delta"
    return cfg


async def _run_cycle(engine, connector, wb_cfg, rows, mock_fn, result):
    mock_transport = AsyncMock()
    mock_transport.__aenter__ = AsyncMock(return_value=mock_transport)
    mock_transport.__aexit__ = AsyncMock(return_value=None)
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(
        return_value=AsyncMock(fetchone=AsyncMock(return_value=(True,)))
    )
    mock_conn.commit = AsyncMock()
    engine._pool.connection = MagicMock(return_value=mock_conn)
    with patch.object(engine, "_dispatch_row", new=mock_fn):
        with patch.object(engine, "_fetch_delta_rows", new=AsyncMock(return_value=rows)):
            with patch.object(engine, "_write_feedback", new=AsyncMock()):
                with patch.object(engine, "_auto_dead_letter_exceeded_rows", new=AsyncMock()):
                    with patch.object(engine, "_update_desired_state_statuses", new=AsyncMock()):
                        with patch(
                            "inandout.writeback.engine.HttpTransportAdapter",
                            return_value=mock_transport,
                        ):
                            with patch(
                                "inandout.deadletter.writeback.move_to_dead_letter",
                                new=AsyncMock(),
                            ):
                                with patch(
                                    "inandout.writeback.engine.WritebackResult",
                                    return_value=result,
                                ):
                                    span = MagicMock()
                                    try:
                                        await engine._run_writeback_cycle_inner(
                                            connector, "contacts", wb_cfg, "_delta", span
                                        )
                                    except Exception:
                                        pass


@pytest.mark.anyio
async def test_group_member_aborted_when_first_member_fails() -> None:
    dispatched_ids: list[str] = []
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    engine._namespace = "public"
    wb_cfg = _make_minimal_wb_cfg()
    connector = MagicMock()
    connector.name = "grp_test"
    connector.circuit_breaker = {}
    rows = [
        {"external_id": "a", "_action": "update", "_group_id": "g1", "name": "A"},
        {"external_id": "b", "_action": "update", "_group_id": "g1", "name": "B"},
        {"external_id": "c", "_action": "update", "name": "C"},
    ]
    result = WritebackResult(connector="grp_test", datatype="contacts", delta_table="_delta")

    async def mock_fn(transport, conn, cfg, action, eid, row, log, res) -> None:
        dispatched_ids.append(eid)
        if eid == "a":
            res.failed += 1
            res._failed_entries.append((eid, action, "http_500"))
        else:
            res.processed += 1

    await _run_cycle(engine, connector, wb_cfg, rows, mock_fn, result)
    assert "a" in dispatched_ids, f"Expected a dispatched; got {dispatched_ids}"
    assert "b" not in dispatched_ids, f"Expected b skipped by group abort; got {dispatched_ids}"
    assert result.failed >= 2, f"Expected >=2 failures; got {result.failed}"
    assert "c" in dispatched_ids, f"Expected c dispatched; got {dispatched_ids}"


@pytest.mark.anyio
async def test_singleton_failure_does_not_abort_unrelated_singletons() -> None:
    dispatched_ids: list[str] = []
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    engine._namespace = "public"
    wb_cfg = _make_minimal_wb_cfg()
    connector = MagicMock()
    connector.name = "singleton_test"
    connector.circuit_breaker = {}
    rows = [
        {"external_id": "x", "_action": "update", "name": "X"},
        {"external_id": "y", "_action": "update", "name": "Y"},
        {"external_id": "z", "_action": "update", "name": "Z"},
    ]
    result = WritebackResult(
        connector="singleton_test", datatype="orders", delta_table="_delta"
    )

    async def mock_fn(transport, conn, cfg, action, eid, row, log, res) -> None:
        dispatched_ids.append(eid)
        if eid == "x":
            res.failed += 1
            res._failed_entries.append((eid, action, "http_503"))
        else:
            res.processed += 1

    await _run_cycle(engine, connector, wb_cfg, rows, mock_fn, result)
    for eid in ("x", "y", "z"):
        assert eid in dispatched_ids, f"Expected {eid} dispatched independently; got {dispatched_ids}"
