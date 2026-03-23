"""Unit tests for CDC pipeline end-to-end (A4)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ingestion_cfg(primary_key: str = "id") -> MagicMock:
    cfg = MagicMock()
    cfg.primary_key = primary_key
    cfg.primary_key_expression = None
    cfg.history_mode = "overwrite"
    cfg.source_mode = "cdc"
    cfg.cdc = MagicMock()
    cfg.list = MagicMock()
    cfg.list.incremental = None
    return cfg


def _make_connector() -> MagicMock:
    connector = MagicMock()
    connector.name = "test_cdc_connector"
    connector.auth = MagicMock()
    return connector


def _make_dtype_cfg(field_mappings=None, quality_rules=None) -> MagicMock:
    cfg = MagicMock()
    cfg.field_mappings = field_mappings or []
    cfg.strict_field_mapping = False
    cfg.quality_rules = quality_rules
    cfg.timestamp_fields = []
    cfg.shared_table = None
    return cfg


def _make_pool() -> tuple[MagicMock, MagicMock]:
    """Return (pool, inner_conn) with proper async mocks."""
    inner_conn = MagicMock()
    inner_conn.commit = AsyncMock()
    inner_conn.execute = AsyncMock(return_value=MagicMock())

    txn_ctx = MagicMock()
    txn_ctx.__aenter__ = AsyncMock(return_value=None)
    txn_ctx.__aexit__ = AsyncMock(return_value=None)
    inner_conn.transaction.return_value = txn_ctx

    conn_ctx = MagicMock()
    conn_ctx.__aenter__ = AsyncMock(return_value=inner_conn)
    conn_ctx.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.connection.return_value = conn_ctx

    return pool, inner_conn


class _FakeCdcSource:
    def __init__(self, records: list[dict]) -> None:
        self._records = records
        self.committed = False

    async def consume(self, batch_size: int = 100, timeout_secs: float = 5.0) -> list[dict]:
        return self._records

    async def commit(self) -> None:
        self.committed = True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cdc_record_goes_through_field_mapping_before_upsert():
    """CDC records are field-mapped before being upserted."""
    from inandout.config.field_mapping import FieldMapping
    from inandout.ingestion.engine import IngestionEngine, SyncResult

    pool, inner_conn = _make_pool()

    mapping = FieldMapping(source="old_name", target="new_name")
    dtype_cfg = _make_dtype_cfg(field_mappings=[mapping])
    ingestion_cfg = _make_ingestion_cfg()
    connector = _make_connector()

    cdc_source = _FakeCdcSource([{"old_name": "value", "id": "r-1"}])
    result = SyncResult(uuid.uuid4(), connector.name, "items", "cdc")
    engine = IngestionEngine(pool)

    with (
        patch("inandout.ingestion.engine.ensure_source_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_source_history_table", new=AsyncMock()),
        patch(
            "inandout.ingestion.engine.apply_field_mappings",
            return_value={"new_name": "value", "id": "r-1"},
        ) as mock_fm,
        patch("inandout.ingestion.engine.apply_hooks", new=AsyncMock(side_effect=lambda r, *a, **kw: r)),
        patch("inandout.ingestion.engine.validate_record", return_value=[]),
        patch("inandout.ingestion.engine._upsert_record", new=AsyncMock(return_value=(1, 0, 0))),
        patch("inandout.ingestion.engine.set_watermark", new=AsyncMock()),
    ):
        await engine._run_cdc_sync(
            connector, "items", ingestion_cfg, result, MagicMock(), cdc_source, dtype_cfg=dtype_cfg
        )

    mock_fm.assert_called_once()


@pytest.mark.anyio
async def test_cdc_record_quality_violation_goes_to_dead_letter():
    """CDC record failing quality rules is dead-lettered, not upserted."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    from inandout.ingestion.quality import QualityViolation

    pool, inner_conn = _make_pool()

    quality_rules = MagicMock()
    dtype_cfg = _make_dtype_cfg(quality_rules=quality_rules)
    ingestion_cfg = _make_ingestion_cfg()
    connector = _make_connector()

    cdc_source = _FakeCdcSource([{"id": "r-bad"}])
    result = SyncResult(uuid.uuid4(), connector.name, "items", "cdc")
    engine = IngestionEngine(pool)

    upsert_called = []

    async def _fake_upsert(*args, **kw):
        upsert_called.append(args)
        return 1, 0, 0

    dl_written = []

    async def _fake_dl(conn, table, ext_id, record, msg, cls, run_id):
        dl_written.append((ext_id, cls))

    with (
        patch("inandout.ingestion.engine.ensure_source_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_source_history_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.apply_field_mappings", side_effect=lambda r, *a, **kw: r),
        patch("inandout.ingestion.engine.apply_hooks", new=AsyncMock(side_effect=lambda r, *a, **kw: r)),
        patch(
            "inandout.ingestion.engine.validate_record",
            return_value=[QualityViolation(field="id", rule="required", value=None, message="id must be set")],
        ),
        patch("inandout.ingestion.engine._upsert_record", new=AsyncMock(side_effect=_fake_upsert)),
        patch("inandout.ingestion.engine._write_dead_letter", new=AsyncMock(side_effect=_fake_dl)),
        patch("inandout.ingestion.engine.set_watermark", new=AsyncMock()),
    ):
        await engine._run_cdc_sync(
            connector, "items", ingestion_cfg, result, MagicMock(), cdc_source, dtype_cfg=dtype_cfg
        )

    assert result.records_errored == 1
    assert len(upsert_called) == 0, "quality-violated record must NOT be upserted"
    assert len(dl_written) == 1
    assert dl_written[0][1] == "quality_violation"


@pytest.mark.anyio
async def test_cdc_delete_event_writes_tombstone_not_upsert():
    """CDC record with _cdc_op=DELETE → tombstone written, not upserted."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult

    pool, inner_conn = _make_pool()

    ingestion_cfg = _make_ingestion_cfg()
    connector = _make_connector()
    dtype_cfg = _make_dtype_cfg()

    cdc_source = _FakeCdcSource([{"id": "del-1", "_cdc_op": "DELETE"}])
    result = SyncResult(uuid.uuid4(), connector.name, "items", "cdc")
    engine = IngestionEngine(pool)

    upsert_called = []

    async def _fake_upsert(*args, **kw):
        upsert_called.append(args)
        return 1, 0, 0

    with (
        patch("inandout.ingestion.engine.ensure_source_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_source_history_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.apply_field_mappings", side_effect=lambda r, *a, **kw: r),
        patch("inandout.ingestion.engine.apply_hooks", new=AsyncMock(side_effect=lambda r, *a, **kw: r)),
        patch("inandout.ingestion.engine.validate_record", return_value=[]),
        patch("inandout.ingestion.engine._upsert_record", new=AsyncMock(side_effect=_fake_upsert)),
        patch("inandout.ingestion.engine.set_watermark", new=AsyncMock()),
    ):
        await engine._run_cdc_sync(
            connector, "items", ingestion_cfg, result, MagicMock(), cdc_source, dtype_cfg=dtype_cfg
        )

    assert len(upsert_called) == 0, "DELETE CDC event must NOT be upserted"
    # Tombstone is written via inner_conn.execute (UPDATE ... SET _deleted_at)
    assert inner_conn.execute.called


@pytest.mark.anyio
async def test_cdc_plugin_hooks_applied():
    """Plugin hooks are applied to CDC records."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult

    pool, inner_conn = _make_pool()

    ingestion_cfg = _make_ingestion_cfg()
    connector = _make_connector()
    dtype_cfg = _make_dtype_cfg()

    records = [{"id": "r-1", "name": "original"}]
    cdc_source = _FakeCdcSource(records)
    result = SyncResult(uuid.uuid4(), connector.name, "items", "cdc")
    engine = IngestionEngine(pool)

    hooks_applied_to: list[dict] = []

    async def _hook(record, *args, **kw):
        hooks_applied_to.append(record)
        return {**record, "name": "hooked"}

    with (
        patch("inandout.ingestion.engine.ensure_source_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_source_history_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.apply_field_mappings", side_effect=lambda r, *a, **kw: r),
        patch("inandout.ingestion.engine.apply_hooks", new=AsyncMock(side_effect=_hook)),
        patch("inandout.ingestion.engine.validate_record", return_value=[]),
        patch("inandout.ingestion.engine._upsert_record", new=AsyncMock(return_value=(1, 0, 0))),
        patch("inandout.ingestion.engine.set_watermark", new=AsyncMock()),
    ):
        await engine._run_cdc_sync(
            connector, "items", ingestion_cfg, result, MagicMock(), cdc_source, dtype_cfg=dtype_cfg
        )

    assert len(hooks_applied_to) == 1
    assert hooks_applied_to[0]["id"] == "r-1"


@pytest.mark.anyio
async def test_cdc_watermark_updated_after_batch():
    """Watermark is updated with CDC sequence after a successful batch."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult

    pool, inner_conn = _make_pool()

    ingestion_cfg = _make_ingestion_cfg()
    connector = _make_connector()
    dtype_cfg = _make_dtype_cfg()

    records = [{"id": "r-1", "_cdc_seq": "seq-100"}]
    cdc_source = _FakeCdcSource(records)
    result = SyncResult(uuid.uuid4(), connector.name, "items", "cdc")
    engine = IngestionEngine(pool)

    watermark_updates: list = []

    async def _mock_set_wm(conn, connector, datatype, wm_type, value, run_id):
        watermark_updates.append((connector, datatype, value))

    with (
        patch("inandout.ingestion.engine.ensure_source_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.ensure_source_history_table", new=AsyncMock()),
        patch("inandout.ingestion.engine.apply_field_mappings", side_effect=lambda r, *a, **kw: r),
        patch("inandout.ingestion.engine.apply_hooks", new=AsyncMock(side_effect=lambda r, *a, **kw: r)),
        patch("inandout.ingestion.engine.validate_record", return_value=[]),
        patch("inandout.ingestion.engine._upsert_record", new=AsyncMock(return_value=(1, 0, 0))),
        patch("inandout.ingestion.engine.set_watermark", new=AsyncMock(side_effect=_mock_set_wm)),
    ):
        await engine._run_cdc_sync(
            connector, "items", ingestion_cfg, result, MagicMock(), cdc_source, dtype_cfg=dtype_cfg
        )

    assert len(watermark_updates) == 1
    assert watermark_updates[0][2] == "seq-100"
