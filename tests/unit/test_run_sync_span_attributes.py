"""Unit tests for run_sync OTel span attribute assertions.

Verifies that run_sync sets the expected OpenTelemetry span attributes on
both the start path (connector, datatype, mode) and the finish path
(records.inserted, records.updated).

Approach: replace the module-level _tracer with a MagicMock that records
all set_attribute calls, then assert on the captured attribute map.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import inandout.ingestion.engine as _engine_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(name: str = "hubspot") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    cfg.api_version = "v1"
    return cfg


def _make_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.history_mode = "none"
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.schedule = MagicMock()
    cfg.schedule.cron = None
    cfg.schedule.interval = None
    return cfg


def _make_pool() -> MagicMock:
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(
        return_value=AsyncMock(
            fetchone=AsyncMock(return_value=("lock-row-id",)),
            rowcount=1,
        )
    )
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# Span attribute tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_run_sync_sets_connector_and_datatype_span_attributes():
    """span.set_attribute must be called with 'connector' and 'datatype'."""
    from inandout.ingestion.engine import IngestionEngine

    attrs: dict[str, object] = {}
    mock_span = MagicMock()
    mock_span.set_attribute = lambda k, v: attrs.__setitem__(k, v)
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=False)

    mock_tracer = MagicMock()
    mock_tracer.start_as_current_span = MagicMock(return_value=mock_span)

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    with (
        patch.object(_engine_mod, "_tracer", mock_tracer),
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", new=AsyncMock()),
    ):
        await engine.run_sync(_make_connector("salesforce"), "deals", _make_ingestion_cfg())

    assert attrs.get("connector") == "salesforce"
    assert attrs.get("datatype") == "deals"


@pytest.mark.anyio
async def test_run_sync_sets_mode_span_attribute_full():
    """When no watermark exists, span 'mode' attribute must be 'full'."""
    from inandout.ingestion.engine import IngestionEngine

    attrs: dict[str, object] = {}
    mock_span = MagicMock()
    mock_span.set_attribute = lambda k, v: attrs.__setitem__(k, v)
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=False)

    mock_tracer = MagicMock()
    mock_tracer.start_as_current_span = MagicMock(return_value=mock_span)

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    with (
        patch.object(_engine_mod, "_tracer", mock_tracer),
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", new=AsyncMock()),
    ):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert attrs.get("mode") == "full"


@pytest.mark.anyio
async def test_run_sync_sets_records_inserted_and_updated_span_attributes():
    """span must receive 'records.inserted' and 'records.updated' after sync."""
    from inandout.ingestion.engine import IngestionEngine

    attrs: dict[str, object] = {}
    mock_span = MagicMock()
    mock_span.set_attribute = lambda k, v: attrs.__setitem__(k, v)
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=False)

    mock_tracer = MagicMock()
    mock_tracer.start_as_current_span = MagicMock(return_value=mock_span)

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    async def _fake_do_sync(connector, datatype, ingestion_cfg, result, wm, log, **kw):
        result.records_inserted = 42
        result.records_updated = 7

    with (
        patch.object(_engine_mod, "_tracer", mock_tracer),
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", side_effect=_fake_do_sync),
    ):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert attrs.get("records.inserted") == 42
    assert attrs.get("records.updated") == 7


@pytest.mark.anyio
async def test_run_sync_span_name_is_ingestion_run_sync():
    """start_as_current_span must be called with 'ingestion.run_sync'."""
    from inandout.ingestion.engine import IngestionEngine

    mock_span = MagicMock()
    mock_span.set_attribute = MagicMock()
    mock_span.__enter__ = MagicMock(return_value=mock_span)
    mock_span.__exit__ = MagicMock(return_value=False)

    mock_tracer = MagicMock()
    mock_tracer.start_as_current_span = MagicMock(return_value=mock_span)

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    with (
        patch.object(_engine_mod, "_tracer", mock_tracer),
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", new=AsyncMock()),
    ):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    mock_tracer.start_as_current_span.assert_called_once_with("ingestion.run_sync")
