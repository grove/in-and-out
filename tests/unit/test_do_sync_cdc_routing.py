"""Unit tests for _do_sync CDC branch routing.

When ingestion_cfg.source_mode == "cdc" and ingestion_cfg.cdc is not None,
_do_sync must call _run_cdc_sync instead of the polling/HTTP path.
Verified by patching both branches and asserting only CDC is invoked.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cdc_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "cdc"
    cfg.cdc = MagicMock()  # non-None → CDC branch active
    cfg.history_mode = "none"
    cfg.list = MagicMock()
    cfg.list.incremental = None
    return cfg


def _make_polling_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.history_mode = "none"
    cfg.list = MagicMock()
    cfg.list.incremental = None
    return cfg


def _make_connector(name: str = "testconn") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    cfg.api_version = "v1"
    return cfg


def _make_pool() -> MagicMock:
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None), rowcount=0))
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# CDC branch is taken when source_mode == "cdc"
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cdc_mode_calls_run_cdc_sync():
    """_do_sync must call _run_cdc_sync when source_mode='cdc'."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    run_cdc_sync_called = []
    ensure_table_called = []

    async def _fake_cdc_sync(connector, datatype, ingestion_cfg, result, log, cdc_source, **kw):
        run_cdc_sync_called.append(True)
        result.status = "completed"

    async def _fake_ensure_source_table(*args, **kwargs):
        ensure_table_called.append(True)

    result = SyncResult(uuid.uuid4(), "testconn", "contacts", "full")
    log = MagicMock()

    with (
        patch.object(engine, "_run_cdc_sync", side_effect=_fake_cdc_sync),
        patch("inandout.ingestion.cdc.get_cdc_source", return_value=MagicMock()),
        patch("inandout.ingestion.engine.ensure_source_table",
              side_effect=_fake_ensure_source_table),
    ):
        await engine._do_sync(
            _make_connector(), "contacts",
            _make_cdc_ingestion_cfg(), result, None, log,
        )

    assert run_cdc_sync_called, "_run_cdc_sync must be called in CDC mode"
    assert not ensure_table_called, (
        "ensure_source_table (polling path) must NOT be called in CDC mode"
    )


# ---------------------------------------------------------------------------
# CDC branch NOT taken when source_mode == "polling"
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_polling_mode_does_not_call_run_cdc_sync():
    """_do_sync must NOT call _run_cdc_sync when source_mode='polling'."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    cdc_called = []

    async def _fake_cdc_sync(*args, **kwargs):
        cdc_called.append(True)

    result = SyncResult(uuid.uuid4(), "testconn", "contacts", "full")
    log = MagicMock()

    with (
        patch.object(engine, "_run_cdc_sync", side_effect=_fake_cdc_sync),
        patch("inandout.ingestion.engine.ensure_source_table", new_callable=AsyncMock),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new_callable=AsyncMock),
        # Stub the HTTP transport so the rest of _do_sync can run safely
        patch("inandout.ingestion.engine.HttpTransportAdapter") as mock_transport_cls,
    ):
        mock_transport_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_transport_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        try:
            await engine._do_sync(
                _make_connector(), "contacts",
                _make_polling_ingestion_cfg(), result, None, log,
            )
        except Exception:
            pass  # polling path may fail due to transport stubs — that's fine

    assert not cdc_called, "_run_cdc_sync must NOT be called in polling mode"


# ---------------------------------------------------------------------------
# CDC branch NOT taken when cdc config is None (even if source_mode == "cdc")
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_cdc_mode_requires_non_none_cdc_config():
    """If source_mode='cdc' but ingestion_cfg.cdc is None, CDC branch is skipped."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    cfg = _make_cdc_ingestion_cfg()
    cfg.cdc = None  # override to None

    engine = IngestionEngine(_make_pool())
    engine._read_pool = _make_pool()

    cdc_called = []

    async def _fake_cdc_sync(*args, **kwargs):
        cdc_called.append(True)

    result = SyncResult(uuid.uuid4(), "testconn", "contacts", "full")
    log = MagicMock()

    with (
        patch.object(engine, "_run_cdc_sync", side_effect=_fake_cdc_sync),
        patch("inandout.ingestion.engine.ensure_source_table", new_callable=AsyncMock),
        patch("inandout.ingestion.engine.ensure_dead_letter_table", new_callable=AsyncMock),
        patch("inandout.ingestion.engine.HttpTransportAdapter") as mock_transport_cls,
    ):
        mock_transport_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_transport_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        try:
            await engine._do_sync(
                _make_connector(), "contacts", cfg, result, None, log,
            )
        except Exception:
            pass

    assert not cdc_called, "_run_cdc_sync must not be called when cdc config is None"
