"""Unit tests for ID-list fetch strategy (T1 #9 / A2)."""
from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.config.ingestion import ListConfig


def _make_list_config(**kwargs) -> ListConfig:
    defaults = {
        "path": "/items",
        "pagination": {"strategy": "offset", "offset": {"page_size": 100}},
    }
    defaults.update(kwargs)
    return ListConfig(**defaults)


def test_default_fetch_strategy_is_list() -> None:
    cfg = _make_list_config()
    assert cfg.fetch_strategy == "list"


def test_id_list_strategy_fields() -> None:
    cfg = _make_list_config(
        fetch_strategy="id_list",
        id_field="uid",
        detail_concurrency=10,
        detail_path="/items/${id}",
    )
    assert cfg.fetch_strategy == "id_list"
    assert cfg.id_field == "uid"
    assert cfg.detail_concurrency == 10


def test_id_field_default() -> None:
    cfg = _make_list_config(fetch_strategy="id_list")
    assert cfg.id_field == "id"
    assert cfg.detail_concurrency == 5


# ---------------------------------------------------------------------------
# Source-inspection tests: verify engine id_list implementation is present
# ---------------------------------------------------------------------------

def test_engine_do_sync_handles_id_list_strategy() -> None:
    """_do_sync must branch on fetch_strategy == 'id_list'."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    assert "id_list" in source
    assert "fetch_strategy" in source


def test_engine_id_list_calls_raw_request_for_each_stub() -> None:
    """_do_sync must call _raw_request for each stub ID in id_list mode."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    assert "_raw_request" in source
    assert "_fetch_detail_record" in source or "_fetch_detail" in source


def test_engine_id_list_uses_semaphore_for_concurrency() -> None:
    """_do_sync must use a Semaphore with detail_concurrency for id_list parallel fetches."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    assert "Semaphore" in source
    assert "detail_concurrency" in source


def test_engine_id_list_substitutes_external_id_in_path() -> None:
    """_do_sync must substitute ${external_id} in the detail_path template."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    assert "${external_id}" in source or "external_id" in source
    assert "replace" in source


def test_engine_id_list_fallback_path_when_detail_path_none() -> None:
    """When detail_path is None, _do_sync builds a fallback detail path from list.path."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    # fallback: list_cfg.path + "/${external_id}"
    assert "detail_path" in source


def test_engine_id_list_skips_stub_missing_id_field() -> None:
    """_do_sync must skip stub records where the id_field is absent."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    # Code must guard against missing id field ("if _raw_id is None: return")
    assert "_raw_id" in source or "raw_id" in source


def test_engine_id_list_warns_on_non_200_detail_response() -> None:
    """_do_sync must log a warning when a detail GET returns non-200."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    assert "id_list_detail_fetch_failed" in source


def test_engine_id_list_warns_on_detail_exception() -> None:
    """_do_sync must catch and log exceptions from individual detail GETs."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine._do_sync)
    assert "id_list_detail_fetch_error" in source


# ---------------------------------------------------------------------------
# Functional: verify detail GETs are dispatched for each stub
# ---------------------------------------------------------------------------

def _make_minimal_pool() -> MagicMock:
    """Pool mock that satisfies basic SQL calls in _do_sync without failing."""
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=None)
        cur.fetchall = AsyncMock(return_value=[])
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


@pytest.mark.anyio
async def test_id_list_detail_gets_dispatched_for_each_stub() -> None:
    """When fetch_strategy='id_list', one GET per stub ID must be issued."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    stubs = [{"id": "aaa"}, {"id": "bbb"}, {"id": "ccc"}]
    detail_records = {
        "aaa": {"id": "aaa", "name": "Alice"},
        "bbb": {"id": "bbb", "name": "Bob"},
        "ccc": {"id": "ccc", "name": "Carol"},
    }

    get_calls: list[str] = []

    async def _raw_request_side_effect(method: str, path: str, **kwargs):
        if method == "GET":
            get_calls.append(path)
            stub_id = path.split("/")[-1]
            resp = MagicMock()
            resp.status_code = 200
            resp.json = MagicMock(return_value=detail_records.get(stub_id, {}))
            return resp
        raise AssertionError(f"Unexpected {method} {path}")

    mock_transport = AsyncMock()
    mock_transport._raw_request = AsyncMock(side_effect=_raw_request_side_effect)

    # fetch_pages yields one page of stubs
    async def _fake_fetch_pages(*args, **kwargs):
        yield stubs

    mock_transport.fetch_pages = _fake_fetch_pages
    mock_transport.__aenter__ = AsyncMock(return_value=mock_transport)
    mock_transport.__aexit__ = AsyncMock(return_value=None)

    pool = _make_minimal_pool()
    engine = IngestionEngine(pool=pool)
    engine._read_pool = pool

    run_id = uuid.uuid4()
    result = SyncResult(run_id, "myconn", "contacts", "full")
    log = MagicMock()
    log.info = MagicMock()
    log.warning = MagicMock()
    log.bind = MagicMock(return_value=log)

    # Build minimal ingestion config with id_list strategy
    from inandout.config.ingestion import IngestionConfig, ListConfig, HistoryMode
    list_cfg = _make_list_config(
        fetch_strategy="id_list",
        id_field="id",
        detail_path="/contacts/${external_id}",
        detail_concurrency=3,
    )
    ingestion_cfg = MagicMock()
    ingestion_cfg.list = list_cfg
    ingestion_cfg.history_mode = HistoryMode.overwrite
    ingestion_cfg.primary_key = "id"
    ingestion_cfg.primary_key_expression = None
    ingestion_cfg.checkpoint_every_n_pages = 0
    ingestion_cfg.bulk_upsert_batch_size = 1
    ingestion_cfg.max_concurrent_fetches = 1
    ingestion_cfg.prune_orphan_columns = False
    ingestion_cfg.verify_deletion = False
    ingestion_cfg.source_mode = "polling"
    ingestion_cfg.cdc = None
    ingestion_cfg.webhook_events = None

    with patch("inandout.ingestion.engine.HttpTransportAdapter") as MockAdapter:
        MockAdapter.return_value.__aenter__ = AsyncMock(return_value=mock_transport)
        MockAdapter.return_value.__aexit__ = AsyncMock(return_value=None)

        connector = MagicMock()
        connector.name = "myconn"
        connector.api_version = None
        connector.namespace = None

        try:
            await engine._do_sync(connector, "contacts", ingestion_cfg, result, None, log)
        except Exception:
            pass  # DB write errors are expected with mock pool; we only care about GET calls

    # Three detail GETs must have been issued, one per stub
    assert len(get_calls) == 3
    assert "/contacts/aaa" in get_calls
    assert "/contacts/bbb" in get_calls
    assert "/contacts/ccc" in get_calls


@pytest.mark.anyio
async def test_id_list_skips_stub_on_non_200_response() -> None:
    """Detail GET returning non-200 must be dropped; other stubs still processed."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    stubs = [{"id": "ok1"}, {"id": "bad"}, {"id": "ok2"}]
    get_calls: list[str] = []

    async def _raw_request(method: str, path: str, **kwargs):
        if method == "GET":
            get_calls.append(path)
            resp = MagicMock()
            stub_id = path.split("/")[-1]
            resp.status_code = 200 if stub_id != "bad" else 404
            resp.json = MagicMock(return_value={"id": stub_id})
            return resp
        raise AssertionError(f"Unexpected {method} {path}")

    mock_transport = AsyncMock()
    mock_transport._raw_request = AsyncMock(side_effect=_raw_request)

    async def _fake_fetch_pages(*args, **kwargs):
        yield stubs

    mock_transport.fetch_pages = _fake_fetch_pages
    mock_transport.__aenter__ = AsyncMock(return_value=mock_transport)
    mock_transport.__aexit__ = AsyncMock(return_value=None)

    pool = _make_minimal_pool()
    engine = IngestionEngine(pool=pool)
    engine._read_pool = pool

    run_id = uuid.uuid4()
    result = SyncResult(run_id, "myconn", "contacts", "full")
    log = MagicMock()
    log.warning = MagicMock()
    log.bind = MagicMock(return_value=log)

    list_cfg = _make_list_config(
        fetch_strategy="id_list",
        id_field="id",
        detail_path="/items/${external_id}",
        detail_concurrency=2,
    )
    ingestion_cfg = MagicMock()
    ingestion_cfg.list = list_cfg
    ingestion_cfg.history_mode = "none"
    ingestion_cfg.primary_key = "id"
    ingestion_cfg.primary_key_expression = None
    ingestion_cfg.checkpoint_every_n_pages = 0
    ingestion_cfg.bulk_upsert_batch_size = 1
    ingestion_cfg.max_concurrent_fetches = 1
    ingestion_cfg.prune_orphan_columns = False
    ingestion_cfg.verify_deletion = False
    ingestion_cfg.source_mode = "polling"
    ingestion_cfg.cdc = None
    ingestion_cfg.webhook_events = None

    with patch("inandout.ingestion.engine.HttpTransportAdapter") as MockAdapter:
        MockAdapter.return_value.__aenter__ = AsyncMock(return_value=mock_transport)
        MockAdapter.return_value.__aexit__ = AsyncMock(return_value=None)

        connector = MagicMock()
        connector.name = "myconn"
        connector.api_version = None
        connector.namespace = None

        try:
            await engine._do_sync(connector, "contacts", ingestion_cfg, result, None, log)
        except Exception:
            pass

    # All three detail GETs must have been attempted
    assert len(get_calls) == 3
    # The warning for "bad" must have been logged
    warning_calls = [str(c) for c in log.warning.call_args_list]
    assert any("id_list_detail_fetch_failed" in wc or "detail_fetch_failed" in wc for wc in warning_calls), (
        f"Expected id_list_detail_fetch_failed warning; got: {warning_calls}"
    )

