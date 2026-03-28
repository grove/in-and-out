"""Unit tests for deletion verification (Priority 8 — Phase 2)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# IngestionConfig.verify_deletion field tests
# ---------------------------------------------------------------------------


def test_ingestion_config_has_verify_deletion():
    """IngestionConfig should have verify_deletion defaulting to True."""
    from inandout.config.ingestion import IngestionConfig

    cfg = IngestionConfig(
        primary_key="id",
        history_mode="overwrite",
        schedule={"interval": "5m"},
        list={
            "path": "/contacts",
            "pagination": {"strategy": "offset", "offset": {
                "page_size": 100,
                "offset_param": "offset",
                "limit_param": "limit",
            }},
        },
    )
    assert hasattr(cfg, "verify_deletion")
    assert cfg.verify_deletion is True


def test_ingestion_config_verify_deletion_can_be_disabled():
    """IngestionConfig with verify_deletion=False should be valid."""
    from inandout.config.ingestion import IngestionConfig

    cfg = IngestionConfig(
        primary_key="id",
        history_mode="overwrite",
        schedule={"interval": "5m"},
        list={
            "path": "/contacts",
            "pagination": {"strategy": "offset", "offset": {
                "page_size": 100,
                "offset_param": "offset",
                "limit_param": "limit",
            }},
        },
        verify_deletion=False,
    )
    assert cfg.verify_deletion is False


# ---------------------------------------------------------------------------
# ListConfig.detail_path field tests
# ---------------------------------------------------------------------------


def test_list_config_has_detail_path():
    """ListConfig should have detail_path defaulting to None."""
    from inandout.config.ingestion import ListConfig

    cfg = ListConfig(
        path="/contacts",
        pagination={"strategy": "offset", "offset": {
            "page_size": 100,
            "offset_param": "offset",
            "limit_param": "limit",
        }},
    )
    assert hasattr(cfg, "detail_path")
    assert cfg.detail_path is None


def test_list_config_detail_path_can_be_set():
    """ListConfig.detail_path should accept a path template string."""
    from inandout.config.ingestion import ListConfig

    cfg = ListConfig(
        path="/contacts",
        pagination={"strategy": "offset", "offset": {
            "page_size": 100,
            "offset_param": "offset",
            "limit_param": "limit",
        }},
        detail_path="/contacts/${external_id}",
    )
    assert cfg.detail_path == "/contacts/${external_id}"


# ---------------------------------------------------------------------------
# _tombstone_missing deletion verification tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_tombstone_missing_without_verification_tombstones_all_missing():
    """Without verify_deletion, all missing IDs should be tombstoned."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    tombstoned: list[str] = []

    mock_cursor_count = MagicMock()
    mock_cursor_count.fetchone = AsyncMock(return_value=(2,))

    mock_cursor_existing = MagicMock()
    mock_cursor_existing.fetchall = AsyncMock(return_value=[("id-1",), ("id-2",)])

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    # track_transaction
    mock_txn = MagicMock()
    mock_txn.__aenter__ = AsyncMock(return_value=None)
    mock_txn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_txn)

    call_count = [0]

    async def _execute(sql, params=None):
        call_count[0] += 1
        if "COUNT(*)" in sql:
            return mock_cursor_count
        if "SELECT external_id" in sql:
            return mock_cursor_existing
        if "UPDATE" in sql and "_deleted_at" in sql:
            tombstoned.append(params[0] if params else "?")
            return MagicMock()
        return MagicMock()

    mock_conn.execute = AsyncMock(side_effect=_execute)
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = IngestionEngine(pool=mock_pool)
    result = SyncResult(uuid.uuid4(), "myconn", "contacts", "full")

    # seen_ids = {"id-1"}, so "id-2" is missing
    await engine._tombstone_missing(
        table="inout_src_myconn_contacts",
        seen_ids={"id-1"},
        result=result,
        log=MagicMock(),
        connector_name="myconn",
        datatype="contacts",
        connector=None,
        ingestion_cfg=None,  # no verify_deletion
    )

    assert result.records_deleted == 1
    assert "id-2" in tombstoned


@pytest.mark.anyio
async def test_tombstone_missing_with_verification_calls_detail_path():
    """With verify_deletion=True and detail_path, _tombstone_missing should GET detail_path."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    tombstoned: list[str] = []

    mock_cursor_count = MagicMock()
    mock_cursor_count.fetchone = AsyncMock(return_value=(2,))

    mock_cursor_existing = MagicMock()
    mock_cursor_existing.fetchall = AsyncMock(return_value=[("id-1",), ("id-2",)])

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    mock_txn = MagicMock()
    mock_txn.__aenter__ = AsyncMock(return_value=None)
    mock_txn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_txn)

    async def _execute(sql, params=None):
        if "COUNT(*)" in sql:
            return mock_cursor_count
        if "SELECT external_id" in sql:
            return mock_cursor_existing
        if "UPDATE" in sql and "_deleted_at" in sql:
            tombstoned.append(params[0] if params else "?")
            return MagicMock()
        return MagicMock()

    mock_conn.execute = AsyncMock(side_effect=_execute)
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = IngestionEngine(pool=mock_pool)
    result = SyncResult(uuid.uuid4(), "myconn", "contacts", "full")

    # Build a mock ingestion config with verify_deletion=True
    mock_ingestion_cfg = MagicMock()
    mock_ingestion_cfg.verify_deletion = True
    mock_ingestion_cfg.list = MagicMock()
    mock_ingestion_cfg.list.detail_path = "/contacts/${external_id}"

    # Build a mock connector
    mock_connector = MagicMock()
    mock_connector.name = "myconn"

    # Mock the HttpTransportAdapter to return 404 for id-2
    import httpx

    mock_transport = AsyncMock()
    mock_transport.__aenter__ = AsyncMock(return_value=mock_transport)
    mock_transport.__aexit__ = AsyncMock(return_value=False)
    mock_transport._raw_request = AsyncMock(
        return_value=httpx.Response(404)
    )

    with patch("inandout.ingestion.engine.HttpTransportAdapter", return_value=mock_transport):
        await engine._tombstone_missing(
            table="inout_src_myconn_contacts",
            seen_ids={"id-1"},
            result=result,
            log=MagicMock(),
            connector_name="myconn",
            datatype="contacts",
            connector=mock_connector,
            ingestion_cfg=mock_ingestion_cfg,
        )

    # id-2 got 404 → should be tombstoned
    assert result.records_deleted == 1
    assert "id-2" in tombstoned


@pytest.mark.anyio
async def test_tombstone_missing_verification_skips_200_response():
    """With verify_deletion, a 200 response means record still exists — skip tombstone."""
    from inandout.ingestion.engine import IngestionEngine, SyncResult
    import uuid

    tombstoned: list[str] = []

    mock_cursor_count = MagicMock()
    mock_cursor_count.fetchone = AsyncMock(return_value=(2,))

    mock_cursor_existing = MagicMock()
    mock_cursor_existing.fetchall = AsyncMock(return_value=[("id-1",), ("id-2",)])

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    mock_txn = MagicMock()
    mock_txn.__aenter__ = AsyncMock(return_value=None)
    mock_txn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction = MagicMock(return_value=mock_txn)

    async def _execute(sql, params=None):
        if "COUNT(*)" in sql:
            return mock_cursor_count
        if "SELECT external_id" in sql:
            return mock_cursor_existing
        if "UPDATE" in sql and "_deleted_at" in sql:
            tombstoned.append(params[0] if params else "?")
            return MagicMock()
        return MagicMock()

    mock_conn.execute = AsyncMock(side_effect=_execute)
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    engine = IngestionEngine(pool=mock_pool)
    result = SyncResult(uuid.uuid4(), "myconn", "contacts", "full")

    mock_ingestion_cfg = MagicMock()
    mock_ingestion_cfg.verify_deletion = True
    mock_ingestion_cfg.list = MagicMock()
    mock_ingestion_cfg.list.detail_path = "/contacts/${external_id}"

    mock_connector = MagicMock()
    mock_connector.name = "myconn"

    import httpx

    mock_transport = AsyncMock()
    mock_transport.__aenter__ = AsyncMock(return_value=mock_transport)
    mock_transport.__aexit__ = AsyncMock(return_value=False)
    # Return 200 — record still exists
    mock_transport._raw_request = AsyncMock(
        return_value=httpx.Response(200, json={"id": "id-2", "name": "Bob"})
    )

    with patch("inandout.ingestion.engine.HttpTransportAdapter", return_value=mock_transport):
        await engine._tombstone_missing(
            table="inout_src_myconn_contacts",
            seen_ids={"id-1"},
            result=result,
            log=MagicMock(),
            connector_name="myconn",
            datatype="contacts",
            connector=mock_connector,
            ingestion_cfg=mock_ingestion_cfg,
        )

    # id-2 is still 200 → should NOT be tombstoned
    assert result.records_deleted == 0
    assert "id-2" not in tombstoned
