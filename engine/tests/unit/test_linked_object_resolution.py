"""Unit tests for T1 #16 — linked/nested object resolution in IngestionEngine."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_linked_obj(field, datatype, detail_path, concurrency=3, primary_key="id"):
    from inandout.config.connector import LinkedObject
    return LinkedObject(
        field=field,
        datatype=datatype,
        detail_path=detail_path,
        concurrency=concurrency,
        primary_key=primary_key,
    )


# ---------------------------------------------------------------------------
# Config field tests
# ---------------------------------------------------------------------------

def test_linked_object_primary_key_default():
    """LinkedObject.primary_key defaults to 'id'."""
    obj = _make_linked_obj("item_ids", "items", "/items/${id}")
    assert obj.primary_key == "id"


def test_linked_object_primary_key_custom():
    """LinkedObject.primary_key can be set to a custom field."""
    obj = _make_linked_obj("order_ids", "orders", "/orders/${id}", primary_key="order_id")
    assert obj.primary_key == "order_id"


# ---------------------------------------------------------------------------
# _resolve_linked_objects behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_linked_objects_fans_out_get_requests():
    """_resolve_linked_objects fires GET requests for each unique child ID."""
    from inandout.ingestion.engine import IngestionEngine

    pool = MagicMock()
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._pool = pool
    engine._namespace = "public"

    linked_obj = _make_linked_obj(
        field="line_item_ids",
        datatype="line_items",
        detail_path="/line-items/${id}",
    )

    parent_records = [
        {"id": "order-1", "line_item_ids": ["li-1", "li-2"]},
        {"id": "order-2", "line_item_ids": ["li-2", "li-3"]},  # li-2 is duplicate
    ]

    # Mock transport to return child records
    transport = AsyncMock()
    def _raw_resp(child_id: str):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": child_id, "name": f"Item {child_id}"}
        return mock_resp

    call_args: list[str] = []

    async def _mock_raw_request(method, path, **kwargs):
        child_id = path.split("/")[-1]
        call_args.append(child_id)
        return _raw_resp(child_id)

    transport._raw_request = _mock_raw_request

    # Mock pool.connection
    conn_mock = AsyncMock()
    conn_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    conn_mock.commit = AsyncMock()
    conn_mock.transaction = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=None)))

    pool.connection = MagicMock(return_value=conn_mock)

    result = MagicMock()
    result.run_id = "test-run"
    log = MagicMock()

    with (
        patch("inandout.ingestion.engine.source_table_name", return_value="inout_src_crm_line_items"),
        patch("inandout.ingestion.engine.ensure_source_table", AsyncMock()),
        patch("inandout.ingestion.engine._upsert_record", AsyncMock(return_value=(1, 0, 0))),
        patch("inandout.ingestion.engine._compute_raw_hash", return_value="hash123"),
    ):
        connector = MagicMock()
        connector.name = "crm"

        await engine._resolve_linked_objects(
            transport=transport,
            connector=connector,
            namespace="public",
            linked_objects=[linked_obj],
            parent_records=parent_records,
            result=result,
            log=log,
        )

    # 3 unique IDs: li-1, li-2, li-3
    assert set(call_args) == {"li-1", "li-2", "li-3"}
    assert len(call_args) == 3  # no duplicates


@pytest.mark.asyncio
async def test_resolve_linked_objects_skips_empty_field():
    """_resolve_linked_objects skips parents where the field is absent or empty."""
    from inandout.ingestion.engine import IngestionEngine

    pool = MagicMock()
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._pool = pool
    engine._namespace = "public"

    linked_obj = _make_linked_obj("tag_ids", "tags", "/tags/${id}")
    parent_records = [
        {"id": "rec-1"},                    # field missing
        {"id": "rec-2", "tag_ids": []},     # empty list
        {"id": "rec-3", "tag_ids": None},   # null
    ]

    transport = AsyncMock()
    transport._raw_request = AsyncMock()

    conn_mock = AsyncMock()
    conn_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    conn_mock.commit = AsyncMock()
    pool.connection = MagicMock(return_value=conn_mock)

    result = MagicMock()
    result.run_id = "run-2"
    log = MagicMock()

    with (
        patch("inandout.ingestion.engine.source_table_name", return_value="inout_src_crm_tags"),
        patch("inandout.ingestion.engine.ensure_source_table", AsyncMock()),
    ):
        connector = MagicMock()
        connector.name = "crm"

        await engine._resolve_linked_objects(
            transport=transport,
            connector=connector,
            namespace="public",
            linked_objects=[linked_obj],
            parent_records=parent_records,
            result=result,
            log=log,
        )

    # No GET requests should be fired because no child IDs
    transport._raw_request.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_linked_objects_handles_fetch_failure():
    """_resolve_linked_objects continues when a child GET fails (logs warning)."""
    from inandout.ingestion.engine import IngestionEngine

    pool = MagicMock()
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._pool = pool
    engine._namespace = "public"

    linked_obj = _make_linked_obj("item_ids", "items", "/items/${id}")
    parent_records = [{"id": "parent-1", "item_ids": ["item-fail"]}]

    async def _failing_request(method, path, **kwargs):
        raise Exception("Connection timeout")

    transport = AsyncMock()
    transport._raw_request = _failing_request

    conn_mock = AsyncMock()
    conn_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    conn_mock.commit = AsyncMock()
    pool.connection = MagicMock(return_value=conn_mock)

    result = MagicMock()
    result.run_id = "run-3"
    log = MagicMock()

    with (
        patch("inandout.ingestion.engine.source_table_name", return_value="inout_src_crm_items"),
        patch("inandout.ingestion.engine.ensure_source_table", AsyncMock()),
    ):
        connector = MagicMock()
        connector.name = "crm"

        # Should not raise even when GET fails
        await engine._resolve_linked_objects(
            transport=transport,
            connector=connector,
            namespace="public",
            linked_objects=[linked_obj],
            parent_records=parent_records,
            result=result,
            log=log,
        )

    log.warning.assert_called()


@pytest.mark.asyncio
async def test_resolve_linked_objects_uses_custom_primary_key():
    """_resolve_linked_objects uses the configured primary_key to extract child external_id."""
    from inandout.ingestion.engine import IngestionEngine

    pool = MagicMock()
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._pool = pool
    engine._namespace = "public"

    linked_obj = _make_linked_obj(
        "order_refs",
        "orders",
        "/orders/${id}",
        primary_key="order_id",
    )
    parent_records = [{"id": "cust-1", "order_refs": ["ord-999"]}]

    upserted_ids: list[str] = []

    async def _mock_upsert(conn, table, ext_id, record, raw_hash, run_id, **kwargs):
        upserted_ids.append(ext_id)
        return (1, 0, 0)

    async def _mock_raw_request(method, path, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"order_id": "ord-999", "total": 100}
        return resp

    transport = AsyncMock()
    transport._raw_request = _mock_raw_request

    conn_mock = AsyncMock()
    conn_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    conn_mock.commit = AsyncMock()
    conn_mock.transaction = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=None)))
    pool.connection = MagicMock(return_value=conn_mock)

    result = MagicMock()
    result.run_id = "run-4"
    log = MagicMock()

    with (
        patch("inandout.ingestion.engine.source_table_name", return_value="inout_src_crm_orders"),
        patch("inandout.ingestion.engine.ensure_source_table", AsyncMock()),
        patch("inandout.ingestion.engine._upsert_record", _mock_upsert),
        patch("inandout.ingestion.engine._compute_raw_hash", return_value="h"),
    ):
        connector = MagicMock()
        connector.name = "crm"

        await engine._resolve_linked_objects(
            transport=transport,
            connector=connector,
            namespace="public",
            linked_objects=[linked_obj],
            parent_records=parent_records,
            result=result,
            log=log,
        )

    assert "ord-999" in upserted_ids


@pytest.mark.asyncio
async def test_resolve_linked_objects_scalar_id_field():
    """_resolve_linked_objects handles scalar (non-list) child ID field."""
    from inandout.ingestion.engine import IngestionEngine

    pool = MagicMock()
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._pool = pool
    engine._namespace = "public"

    # field holds a single ID (not a list)
    linked_obj = _make_linked_obj("account_id", "accounts", "/accounts/${id}")
    parent_records = [{"id": "contact-1", "account_id": "acc-42"}]

    called_paths: list[str] = []

    async def _mock_raw_request(method, path, **kwargs):
        called_paths.append(path)
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"id": "acc-42", "name": "Acme"}
        return resp

    transport = AsyncMock()
    transport._raw_request = _mock_raw_request

    conn_mock = AsyncMock()
    conn_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    conn_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    conn_mock.commit = AsyncMock()
    conn_mock.transaction = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=None)))
    pool.connection = MagicMock(return_value=conn_mock)

    result = MagicMock()
    result.run_id = "run-5"
    log = MagicMock()

    with (
        patch("inandout.ingestion.engine.source_table_name", return_value="inout_src_crm_accounts"),
        patch("inandout.ingestion.engine.ensure_source_table", AsyncMock()),
        patch("inandout.ingestion.engine._upsert_record", AsyncMock(return_value=(1, 0, 0))),
        patch("inandout.ingestion.engine._compute_raw_hash", return_value="h"),
    ):
        connector = MagicMock()
        connector.name = "crm"

        await engine._resolve_linked_objects(
            transport=transport,
            connector=connector,
            namespace="public",
            linked_objects=[linked_obj],
            parent_records=parent_records,
            result=result,
            log=log,
        )

    assert "/accounts/acc-42" in called_paths
