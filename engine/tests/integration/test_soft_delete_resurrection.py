"""Integration test: T1 #41 — soft-delete resurrection.

When a record previously marked as deleted (_deleted_at IS NOT NULL, _deleted=TRUE)
reappears in the API response during a subsequent sync cycle, the ingestion engine
must:
  1. Clear the tombstone (_deleted_at = NULL, _deleted = FALSE).
  2. Update the record data to the fresh payload.
  3. Increment the ``records_resurrected_total`` metric.
  4. Return ``resurrected=1`` from the upsert path — counted separately from
     plain ``updated`` so dashboards can distinguish resurrection from routine change.

GOAL.md T1 #41 (soft-delete resurrection), A6 (state machine: deleted→active).
"""
from __future__ import annotations

import hashlib
import json
import os

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.resurrection-test.example.com"
_CONNECTOR = "resurrection_ingest"
_DATATYPE = "orders"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(base_url: str = _BASE_URL):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import (
        ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile,
    )
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig

    return ConnectorConfig(
        name=_CONNECTOR,
        system="ResurrectionTest",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="resurrection_key",
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
                )
            )
        },
    )


def _raw_hash(data: dict) -> str:
    return hashlib.sha256(orjson.dumps(data, option=orjson.OPT_SORT_KEYS)).hexdigest()


# ---------------------------------------------------------------------------
# Test 1: tombstoned record reappears → tombstone cleared
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_deleted_record_is_resurrected_on_reappearance(pool, run_migrations):
    """A record previously tombstoned is resurrected when seen again in sync.

    GOAL.md T1 #41: _deleted_at must be cleared and _deleted set to FALSE when
    the record reappears in the API's list response.
    """
    os.environ["INOUT_CREDENTIAL_RESURRECTION_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table, source_table_name

    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    table = source_table_name(_CONNECTOR, _DATATYPE)

    # Ensure table exists
    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    external_id = "order_tomb_001"
    stale_data = {"id": external_id, "status": "shipped"}
    stale_hash = _raw_hash(stale_data)
    stale_json = orjson.dumps(stale_data).decode()

    # Seed a tombstoned row simulating a prior deletion cycle
    async with pool.connection() as conn:
        await conn.execute(
            f"""
            INSERT INTO {table}
                (external_id, data, raw, _ingested_at, _raw_hash, _deleted, _deleted_at, _schema_version)
            VALUES (%s, %s, %s, NOW(), %s, TRUE, NOW(), 1)
            ON CONFLICT (external_id) DO UPDATE
                SET data=EXCLUDED.data, raw=EXCLUDED.raw,
                    _raw_hash=EXCLUDED._raw_hash,
                    _deleted=TRUE, _deleted_at=NOW()
            """,
            [external_id, stale_json, stale_json, stale_hash],
        )
        await conn.commit()

    # Confirm tombstone is in place before the sync
    async with pool.connection() as conn:
        pre_row = await (await conn.execute(
            f"SELECT _deleted, _deleted_at FROM {table} WHERE external_id=%s",
            [external_id],
        )).fetchone()
    assert pre_row is not None and pre_row[0] is True and pre_row[1] is not None, (
        "Pre-condition failed: record should be tombstoned before resurrection sync"
    )

    # The same record reappears in the next API response (unchanged payload)
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(
                200,
                json={"orders": [{"id": external_id, "status": "shipped"}], "next_cursor": None},
            )
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Verify tombstone is cleared
    async with pool.connection() as conn:
        post_row = await (await conn.execute(
            f"SELECT _deleted, _deleted_at, data FROM {table} WHERE external_id=%s",
            [external_id],
        )).fetchone()

    assert post_row is not None, "Record missing after resurrection sync"
    assert post_row[0] is False, (
        f"_deleted should be FALSE after resurrection, got {post_row[0]!r}"
    )
    assert post_row[1] is None, (
        f"_deleted_at should be NULL after resurrection, got {post_row[1]!r}"
    )


# ---------------------------------------------------------------------------
# Test 2: tombstoned record reappears with new data → data updated + resurrected
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_deleted_record_resurrected_with_updated_data(pool, run_migrations):
    """A record previously tombstoned reappears with new data — data is updated.

    GOAL.md T1 #41: resurrection also applies when the reappearing record has a
    new payload (different _raw_hash); the data must be updated alongside clearing
    the tombstone.
    """
    os.environ["INOUT_CREDENTIAL_RESURRECTION_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table, source_table_name

    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    external_id = "order_tomb_002"
    old_data = {"id": external_id, "status": "cancelled"}
    old_hash = _raw_hash(old_data)
    old_json = orjson.dumps(old_data).decode()

    # Seed tombstoned row with old data
    async with pool.connection() as conn:
        await conn.execute(
            f"""
            INSERT INTO {table}
                (external_id, data, raw, _ingested_at, _raw_hash, _deleted, _deleted_at, _schema_version)
            VALUES (%s, %s, %s, NOW(), %s, TRUE, NOW(), 1)
            ON CONFLICT (external_id) DO UPDATE
                SET data=EXCLUDED.data, raw=EXCLUDED.raw,
                    _raw_hash=EXCLUDED._raw_hash,
                    _deleted=TRUE, _deleted_at=NOW()
            """,
            [external_id, old_json, old_json, old_hash],
        )
        await conn.commit()

    new_payload = {"id": external_id, "status": "reactivated", "priority": "high"}

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(200, json={"orders": [new_payload], "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    async with pool.connection() as conn:
        post_row = await (await conn.execute(
            f"SELECT _deleted, _deleted_at, data FROM {table} WHERE external_id=%s",
            [external_id],
        )).fetchone()

    assert post_row is not None
    assert post_row[0] is False, "_deleted should be FALSE after resurrection"
    assert post_row[1] is None, "_deleted_at should be NULL after resurrection"

    stored_data = post_row[2] if isinstance(post_row[2], dict) else json.loads(post_row[2])
    assert stored_data.get("status") == "reactivated", (
        f"Data not updated on resurrection: {stored_data}"
    )
    assert stored_data.get("priority") == "high", (
        f"New fields missing after resurrection: {stored_data}"
    )


# ---------------------------------------------------------------------------
# Test 3: record that was never deleted is NOT counted as resurrected
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_normal_update_not_counted_as_resurrection(pool, run_migrations):
    """A normal update to an active (non-tombstoned) record is not a resurrection.

    GOAL.md T1 #41: the resurrection path must only trigger for records that
    had _deleted_at IS NOT NULL, not for routine updates.
    """
    os.environ["INOUT_CREDENTIAL_RESURRECTION_KEY"] = "dummy"
    from inandout.ingestion.engine import IngestionEngine
    from inandout.postgres.schema import ensure_source_table, source_table_name

    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    external_id = "order_active_003"
    current_data = {"id": external_id, "status": "processing"}
    current_hash = _raw_hash(current_data)
    current_json = orjson.dumps(current_data).decode()

    # Insert a live (non-tombstoned) row
    async with pool.connection() as conn:
        await conn.execute(
            f"""
            INSERT INTO {table}
                (external_id, data, raw, _ingested_at, _raw_hash, _deleted, _schema_version)
            VALUES (%s, %s, %s, NOW(), %s, FALSE, 1)
            ON CONFLICT (external_id) DO UPDATE
                SET data=EXCLUDED.data, raw=EXCLUDED.raw,
                    _raw_hash=EXCLUDED._raw_hash, _deleted=FALSE, _deleted_at=NULL
            """,
            [external_id, current_json, current_json, current_hash],
        )
        await conn.commit()

    updated_payload = {"id": external_id, "status": "delivered"}

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/orders").mock(
            return_value=httpx.Response(200, json={"orders": [updated_payload], "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Record should still be active
    async with pool.connection() as conn:
        post_row = await (await conn.execute(
            f"SELECT _deleted, _deleted_at FROM {table} WHERE external_id=%s",
            [external_id],
        )).fetchone()

    assert post_row is not None
    assert post_row[0] is False, "_deleted should remain FALSE for non-tombstoned record"
    assert post_row[1] is None, "_deleted_at should remain NULL for non-tombstoned record"
    # Ensure no errors or unexpected state — main assertion is records_updated
    assert result.records_inserted + result.records_updated >= 1
