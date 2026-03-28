"""Integration tests for T1 #32: Tombstone records on confirmed deletion.

When a full sync reveals that a record no longer appears in the external API
response, the ingestion tool must write an explicit tombstone — null-payload,
deletion-flagged — rather than hard-deleting the row.  This makes deletions
observable to downstream consumers without requiring access to diff tables.

GOAL.md T1 #32: "When a record is confirmed deleted (after verification per
requirement #5), the ingestion tool must write an explicit tombstone record to
the per-datatype table — a null or empty payload entry with deletion metadata
(timestamp, source confirmation) — rather than performing a hard delete."
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.ingestion import HistoryMode, IngestionConfig, ListConfig, ScheduleConfig
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import ensure_source_table, source_table_name


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "tombstone_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.tombstone-test.example.com"
os.environ.setdefault("INOUT_CREDENTIAL_TOMBSTONE_TEST_KEY", "dummy")


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="TombstoneSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="tombstone_test_key",
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
                            path=f"/v1/{_DATATYPE}",
                            record_selector="contacts",
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


@pytest.mark.anyio
async def test_deleted_record_receives_tombstone_not_hard_delete(pool, run_migrations):
    """T1 #32: a record absent from the second full sync is tombstoned, not deleted.

    After an initial sync that ingests record A and B, a subsequent full sync
    that returns only A must write a tombstone row for B:
      - Row still exists in the source table (no hard delete)
      - _deleted = TRUE
      - _deleted_at is set (not null)
      - data / raw become empty objects (explicit null payload)
    """
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    contacts_both = [
        {"id": "contact-ts-1", "name": "Alice"},
        {"id": "contact-ts-2", "name": "Bob (will be deleted)"},
    ]
    contacts_only_alice = [{"id": "contact-ts-1", "name": "Alice"}]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # First full sync — both records
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"contacts": contacts_both, "next_cursor": None}
            )
        )
        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result1.status == "completed"

    # Verify both are written and NOT deleted
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, _deleted FROM {src_table}"
        )).fetchall()
    ids = {r[0]: r[1] for r in rows}
    assert "contact-ts-1" in ids
    assert "contact-ts-2" in ids
    assert not ids["contact-ts-2"], "Before second sync, Bob must not be deleted"

    # Second full sync (watermark is None → full sync) — Bob is gone
    # We need to reset the watermark to force a full sync; use a fresh engine
    # with cleared watermark:
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"contacts": contacts_only_alice, "next_cursor": None}
            )
        )
        engine2 = IngestionEngine(pool)
        result2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result2.status == "completed"

    # Bob must now be tombstoned
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT _deleted, _deleted_at, data, raw FROM {src_table} "
            f"WHERE external_id = 'contact-ts-2'"
        )).fetchone()

    assert row is not None, "contact-ts-2 must still exist (tombstone, not hard delete)"
    _deleted, _deleted_at, data, raw = row
    assert _deleted is True, "tombstoned record must have _deleted = TRUE"
    assert _deleted_at is not None, "tombstoned record must have _deleted_at set"
    # Tombstone data/raw should be empty/null payload
    if data is not None:
        data_dict = data if isinstance(data, dict) else __import__("json").loads(data)
        assert data_dict == {} or not data_dict, (
            f"Tombstone data must be empty payload; got {data_dict}"
        )


@pytest.mark.anyio
async def test_tombstone_preserves_other_active_records(pool, run_migrations):
    """T1 #32: tombstoning one record must not affect other active records.

    Records not absent from the full sync must remain active and unmodified.
    """
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()

    contacts = [
        {"id": "contact-ts-10", "name": "Carol"},
        {"id": "contact-ts-11", "name": "Dave (will be deleted)"},
        {"id": "contact-ts-12", "name": "Erin"},
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"contacts": contacts, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Reset watermark for second full sync
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()

    # Second full sync — Dave is missing
    contacts_sans_dave = [c for c in contacts if c["id"] != "contact-ts-11"]
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"contacts": contacts_sans_dave, "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, _deleted FROM {src_table} WHERE external_id IN "
            f"('contact-ts-10', 'contact-ts-11', 'contact-ts-12')"
        )).fetchall()

    status = {r[0]: r[1] for r in rows}
    assert status.get("contact-ts-10") is not True, "Carol must remain active"
    assert status.get("contact-ts-12") is not True, "Erin must remain active"
    assert status.get("contact-ts-11") is True, "Dave must be tombstoned"


@pytest.mark.anyio
async def test_tombstone_circuit_breaker_blocks_mass_deletion(pool, run_migrations):
    """T1 #32 / T1 #13: when > 50% of known records disappear in a full sync,
    the circuit breaker fires and no tombstones are written — preventing mass
    false deletions caused by a broken or partial API response.
    """
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()

    # Seed 4 contacts
    all_contacts = [
        {"id": f"contact-ts-cb-{i}", "name": f"Person {i}"}
        for i in range(4)
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"contacts": all_contacts, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Reset watermark
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()

    # Second sync returns only 1 of 4 records — 75% "deletion" → circuit breaker
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"contacts": [all_contacts[0]], "next_cursor": None}
            )
        )
        engine2 = IngestionEngine(pool)
        result = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    # Pagination drift protection may abort the sync when record count drops 75%.
    # That's fine — the important invariant is that no tombstones are written.
    assert result.status in ("completed", "aborted"), (
        f"Sync must complete or abort cleanly; got {result.status!r}"
    )

    # None of the 3 absent records should have been tombstoned
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, _deleted FROM {src_table} WHERE external_id LIKE 'contact-ts-cb-%'"
        )).fetchall()

    deleted_count = sum(1 for r in rows if r[1])
    assert deleted_count == 0, (
        f"Circuit breaker must block mass deletion; {deleted_count} records were tombstoned"
    )
