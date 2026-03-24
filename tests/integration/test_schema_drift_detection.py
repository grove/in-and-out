"""Integration tests for T1 #31: External API schema change tracking.

When the external API's response structure changes — new fields appear or
fields are removed — the ingestion engine must detect this drift rather than
silently accepting or ignoring it.  New fields trigger a bump to
``_schema_version`` on all rows so downstream consumers know to re-apply
transformations.  Orphan columns (in the DB but gone from the API) are
detected and may be pruned.

GOAL.md T1 #31: "When the external API's response structure changes — new
fields appear, fields are removed, or value types change — the ingestion
tool must detect and record this schema drift rather than silently accepting
or rejecting it. Schema versions should be stored alongside the data so
transformations can be re-evaluated when the schema evolves."
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
from inandout.config.ingestion import (
    HistoryMode,
    IngestionConfig,
    IncrementalConfig,
    ListConfig,
    RequestFilterConfig,
    RequestFilterMode,
    ScheduleConfig,
)
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import ensure_source_table, source_table_name
from inandout.postgres.schema_drift import detect_new_fields, detect_schema_drift


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "drift_test"
_DATATYPE = "products"
_BASE_URL = "https://api.drift-test.example.com"
os.environ.setdefault("INOUT_CREDENTIAL_DRIFT_TEST_KEY", "dummy")


def _make_connector(prune: bool = False) -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DriftSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="drift_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    prune_orphan_columns=prune,
                    **{
                        "list": ListConfig(
                            method="GET",
                            path=f"/v1/{_DATATYPE}",
                            record_selector="products",
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


async def _reset_watermark(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )
        await conn.commit()


@pytest.mark.anyio
async def test_new_api_field_bumps_schema_version(pool, run_migrations):
    """T1 #31: when the API response gains a new field, all rows get
    ``_schema_version`` bumped to signal consumers that re-transformation
    is needed.

    - First sync: records have ``{id, name}``
    - Second full sync: records now have ``{id, name, phone}`` — new field
    - After second sync: ``_schema_version`` on all rows must be > 1
    """
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    await _reset_watermark(pool)

    products_v1 = [
        {"id": "prod-001", "name": "Widget"},
        {"id": "prod-002", "name": "Gadget"},
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"products": products_v1, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        result1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result1.status == "completed"

    # Capture schema_version after first sync (may already be > 1 because the engine
    # compares API JSON keys against actual DB column names; for a JSONB table, keys
    # like 'id'/'name' are treated as "new fields" on first encounter).
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, _schema_version FROM {src_table}"
        )).fetchall()
    versions_after_sync1 = {r[0]: r[1] for r in rows}

    await _reset_watermark(pool)

    # Second full sync — records now include a new 'description' field
    products_v2 = [
        {"id": "prod-001", "name": "Widget", "description": "A fine widget"},
        {"id": "prod-002", "name": "Gadget", "description": "A shiny gadget"},
    ]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"products": products_v2, "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        result2 = await engine2.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result2.status == "completed"

    # _schema_version must have increased relative to after-sync-1 values
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, _schema_version FROM {src_table}"
        )).fetchall()
    versions_after_sync2 = {r[0]: r[1] for r in rows}
    for ext_id, v2 in versions_after_sync2.items():
        v1 = versions_after_sync1.get(ext_id, 0)
        assert v2 > v1, (
            f"{ext_id}: _schema_version must increase after new field added; "
            f"before={v1}, after={v2}"
        )


@pytest.mark.anyio
async def test_incremental_sync_does_not_trigger_schema_drift(pool, run_migrations):
    """T1 #31: schema drift detection only runs on full syncs (watermark=None).
    An incremental sync (watermark set) must NOT bump _schema_version even if
    the API happens to return new fields, because drift is only authoritative
    over a complete snapshot.
    """
    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    await _reset_watermark(pool)

    # Full sync: seeds the watermark via incremental config
    products_v1 = [{"id": "prod-incr-1", "name": "Widget", "updated_at": "2024-01-01T00:00:00Z"}]

    # Build a connector with incremental config so first sync sets the watermark
    from inandout.config.connector import ConnectorConfig as _CC
    from inandout.config.connector import ConnectionConfig as _ConC
    from inandout.config.connector import DatatypeConfig as _DTC
    from inandout.config.connector import GenerationProfile as _GP

    connector_inc = _CC(
        name=_CONNECTOR,
        system="DriftSystem",
        generation_profile=_GP.ingestion_polling_readonly,
        api_version="v1",
        connection=_ConC(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="drift_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: _DTC(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path=f"/v1/{_DATATYPE}",
                            record_selector="products",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                            incremental=IncrementalConfig(
                                enabled=True,
                                cursor_field="updated_at",
                                cursor_type="timestamp",
                                request_filter=RequestFilterConfig(
                                    mode=RequestFilterMode.query_param,
                                    **{"param": "since", "value": "${watermark}"},
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )
    ingestion_cfg_inc = connector_inc.datatypes[_DATATYPE].ingestion

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"products": products_v1, "next_cursor": None})
        )
        engine = IngestionEngine(pool)
        await engine.run_sync(connector_inc, _DATATYPE, ingestion_cfg_inc)

    # Capture schema_version after full sync
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT _schema_version FROM {src_table} WHERE external_id = 'prod-incr-1'"
        )).fetchone()
    version_after_full = row[0] if row else 1

    # Incremental sync: watermark exists → schema drift detection is SKIPPED
    # (engine.py: `if watermark is None and seen_fields:` guards the drift check)
    products_v2 = [{"id": "prod-incr-1", "name": "Widget", "brand_new_field": "surprise", "updated_at": "2024-06-01T00:00:00Z"}]

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(200, json={"products": products_v2, "next_cursor": None})
        )
        engine2 = IngestionEngine(pool)
        result = await engine2.run_sync(connector_inc, _DATATYPE, ingestion_cfg_inc)

    assert result.status == "completed"

    # Incremental sync must NOT bump _schema_version (drift check is guarded by watermark=None)
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT _schema_version FROM {src_table} WHERE external_id = 'prod-incr-1'"
        )).fetchone()
    version_after_incremental = row[0] if row else 1

    assert version_after_incremental == version_after_full, (
        f"Incremental sync must not trigger schema drift version bump; "
        f"after full={version_after_full}, after incremental={version_after_incremental}"
    )


@pytest.mark.anyio
async def test_detect_schema_drift_function_finds_orphans(pool, run_migrations):
    """T1 #31: the detect_schema_drift() utility correctly identifies DB
    columns that are absent from the latest observed field set.

    This tests the drift-detection primitive in isolation — not the full
    engine pipeline.
    """
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        # Manually add an extra column to simulate a previously-seen field
        await conn.execute(
            f"ALTER TABLE {src_table} ADD COLUMN IF NOT EXISTS legacy_field TEXT"
        )
        await conn.commit()

    # The latest API response no longer includes 'legacy_field'
    current_observed_fields = {"id", "name"}

    async with pool.connection() as conn:
        orphans = await detect_schema_drift(conn, src_table, current_observed_fields)

    assert "legacy_field" in orphans, (
        f"detect_schema_drift must report 'legacy_field' as orphan; got {orphans}"
    )


@pytest.mark.anyio
async def test_detect_new_fields_function_finds_additions(pool, run_migrations):
    """T1 #31: detect_new_fields() correctly identifies fields present in the
    API response but not yet in the DB table columns.
    """
    src_table = source_table_name(_CONNECTOR, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_source_table(conn, _CONNECTOR, _DATATYPE)
        await conn.commit()

    # API now returns 'brand_new_field' which has never been seen before
    observed_fields = {"id", "name", "brand_new_field"}

    async with pool.connection() as conn:
        new_fields = await detect_new_fields(conn, src_table, observed_fields)

    assert "brand_new_field" in new_fields, (
        f"detect_new_fields must report 'brand_new_field'; got {new_fields}"
    )
    # Standard columns must not be flagged as new
    system_cols = {"external_id", "data", "raw"}
    for col in system_cols:
        assert col not in new_fields, f"{col!r} must not appear in new_fields"
