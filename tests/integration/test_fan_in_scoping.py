"""Integration tests for multi-connector fan-in shared tables (T1 #46)."""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import source_table_name

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_DATATYPE = "contacts"


def _make_ingestion_cfg() -> IngestionConfig:
    return IngestionConfig(
        primary_key="id",
        history_mode=HistoryMode.overwrite,
        schedule=ScheduleConfig(interval="5m"),
        **{
            "list": ListConfig(
                method="GET",
                path="/v1/contacts",
                record_selector="results",
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


def _make_fan_in_connector(name: str, base_url: str, shared_table: str) -> ConnectorConfig:
    return ConnectorConfig(
        name=name,
        system="FanInSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=base_url),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref=f"{name}_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                shared_table=shared_table,
                ingestion=_make_ingestion_cfg(),
            )
        },
    )


def test_fan_in_shared_table_name_convention():
    """source_table_name with shared_table returns inout_src_{shared_table}."""
    table = source_table_name("connector_a", _DATATYPE, shared_table="people")
    assert table == "inout_src_people"

    # Without shared_table, connector-scoped name is used
    scoped = source_table_name("connector_a", _DATATYPE)
    assert scoped == "inout_src_connector_a_contacts"


@pytest.mark.anyio
async def test_fan_in_two_connectors_write_to_shared_table(pool):
    """Two connectors configured with the same shared_table both write to inout_src_{shared_table}.

    Each row is tagged with _connector so rows can be attributed to their source.
    """
    shared = "fan_in_people_a"
    os.environ["INOUT_CREDENTIAL_FANIN_A1_KEY"] = "dummy"
    os.environ["INOUT_CREDENTIAL_FANIN_B1_KEY"] = "dummy"

    conn_a = _make_fan_in_connector("fanin_a1", "https://api.conn-a.example.com", shared)
    conn_b = _make_fan_in_connector("fanin_b1", "https://api.conn-b.example.com", shared)

    shared_table = f"inout_src_{shared}"

    # Start with a clean shared table
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {shared_table}")
        await conn.commit()

    with respx.mock(assert_all_called=False) as mock:
        mock.get("https://api.conn-a.example.com/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": [{"id": "p1", "name": "Alice"}], "next_cursor": None})
        )
        mock.get("https://api.conn-b.example.com/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": [{"id": "p2", "name": "Bob"}], "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result_a = await engine.run_sync(conn_a, _DATATYPE, conn_a.datatypes[_DATATYPE].ingestion)
        result_b = await engine.run_sync(conn_b, _DATATYPE, conn_b.datatypes[_DATATYPE].ingestion)

    assert result_a.status == "completed"
    assert result_b.status == "completed"
    assert result_a.records_inserted == 1
    assert result_b.records_inserted == 1

    async with pool.connection() as conn:
        rows = await (await conn.execute(
            f"SELECT external_id, _connector FROM {shared_table} ORDER BY external_id"
        )).fetchall()

    assert len(rows) == 2
    connectors = {r[1] for r in rows}
    assert "fanin_a1" in connectors
    assert "fanin_b1" in connectors


@pytest.mark.anyio
async def test_fan_in_connector_column_scopes_rows_per_source(pool):
    """The _connector column correctly tags each row with its source connector.

    Rows from different connectors in the shared table can be identified
    and filtered by their _connector value.
    """
    shared = "fan_in_people_b"
    os.environ["INOUT_CREDENTIAL_FANIN_X_KEY"] = "dummy"
    os.environ["INOUT_CREDENTIAL_FANIN_Y_KEY"] = "dummy"

    conn_x = _make_fan_in_connector("fanin_x", "https://api.conn-x.example.com", shared)
    conn_y = _make_fan_in_connector("fanin_y", "https://api.conn-y.example.com", shared)

    shared_table = f"inout_src_{shared}"

    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {shared_table}")
        await conn.commit()

    with respx.mock(assert_all_called=False) as mock:
        mock.get("https://api.conn-x.example.com/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": [{"id": "cx-1", "val": "X"}, {"id": "cx-2", "val": "X2"}], "next_cursor": None})
        )
        mock.get("https://api.conn-y.example.com/v1/contacts").mock(
            return_value=httpx.Response(200, json={"results": [{"id": "cy-1", "val": "Y"}], "next_cursor": None})
        )

        engine = IngestionEngine(pool)
        result_x = await engine.run_sync(conn_x, _DATATYPE, conn_x.datatypes[_DATATYPE].ingestion)
        result_y = await engine.run_sync(conn_y, _DATATYPE, conn_y.datatypes[_DATATYPE].ingestion)

    assert result_x.status == "completed"
    assert result_y.status == "completed"

    async with pool.connection() as conn:
        # Each connector's rows are independently queryable
        x_rows = await (await conn.execute(
            f"SELECT external_id FROM {shared_table} WHERE _connector = 'fanin_x' ORDER BY external_id"
        )).fetchall()
        y_rows = await (await conn.execute(
            f"SELECT external_id FROM {shared_table} WHERE _connector = 'fanin_y' ORDER BY external_id"
        )).fetchall()

    assert len(x_rows) == 2
    assert len(y_rows) == 1
    assert x_rows[0][0] == "cx-1"
    assert x_rows[1][0] == "cx-2"
    assert y_rows[0][0] == "cy-1"
