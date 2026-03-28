"""Integration tests for the management API (Step 40)."""
from __future__ import annotations

import os
import uuid

import httpx
import pytest

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.ingestion.daemon import _build_app
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.schema import ensure_dead_letter_table, dead_letter_table_name


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR_NAME = "api_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.apitest.example.com"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR_NAME,
        system="APITestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="api_test_key",
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
                            path="/v1/contacts",
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
                ),
            )
        },
    )


def _build_test_app(pool):
    """Build the FastAPI app for testing with the given pool."""
    connector = _make_connector()

    class _FakeConnectorFileCfg:
        def __init__(self, c):
            self.connector = c

    engine = IngestionEngine(pool)
    return _build_app(engine, [_FakeConnectorFileCfg(connector)], pool=pool)


@pytest.mark.anyio
async def test_api_connectors_list(pool, run_migrations):
    """GET /api/connectors returns connector names from inout_ops_sync_run."""
    os.environ.setdefault("INOUT_CREDENTIAL_API_TEST_KEY", "dummy")

    # Insert a sync run row so the connector appears in the list
    run_id = str(uuid.uuid4())
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_sync_run (id, connector, datatype, mode, status, started_at, finished_at)
            VALUES (%s, %s, %s, 'full', 'completed', NOW(), NOW())
            """,
            [run_id, _CONNECTOR_NAME, _DATATYPE],
        )
        await conn.commit()

    app = _build_test_app(pool)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/connectors")

    assert resp.status_code == 200
    data = resp.json()
    names = [item["name"] for item in data]
    assert _CONNECTOR_NAME in names


@pytest.mark.anyio
async def test_api_force_sync_inserts_control_command(pool, run_migrations):
    """POST /api/connectors/{connector}/datatypes/{datatype}/force-sync creates a control row."""
    app = _build_test_app(pool)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/api/connectors/{_CONNECTOR_NAME}/datatypes/{_DATATYPE}/force-sync"
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["command"] == "force_full_sync"
    cmd_id = data["id"]

    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT command, status FROM inout_ops_control WHERE id = %s",
            [cmd_id],
        )).fetchone()

    assert row is not None
    assert row[0] == "force_full_sync"
    assert row[1] == "pending"


@pytest.mark.anyio
async def test_api_pause_and_resume(pool, run_migrations):
    """POST pause/resume inserts correct control commands."""
    app = _build_test_app(pool)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        pause_resp = await client.post(
            f"/api/connectors/{_CONNECTOR_NAME}/datatypes/{_DATATYPE}/pause"
        )
        resume_resp = await client.post(
            f"/api/connectors/{_CONNECTOR_NAME}/datatypes/{_DATATYPE}/resume"
        )

    assert pause_resp.status_code == 200
    assert resume_resp.status_code == 200

    pause_id = pause_resp.json()["id"]
    resume_id = resume_resp.json()["id"]

    async with pool.connection() as conn:
        pause_row = await (await conn.execute(
            "SELECT command FROM inout_ops_control WHERE id = %s", [pause_id]
        )).fetchone()
        resume_row = await (await conn.execute(
            "SELECT command FROM inout_ops_control WHERE id = %s", [resume_id]
        )).fetchone()

    assert pause_row is not None and pause_row[0] == "pause_connector"
    assert resume_row is not None and resume_row[0] == "resume_connector"


@pytest.mark.anyio
async def test_api_dead_letter_list(pool, run_migrations):
    """GET /api/dead-letter/{connector}/{datatype} returns DL rows."""
    # Ensure dead-letter table exists and insert a row
    async with pool.connection() as conn:
        await ensure_dead_letter_table(conn, "ingestion", _CONNECTOR_NAME, _DATATYPE)
        dl_table = dead_letter_table_name("ingestion", _CONNECTOR_NAME, _DATATYPE)
        await conn.execute(
            f"""
            INSERT INTO {dl_table} (external_id, raw, error_message, error_class)
            VALUES (%s, %s, %s, %s)
            """,
            ["ext-123", '{"id": "ext-123"}', "test error", "test_class"],
        )
        await conn.commit()

    app = _build_test_app(pool)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/api/dead-letter/{_CONNECTOR_NAME}/{_DATATYPE}")

    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) >= 1
    ext_ids = [r["external_id"] for r in rows]
    assert "ext-123" in ext_ids


@pytest.mark.anyio
async def test_api_health(pool, run_migrations):
    """GET /api/health returns {'status': 'ok'}."""
    app = _build_test_app(pool)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
