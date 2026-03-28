"""Integration tests for T1 #25: Webhook Event Deduplication.

The same webhook event may be redelivered multiple times due to network
retries, transient errors, or connector restarts.  The ingestion tool must
track processed event IDs and silently discard duplicates — returning HTTP 200
with ``{"status": "duplicate"}`` — without re-writing any data.

GOAL.md T1 #25: "The same webhook event may be redelivered multiple times due
to retries or restarts.  The tool must track processed event IDs to prevent
duplicate follow-up lookups, duplicate writes, or duplicate state transitions."
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import Response
from httpx import ASGITransport, AsyncClient

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
    ListConfig,
    ScheduleConfig,
    WebhookEventsConfig,
    WebhookPayloadType,
)
from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy
from inandout.config.webhooks import (
    FanOutConfig,
    FanOutRoute,
    SignatureAlgorithm,
    SignatureConfig,
    UnmatchedAction,
    WebhookConfig,
)
from inandout.ingestion.engine import IngestionEngine
from inandout.ingestion.webhooks import handle_webhook
from inandout.postgres.schema import source_table_name


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

_CONNECTOR = "dedup_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.dedup-test.example.com"
_SECRET = "dedup_secret_key"
os.environ.setdefault("INOUT_CREDENTIAL_DEDUP_SECRET", _SECRET)
os.environ.setdefault("INOUT_CREDENTIAL_DEDUP_KEY", "dummy")

_EVENT_ID_FIELD = "event_id"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="DedupSystem",
        generation_profile=GenerationProfile.ingestion_webhook_incremental,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="dedup_key",
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
                    webhook_events=WebhookEventsConfig(
                        subscriptions=[{"event": "contact.updated"}],
                        record_id_path="id",
                        payload_type=WebhookPayloadType.full_state,
                        ordering={"field": "updated_at"},
                    ),
                )
            )
        },
    )


def _make_webhook_cfg() -> WebhookConfig:
    return WebhookConfig(
        path=f"/webhooks/{_CONNECTOR}",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Hub-Signature-256",
            credential_ref="dedup_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[FanOutRoute(match="contact.updated", datatype=_DATATYPE)],
            unmatched=UnmatchedAction.log_and_discard,
        ),
        event_id_field=_EVENT_ID_FIELD,
    )


def _sign(body: bytes) -> str:
    return hmac.new(_SECRET.encode(), body, hashlib.sha256).hexdigest()


def _build_app(pool) -> FastAPI:
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    engine = IngestionEngine(pool)

    async def _handler(request: Request) -> Response:
        return await handle_webhook(request, connector, webhook_cfg, engine)

    app = FastAPI()
    app.add_api_route(f"/webhooks/{_CONNECTOR}", _handler, methods=["POST"])
    return app


async def _post(client: AsyncClient, payload: dict) -> AsyncClient:
    body = json.dumps(payload).encode()
    return await client.post(
        f"/webhooks/{_CONNECTOR}",
        content=body,
        headers={
            "X-Hub-Signature-256": _sign(body),
            "Content-Type": "application/json",
        },
    )


@pytest.mark.anyio
async def test_first_delivery_accepted_record_written(pool, run_migrations):
    """T1 #25: the first delivery of an event (unique event_id) is accepted
    and the record is persisted to the source table."""
    app = _build_app(pool)

    payload = {
        "event_type": "contact.updated",
        _EVENT_ID_FIELD: "evt-001",
        "id": "contact-dedup-1",
        "name": "Alice",
    }

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await _post(client, payload)

    assert resp.status_code in (200, 201, 202), (
        f"First delivery must be accepted; got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body.get("status") != "duplicate", "First delivery must not be flagged as duplicate"

    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT external_id FROM {src_table} WHERE external_id = 'contact-dedup-1'"
            )
        ).fetchone()

    assert row is not None, "Record must be written on first delivery"


@pytest.mark.anyio
async def test_duplicate_delivery_discarded_no_second_write(pool, run_migrations):
    """T1 #25: redelivery of the same event_id returns 200 with
    status='duplicate' and does NOT overwrite the already-persisted data."""
    app = _build_app(pool)

    payload_v1 = {
        "event_type": "contact.updated",
        _EVENT_ID_FIELD: "evt-002",
        "id": "contact-dedup-2",
        "name": "Bob Original",
    }
    # Same event_id, different payload content (simulates re-delivery after mutation)
    payload_v2 = {
        "event_type": "contact.updated",
        _EVENT_ID_FIELD: "evt-002",   # same event_id — must be deduplicated
        "id": "contact-dedup-2",
        "name": "Bob Should Not Overwrite",
    }

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp1 = await _post(client, payload_v1)
        assert resp1.status_code in (200, 201, 202)
        assert resp1.json().get("status") != "duplicate"

        resp2 = await _post(client, payload_v2)

    assert resp2.status_code == 200, (
        f"Duplicate delivery must return 200; got {resp2.status_code}"
    )
    assert resp2.json().get("status") == "duplicate", (
        f"Duplicate delivery response must include status='duplicate'; got {resp2.json()}"
    )

    # The source table must still contain the ORIGINAL data
    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {src_table} WHERE external_id = 'contact-dedup-2'"
            )
        ).fetchone()

    assert row is not None
    data = row[0] if isinstance(row[0], dict) else __import__("json").loads(row[0])
    assert data.get("name") == "Bob Original", (
        f"Duplicate delivery must not overwrite existing data; got name={data.get('name')!r}"
    )


@pytest.mark.anyio
async def test_different_event_ids_both_processed(pool, run_migrations):
    """T1 #25: two deliveries with different event_ids are both processed
    independently (dedup key is per event_id, not per record)."""
    app = _build_app(pool)

    payload_a = {
        "event_type": "contact.updated",
        _EVENT_ID_FIELD: "evt-003a",
        "id": "contact-dedup-3",
        "name": "Carol v1",
    }
    payload_b = {
        "event_type": "contact.updated",
        _EVENT_ID_FIELD: "evt-003b",   # different event_id → new event
        "id": "contact-dedup-3",
        "name": "Carol v2",
    }

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp_a = await _post(client, payload_a)
        resp_b = await _post(client, payload_b)

    assert resp_a.json().get("status") != "duplicate"
    assert resp_b.json().get("status") != "duplicate", (
        "A different event_id must be treated as a new event, not a duplicate"
    )

    # Record should have been updated to v2 (last writer wins)
    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {src_table} WHERE external_id = 'contact-dedup-3'"
            )
        ).fetchone()

    assert row is not None
    data = row[0] if isinstance(row[0], dict) else __import__("json").loads(row[0])
    assert data.get("name") == "Carol v2", (
        f"Second event with new event_id must overwrite; got name={data.get('name')!r}"
    )
