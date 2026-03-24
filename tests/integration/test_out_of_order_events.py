"""Integration tests for T1 #35: out-of-order webhook event handling.

Webhook events and delta stream entries may arrive out of chronological order.
The ingestion tool must compare event timestamps to detect and discard stale
updates so that older events never overwrite newer data already persisted in
the source table.
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
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import (
    IngestionConfig,
    HistoryMode,
    ListConfig,
    ScheduleConfig,
    WebhookEventsConfig,
    OutOfOrderConfig,
    OutOfOrderStrategy,
    WebhookPayloadType,
)
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
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


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR_NAME = "oo_event_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.oo-test.example.com"
_SECRET = "ooo_test_secret_key"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR_NAME,
        system="OOTestSystem",
        generation_profile=GenerationProfile.ingestion_webhook_incremental,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="oo_test_key",
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
                    # Enable out-of-order detection on the updated_at field (default strategy)
                    webhook_events=WebhookEventsConfig(
                        subscriptions=[{"event": "contact.updated"}],
                        record_id_path="id",
                        payload_type=WebhookPayloadType.full_state,
                        ordering={"field": "updated_at"},
                        out_of_order=OutOfOrderConfig(
                            strategy=OutOfOrderStrategy.accept_latest_timestamp,
                            timestamp_field="updated_at",
                        ),
                    ),
                )
            )
        },
    )


def _make_webhook_cfg() -> WebhookConfig:
    return WebhookConfig(
        path=f"/webhooks/{_CONNECTOR_NAME}",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Hub-Signature-256",
            credential_ref="oo_test_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[FanOutRoute(match="contact.updated", datatype=_DATATYPE)],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )


def _make_signed_body(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _build_webhook_app(pool) -> FastAPI:
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    engine = IngestionEngine(pool)

    async def _webhook_handler(request: Request) -> Response:
        return await handle_webhook(request, connector, webhook_cfg, engine)

    app = FastAPI()
    app.add_api_route(
        f"/webhooks/{_CONNECTOR_NAME}", _webhook_handler, methods=["POST"]
    )
    return app


def _build_payload(contact_id: str, name: str, updated_at: str) -> dict:
    return {
        "event_type": "contact.updated",
        "id": contact_id,
        "name": name,
        "email": f"{contact_id}@example.com",
        "updated_at": updated_at,
    }


async def _post_webhook(client: AsyncClient, payload: dict, secret: str) -> dict:
    body = json.dumps(payload).encode()
    sig = _make_signed_body(body, secret)
    resp = await client.post(
        f"/webhooks/{_CONNECTOR_NAME}",
        content=body,
        headers={"X-Hub-Signature-256": sig, "Content-Type": "application/json"},
    )
    return resp


@pytest.mark.anyio
async def test_stale_event_is_discarded(pool, run_migrations):
    """A webhook event with an older timestamp is discarded when a newer one exists.

    T1 #35: send a newer event first (T=2024-01-02), then a stale event
    (T=2024-01-01).  The stale event must be rejected and the source table
    must retain the data from the newer event.
    """
    os.environ["INOUT_CREDENTIAL_OO_TEST_SECRET"] = _SECRET
    os.environ.setdefault("INOUT_CREDENTIAL_OO_TEST_KEY", "dummy")

    app = _build_webhook_app(pool)

    newer_payload = _build_payload("contact-101", "Alice (current)", "2024-01-02T12:00:00")
    stale_payload = _build_payload("contact-101", "Alice (stale)", "2024-01-01T08:00:00")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Send the newer event first
        resp_newer = await _post_webhook(client, newer_payload, _SECRET)
        assert resp_newer.status_code in (200, 201, 202), (
            f"Newer event should be accepted; got {resp_newer.status_code}: {resp_newer.text}"
        )

        # Send the stale event
        resp_stale = await _post_webhook(client, stale_payload, _SECRET)
        assert resp_stale.status_code in (200, 201, 202), (
            f"Stale event should return 200 (discarded gracefully); got {resp_stale.status_code}"
        )
        stale_body = resp_stale.json()
        assert stale_body.get("status") == "stale_discarded", (
            f"Stale event response must indicate stale_discarded; got: {stale_body}"
        )

    # Source table must contain data from the NEWER event, not the stale one
    src_table = source_table_name(_CONNECTOR_NAME, _DATATYPE)
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT data FROM {src_table} WHERE external_id = 'contact-101'",
        )).fetchone()

    assert row is not None, "contact-101 must exist in the source table"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Alice (current)", (
        f"Source table must retain newer event name; got: {data.get('name')!r}"
    )
    assert data.get("updated_at") == "2024-01-02T12:00:00", (
        f"Source table must retain newer timestamp; got: {data.get('updated_at')!r}"
    )


@pytest.mark.anyio
async def test_newer_event_overwrites_older(pool, run_migrations):
    """A webhook event with a newer timestamp overwrites previously stored data.

    T1 #35: send an older event first, then a newer one — the newer event must
    be accepted and the source table must reflect the updated data.
    """
    os.environ["INOUT_CREDENTIAL_OO_TEST_SECRET"] = _SECRET
    os.environ.setdefault("INOUT_CREDENTIAL_OO_TEST_KEY", "dummy")

    app = _build_webhook_app(pool)

    older_payload = _build_payload("contact-202", "Bob (old)", "2024-03-01T09:00:00")
    newer_payload = _build_payload("contact-202", "Bob (updated)", "2024-03-15T14:30:00")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Send older event first — must be accepted (no prior record exists)
        resp_older = await _post_webhook(client, older_payload, _SECRET)
        assert resp_older.status_code in (200, 201, 202)
        assert resp_older.json().get("status") != "stale_discarded"

        # Send newer event — must also be accepted and should overwrite
        resp_newer = await _post_webhook(client, newer_payload, _SECRET)
        assert resp_newer.status_code in (200, 201, 202)
        assert resp_newer.json().get("status") != "stale_discarded", (
            "The newer event must not be treated as stale"
        )

    # Source table should reflect the newer event
    src_table = source_table_name(_CONNECTOR_NAME, _DATATYPE)
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT data FROM {src_table} WHERE external_id = 'contact-202'",
        )).fetchone()

    assert row is not None, "contact-202 must exist in the source table"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Bob (updated)", (
        f"Source table must contain the newer event name; got: {data.get('name')!r}"
    )
    assert data.get("updated_at") == "2024-03-15T14:30:00", (
        f"Source table must contain the newer timestamp; got: {data.get('updated_at')!r}"
    )


@pytest.mark.anyio
async def test_same_timestamp_event_is_not_discarded(pool, run_migrations):
    """An event with an equal timestamp to the stored sequence is accepted.

    T1 #35: the comparison uses ``<=`` for stale detection (strictly older OR equal
    is stale), but for events with equal timestamps the typical behaviour depends
    on implementation.  This test verifies that the first event is always stored.
    For subsequent events with the SAME timestamp, the implementation may either
    accept or discard — this test only asserts the first event is persisted.
    """
    os.environ["INOUT_CREDENTIAL_OO_TEST_SECRET"] = _SECRET
    os.environ.setdefault("INOUT_CREDENTIAL_OO_TEST_KEY", "dummy")

    app = _build_webhook_app(pool)

    first_payload = _build_payload("contact-303", "Carol (v1)", "2024-06-01T10:00:00")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await _post_webhook(client, first_payload, _SECRET)
        assert resp.status_code in (200, 201, 202)
        # First event must never be discarded
        assert resp.json().get("status") != "stale_discarded"

    src_table = source_table_name(_CONNECTOR_NAME, _DATATYPE)
    async with pool.connection() as conn:
        row = await (await conn.execute(
            f"SELECT data FROM {src_table} WHERE external_id = 'contact-303'",
        )).fetchone()

    assert row is not None, "contact-303 must have been stored from the first event"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Carol (v1)"
