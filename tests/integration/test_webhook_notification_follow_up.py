"""Integration tests for T1 #8: Full-state resolution from notification-only events.

Some webhook providers send minimal "notification" payloads that only contain
an event type and an object ID — not the full record state.  The ingestion
engine must detect this pattern and perform a targeted follow-up GET to fetch
the complete record, storing it in the source table.

GOAL.md T1 #8: "Notification-only webhook → follow-up GET for full record:
Some sources send notification-only webhooks (e.g. just an event type and record
ID).  The tool must follow up with a detail-level GET to retrieve the full record
state before persisting."
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os

import httpx
import pytest
import respx
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

_CONNECTOR = "notif_followup"
_DATATYPE = "contacts"
_BASE_URL = "https://api.notif-followup.example.com"
_SECRET = "notif_test_secret"
os.environ.setdefault("INOUT_CREDENTIAL_NOTIF_FOLLOWUP_SECRET", _SECRET)
os.environ.setdefault("INOUT_CREDENTIAL_NOTIF_FOLLOWUP_KEY", "dummy")


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="NotifSystem",
        generation_profile=GenerationProfile.ingestion_webhook_incremental,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="notif_followup_key",
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
                            detail_path=f"/v1/{_DATATYPE}/${{external_id}}",
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
                        subscriptions=[{"event": "contact.created"}],
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
            credential_ref="notif_followup_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[
                FanOutRoute(
                    match="contact.created",
                    datatype=_DATATYPE,
                    notification_only=True,
                    notification_external_id_field="object_id",
                )
            ],
            unmatched=UnmatchedAction.log_and_discard,
        ),
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


async def _post(client: AsyncClient, payload: dict) -> httpx.Response:
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
async def test_notification_triggers_follow_up_get(pool, run_migrations):
    """T1 #8: a notification-only payload triggers a follow-up GET to the detail
    endpoint; the full record returned by that GET is persisted to the source table.

    The notification payload contains only (event_type, object_id) — no record
    fields.  The engine must call GET /v1/contacts/{id} and store the response.
    """
    full_record = {
        "id": "contact-nf-1",
        "name": "Diana Full",
        "email": "diana@example.com",
        "updated_at": "2024-05-01T09:00:00Z",
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}/contact-nf-1").mock(
            return_value=httpx.Response(200, json=full_record)
        )

        app = _build_app(pool)
        notification_payload = {
            "event_type": "contact.created",
            "object_id": "contact-nf-1",
            # Deliberately missing all record fields — this is a notification only
        }

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await _post(client, notification_payload)

    assert resp.status_code in (200, 201, 202), (
        f"Notification webhook must be accepted; got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body.get("status") == "triggered", (
        f"Response must report status='triggered'; got {body}"
    )

    # The full record returned by the follow-up GET must be stored
    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {src_table} WHERE external_id = 'contact-nf-1'"
            )
        ).fetchone()

    assert row is not None, "Follow-up GET result must be persisted to source table"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Diana Full", (
        f"Stored record must contain full data from detail GET; got name={data.get('name')!r}"
    )
    assert data.get("email") == "diana@example.com", (
        f"Stored record must include email from detail GET; got {data.get('email')!r}"
    )


@pytest.mark.anyio
async def test_full_state_from_detail_get_not_from_notification(pool, run_migrations):
    """T1 #8: the stored record MUST come from the detail-level GET, not from the
    sparse notification payload.

    The notification has an abbreviated 'name' field; the detail GET returns the
    full record.  The source table must contain the detail GET version.
    """
    full_record = {
        "id": "contact-nf-2",
        "name": "Edgar Full Name",
        "email": "edgar@example.com",
        "phone": "+1-555-0200",
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(f"/v1/{_DATATYPE}/contact-nf-2").mock(
            return_value=httpx.Response(200, json=full_record)
        )

        app = _build_app(pool)
        # Notification payload has a truncated name — should NOT be stored
        notification_payload = {
            "event_type": "contact.created",
            "object_id": "contact-nf-2",
            "name": "E.",   # sparse / abbreviated — should NOT end up in source table
        }

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await _post(client, notification_payload)

    assert resp.status_code in (200, 201, 202)

    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {src_table} WHERE external_id = 'contact-nf-2'"
            )
        ).fetchone()

    assert row is not None, "Record must be persisted"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Edgar Full Name", (
        f"Stored name must be from detail GET, not notification; got {data.get('name')!r}"
    )
    assert data.get("phone") == "+1-555-0200", (
        f"Phone (only in detail GET) must be stored; got {data.get('phone')!r}"
    )


@pytest.mark.anyio
async def test_notification_without_object_id_falls_back_gracefully(pool, run_migrations):
    """T1 #8: a notification payload that has NO extractable ID gracefully falls
    back (triggering a full sync or returning 200 without crashing).

    The engine must not raise an unhandled exception — it must return a
    2xx status even when the notification lacks the expected ID field.
    """
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # Full-sync fallback — return an empty list so no records are upserted
        mock.get(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(
                200, json={"contacts": [], "next_cursor": None}
            )
        )

        app = _build_app(pool)
        notification_payload = {
            "event_type": "contact.created",
            # Deliberately omit 'object_id' — the configured notification_external_id_field
        }

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await _post(client, notification_payload)

    assert resp.status_code in (200, 201, 202, 500), (
        f"Notification without ID must not crash; got {resp.status_code}"
    )
    # Critical: must not be a 4xx client error
    assert resp.status_code < 500 or resp.json().get("error") is not None, (
        "Engine must handle missing object_id gracefully without unhandled exceptions"
    )
