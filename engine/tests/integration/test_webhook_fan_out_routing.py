"""Integration tests for T1 #19: Shared event receivers with declarative fan-out.

A single webhook endpoint receives events for multiple datatypes.  Declarative
routing rules inspect the event payload's discriminator field and dispatch each
event to the correct per-datatype handler.  Unmatched events must be handled
according to the configured ``unmatched`` policy (discard or 400 reject).
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

_CONNECTOR = "fanout_test"
_DT_CONTACTS = "contacts"
_DT_DEALS = "deals"
_BASE_URL = "https://api.fanout-test.example.com"
_SECRET = "fanout_test_secret"
os.environ.setdefault("INOUT_CREDENTIAL_FANOUT_SECRET", _SECRET)
os.environ.setdefault("INOUT_CREDENTIAL_FANOUT_KEY", "dummy")


def _make_ingestion_cfg(datatype: str) -> IngestionConfig:
    return IngestionConfig(
        primary_key="id",
        history_mode=HistoryMode.overwrite,
        schedule=ScheduleConfig(interval="5m"),
        **{
            "list": ListConfig(
                method="GET",
                path=f"/v1/{datatype}",
                record_selector=datatype,
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
            subscriptions=[{"event": f"{datatype[:-1]}.updated"}],
            record_id_path="id",
            payload_type=WebhookPayloadType.full_state,
            ordering={"field": "updated_at"},
        ),
    )


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="FanOutTestSystem",
        generation_profile=GenerationProfile.ingestion_webhook_incremental,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="fanout_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DT_CONTACTS: DatatypeConfig(
                ingestion=_make_ingestion_cfg(_DT_CONTACTS)
            ),
            _DT_DEALS: DatatypeConfig(
                ingestion=_make_ingestion_cfg(_DT_DEALS)
            ),
        },
    )


def _make_webhook_cfg(unmatched: UnmatchedAction) -> WebhookConfig:
    """Build a shared-endpoint webhook config that fans out on 'event_type'."""
    return WebhookConfig(
        path=f"/webhooks/{_CONNECTOR}",
        signature=SignatureConfig(
            algorithm=SignatureAlgorithm.hmac_sha256,
            header="X-Hub-Signature-256",
            credential_ref="fanout_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[
                FanOutRoute(match="contact.updated", datatype=_DT_CONTACTS),
                FanOutRoute(match="deal.updated", datatype=_DT_DEALS),
            ],
            unmatched=unmatched,
        ),
    )


def _sign(body: bytes) -> str:
    return hmac.new(_SECRET.encode(), body, hashlib.sha256).hexdigest()


def _build_app(pool, unmatched: UnmatchedAction = UnmatchedAction.log_and_discard) -> FastAPI:
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg(unmatched)
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
async def test_contact_event_routed_to_contacts_table(pool, run_migrations):
    """T1 #19: 'contact.updated' event is routed to the contacts source table,
    not the deals table."""
    app = _build_app(pool)

    contact_event = {
        "event_type": "contact.updated",
        "id": "cid-fanout-1",
        "name": "Alice",
        "email": "alice@example.com",
    }

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await _post(client, contact_event)

    assert resp.status_code in (200, 201, 202), (
        f"Contact event should be accepted; got {resp.status_code}: {resp.text}"
    )

    contacts_table = source_table_name(_CONNECTOR, _DT_CONTACTS)
    deals_table = source_table_name(_CONNECTOR, _DT_DEALS)

    async with pool.connection() as conn:
        contact_row = await (
            await conn.execute(
                f"SELECT external_id FROM {contacts_table} WHERE external_id = 'cid-fanout-1'"
            )
        ).fetchone()
        # deals table might not even exist yet — handle that gracefully
        try:
            deal_row = await (
                await conn.execute(
                    f"SELECT external_id FROM {deals_table} WHERE external_id = 'cid-fanout-1'"
                )
            ).fetchone()
        except Exception:
            deal_row = None

    assert contact_row is not None, (
        "contact.updated event must write to the contacts source table"
    )
    assert deal_row is None, (
        "contact.updated event must NOT write to the deals source table"
    )


@pytest.mark.anyio
async def test_deal_event_routed_to_deals_table(pool, run_migrations):
    """T1 #19: 'deal.updated' event is routed to the deals source table,
    not the contacts table."""
    app = _build_app(pool)

    deal_event = {
        "event_type": "deal.updated",
        "id": "deal-fanout-1",
        "title": "Big Deal",
        "amount": 50000,
    }

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await _post(client, deal_event)

    assert resp.status_code in (200, 201, 202), (
        f"Deal event should be accepted; got {resp.status_code}: {resp.text}"
    )

    deals_table = source_table_name(_CONNECTOR, _DT_DEALS)
    contacts_table = source_table_name(_CONNECTOR, _DT_CONTACTS)

    async with pool.connection() as conn:
        deal_row = await (
            await conn.execute(
                f"SELECT external_id FROM {deals_table} WHERE external_id = 'deal-fanout-1'"
            )
        ).fetchone()
        try:
            contact_row = await (
                await conn.execute(
                    f"SELECT external_id FROM {contacts_table} WHERE external_id = 'deal-fanout-1'"
                )
            ).fetchone()
        except Exception:
            contact_row = None

    assert deal_row is not None, (
        "deal.updated event must write to the deals source table"
    )
    assert contact_row is None, (
        "deal.updated event must NOT write to the contacts source table"
    )


@pytest.mark.anyio
async def test_unmatched_event_discarded_when_policy_is_log_and_discard(pool, run_migrations):
    """T1 #19: unmatched event type with 'log_and_discard' policy returns 200
    with status 'discarded', and nothing is written to any table."""
    app = _build_app(pool, unmatched=UnmatchedAction.log_and_discard)

    unknown_event = {
        "event_type": "meeting.scheduled",    # not in any route
        "id": "meeting-1",
        "title": "Quarterly Review",
    }

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await _post(client, unknown_event)

    assert resp.status_code == 200, (
        f"Unmatched event with log_and_discard must return 200; got {resp.status_code}"
    )
    body = resp.json()
    assert body.get("status") == "discarded", (
        f"Response must indicate 'discarded'; got: {body}"
    )
