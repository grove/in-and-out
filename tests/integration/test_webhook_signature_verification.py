"""Integration tests for T1 #34: Webhook inbound signature verification.

The ingestion tool must verify HMAC-SHA256 (and HMAC-SHA1) signatures on
inbound webhook payloads.  Payloads that fail verification must be rejected
with HTTP 401 and never be processed.  Valid signatures must be accepted and
the record persisted to the source table.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time

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

_CONNECTOR = "sig_verify_test"
_DATATYPE = "leads"
_BASE_URL = "https://api.sig-test.example.com"
_SECRET = "super_secret_webhook_key"
os.environ.setdefault("INOUT_CREDENTIAL_SIG_VERIFY_SECRET", _SECRET)
os.environ.setdefault("INOUT_CREDENTIAL_SIG_VERIFY_KEY", "dummy")


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="SigVerifySystem",
        generation_profile=GenerationProfile.ingestion_webhook_incremental,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="sig_verify_key",
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
                            record_selector="leads",
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
                        subscriptions=[{"event": "lead.created"}],
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
            credential_ref="sig_verify_secret",
        ),
        fan_out=FanOutConfig(
            discriminator="event_type",
            routes=[FanOutRoute(match="lead.created", datatype=_DATATYPE)],
            unmatched=UnmatchedAction.log_and_discard,
        ),
    )


def _sign_body(body: bytes, secret: str) -> str:
    """Return HMAC-SHA256 hex digest (no prefix)."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _build_app(pool) -> FastAPI:
    connector = _make_connector()
    webhook_cfg = _make_webhook_cfg()
    engine = IngestionEngine(pool)

    async def _handler(request: Request) -> Response:
        return await handle_webhook(request, connector, webhook_cfg, engine)

    app = FastAPI()
    app.add_api_route(f"/webhooks/{_CONNECTOR}", _handler, methods=["POST"])
    return app


@pytest.mark.anyio
async def test_valid_signature_accepted_and_record_persisted(pool, run_migrations):
    """T1 #34: payload with a correct HMAC-SHA256 signature is accepted (200)
    and the record is written to the source table."""
    app = _build_app(pool)

    payload = {"event_type": "lead.created", "id": "lead-001", "name": "Alice"}
    body = json.dumps(payload).encode()
    sig = _sign_body(body, _SECRET)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.post(
            f"/webhooks/{_CONNECTOR}",
            content=body,
            headers={
                "X-Hub-Signature-256": sig,
                "Content-Type": "application/json",
            },
        )

    assert resp.status_code in (200, 201, 202), (
        f"Valid signature must produce 2xx; got {resp.status_code}: {resp.text}"
    )

    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT data FROM {src_table} WHERE external_id = 'lead-001'"
            )
        ).fetchone()

    assert row is not None, "Record must be persisted after valid webhook"
    data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    assert data.get("name") == "Alice"


@pytest.mark.anyio
async def test_invalid_signature_rejected_with_401(pool, run_migrations):
    """T1 #34: payload with a wrong HMAC-SHA256 signature is rejected (401)
    and the record is NOT written to the source table."""
    app = _build_app(pool)

    payload = {"event_type": "lead.created", "id": "lead-002", "name": "Bob"}
    body = json.dumps(payload).encode()
    wrong_sig = _sign_body(body, "completely_wrong_secret")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.post(
            f"/webhooks/{_CONNECTOR}",
            content=body,
            headers={
                "X-Hub-Signature-256": wrong_sig,
                "Content-Type": "application/json",
            },
        )

    assert resp.status_code == 401, (
        f"Invalid signature must produce 401; got {resp.status_code}: {resp.text}"
    )

    # Record must NOT be in the source table
    src_table = source_table_name(_CONNECTOR, _DATATYPE)
    async with pool.connection() as conn:
        row = await (
            await conn.execute(
                f"SELECT external_id FROM {src_table} WHERE external_id = 'lead-002'"
            )
        ).fetchone()

    assert row is None, "Record must NOT be persisted when signature is invalid"


@pytest.mark.anyio
async def test_missing_signature_header_rejected_with_401(pool, run_migrations):
    """T1 #34: payload with no signature header at all is rejected with 401."""
    app = _build_app(pool)

    payload = {"event_type": "lead.created", "id": "lead-003", "name": "Carol"}
    body = json.dumps(payload).encode()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.post(
            f"/webhooks/{_CONNECTOR}",
            content=body,
            headers={"Content-Type": "application/json"},
            # No X-Hub-Signature-256 header
        )

    assert resp.status_code == 401, (
        f"Missing signature must produce 401; got {resp.status_code}: {resp.text}"
    )
