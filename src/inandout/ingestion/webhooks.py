"""Webhook receiver: signature verification, fan-out routing, and upsert."""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Any

import orjson
import structlog
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from inandout.config.connector import ConnectorConfig
from inandout.config.webhooks import SignatureAlgorithm, WebhookConfig, UnmatchedAction
from inandout.ingestion.engine import IngestionEngine, _extract_external_id, _upsert_record, _compute_raw_hash
from inandout.postgres.schema import source_table_name, ensure_source_table
from inandout.transport.auth import resolve_credential

logger = structlog.get_logger(__name__)


async def _log_webhook(
    pool: Any,
    connector: str,
    datatype: str | None,
    external_id: str | None,
    payload_hash: str,
    action: str,
    status: str,
) -> None:
    """Insert a row into inout_ops_webhook_log. Errors are silently swallowed."""
    try:
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO inout_ops_webhook_log
                    (connector, datatype, external_id, payload_hash, action, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [connector, datatype, external_id, payload_hash, action, status],
            )
            await conn.commit()
    except Exception:
        pass  # Audit log failure must never mask the original error

# Max clock skew tolerated between sender and receiver (seconds).
_MAX_TIMESTAMP_SKEW_SECS = 300


def _verify_hmac_sha256(
    secret: str,
    body: bytes,
    provided_sig: str,
    *,
    version: str | None = None,
    timestamp: str | None = None,
) -> bool:
    """Return True if the provided signature matches the computed HMAC-SHA256.

    Supports optional Stripe-style prefix ("v1=<hex>") and optional timestamp
    binding ("v1=<hex>" over "<timestamp>.<body>").
    """
    key = secret.encode()
    payload = body

    if timestamp is not None:
        payload = f"{timestamp}.".encode() + body

    expected = hmac.new(key, payload, hashlib.sha256).hexdigest()

    # Strip optional "v1=" or configured version prefix
    sig = provided_sig
    if version and sig.startswith(f"{version}="):
        sig = sig[len(version) + 1:]
    elif sig.startswith("v1=") or sig.startswith("sha256="):
        sig = sig.split("=", 1)[1]

    return hmac.compare_digest(expected, sig)


def _verify_signature(config: "WebhookConfig", body: bytes, headers: dict) -> bool:
    """Verify the request signature according to the connector's signature config."""
    sig_cfg = config.signature
    header_val = headers.get(sig_cfg.header.lower(), "")
    if not header_val:
        logger.warning("webhook_missing_signature_header", header=sig_cfg.header)
        return False

    secret = resolve_credential(sig_cfg.credential_ref)

    if sig_cfg.algorithm == SignatureAlgorithm.hmac_sha256:
        # Check for timestamp-binding (Stripe uses "t=<ts>,v1=<sig>")
        timestamp: str | None = None
        sig = header_val
        if "," in header_val:
            parts = dict(p.split("=", 1) for p in header_val.split(",") if "=" in p)
            ts_raw = parts.get("t")
            sig = parts.get(sig_cfg.version or "v1", parts.get("v1", ""))
            if ts_raw:
                ts = int(ts_raw)
                if abs(time.time() - ts) > _MAX_TIMESTAMP_SKEW_SECS:
                    logger.warning("webhook_timestamp_too_old", ts=ts)
                    return False
                timestamp = ts_raw

        return _verify_hmac_sha256(secret, body, sig, version=sig_cfg.version, timestamp=timestamp)

    elif sig_cfg.algorithm == SignatureAlgorithm.hmac_sha1:
        key = secret.encode()
        expected = hmac.new(key, body, hashlib.sha1).hexdigest()
        sig = header_val
        if sig.startswith("sha1="):
            sig = sig[5:]
        return hmac.compare_digest(expected, sig)

    logger.warning("webhook_unsupported_signature_algorithm", algorithm=sig_cfg.algorithm)
    return False


def _route_event(webhook_cfg: WebhookConfig, payload: dict) -> str | None:
    """Return the target datatype for this event, or None if unmatched."""
    fan_out = webhook_cfg.fan_out
    discriminator_value = str(payload.get(fan_out.discriminator, ""))
    for route in fan_out.routes:
        if route.match == discriminator_value or discriminator_value.startswith(route.match):
            return route.datatype
    return None


async def handle_webhook(
    request: Request,
    connector: ConnectorConfig,
    webhook_cfg: WebhookConfig,
    engine: IngestionEngine,
) -> Response:
    """Process a single incoming webhook request."""
    log = logger.bind(connector=connector.name, path=webhook_cfg.path)

    body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}

    # Signature verification
    if not _verify_signature(webhook_cfg, body, headers):
        log.warning("webhook_signature_invalid")
        return JSONResponse({"error": "invalid signature"}, status_code=401)

    try:
        payload = orjson.loads(body)
    except Exception:
        log.warning("webhook_invalid_json")
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    if not isinstance(payload, dict):
        log.warning("webhook_payload_not_object")
        return JSONResponse({"error": "payload must be a JSON object"}, status_code=400)

    # Fan-out routing
    datatype = _route_event(webhook_cfg, payload)
    if datatype is None:
        action = webhook_cfg.fan_out.unmatched
        if action == UnmatchedAction.reject_400:
            log.warning("webhook_unmatched_event", discriminator=webhook_cfg.fan_out.discriminator)
            return JSONResponse({"error": "unmatched event type"}, status_code=400)
        log.info("webhook_unmatched_event_discarded")
        return JSONResponse({"status": "discarded"}, status_code=200)

    log = log.bind(datatype=datatype)

    # Determine the ingestion config for this datatype
    dtype_cfg = connector.datatypes.get(datatype)
    if dtype_cfg is None or dtype_cfg.ingestion is None:
        log.warning("webhook_datatype_not_configured")
        return JSONResponse({"error": f"datatype {datatype!r} not configured"}, status_code=400)

    ingestion_cfg = dtype_cfg.ingestion

    # If the payload is a notification (no data fields, just an event reference), do a
    # full-state lookup via the standard ingestion engine.
    webhook_events_cfg = ingestion_cfg.webhook_events if hasattr(ingestion_cfg, "webhook_events") else None
    is_notification_only = (
        webhook_events_cfg is not None
        and getattr(webhook_events_cfg, "payload_type", "full") == "notification"
    )

    payload_hash = hashlib.sha256(body).hexdigest()

    if is_notification_only:
        log.info("webhook_notification_triggering_full_lookup")
        try:
            result = await engine.run_sync(connector, datatype, ingestion_cfg)
            log.info("webhook_lookup_complete", status=result.status, inserted=result.records_inserted)
            await _log_webhook(
                engine._pool, connector.name, datatype, None,
                payload_hash, "sync_triggered", "processed"
            )
            return JSONResponse({"status": "triggered", "sync_status": result.status})
        except Exception as exc:
            log.error("webhook_lookup_failed", error=str(exc))
            await _log_webhook(
                engine._pool, connector.name, datatype, None,
                payload_hash, "sync_triggered", "failed"
            )
            return JSONResponse({"error": "lookup failed"}, status_code=500)

    # Full-payload webhook: upsert the record directly.
    external_id = _extract_external_id(payload, ingestion_cfg.primary_key)
    if external_id is None:
        log.warning("webhook_missing_external_id", keys=list(payload.keys()))
        return JSONResponse({"error": "could not extract primary key"}, status_code=422)

    raw_hash = _compute_raw_hash(payload)
    table = source_table_name(connector.name, datatype)

    try:
        async with engine._pool.connection() as conn:
            await ensure_source_table(conn, connector.name, datatype)
            import uuid
            async with conn.transaction():
                run_id = uuid.uuid4()
                inserted, updated = await _upsert_record(conn, table, external_id, payload, raw_hash, run_id)

        log.info(
            "webhook_upserted",
            external_id=external_id,
            inserted=inserted,
            updated=updated,
        )
        await _log_webhook(
            engine._pool, connector.name, datatype, external_id,
            payload_hash, "direct_upsert", "processed"
        )
        return JSONResponse({"status": "ok", "inserted": inserted, "updated": updated})

    except Exception as exc:
        log.error("webhook_upsert_failed", error=str(exc))
        await _log_webhook(
            engine._pool, connector.name, datatype, None,
            payload_hash, "direct_upsert", "failed"
        )
        return JSONResponse({"error": "upsert failed"}, status_code=500)
