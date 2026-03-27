"""Webhook receiver: signature verification, fan-out routing, and upsert."""

from __future__ import annotations

import base64
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
from inandout.config.webhooks import (
    SignatureAlgorithm,
    SignatureEncoding,
    WebhookConfig,
    UnmatchedAction,
)
from inandout.ingestion.engine import (
    IngestionEngine,
    _extract_external_id,
    _upsert_record,
    _compute_raw_hash,
)
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
    encoding: SignatureEncoding = SignatureEncoding.hex_prefix,
    version: str | None = None,
    timestamp: str | None = None,
) -> bool:
    """Return True if the provided signature matches the computed HMAC-SHA256.

    Supports:
    - Optional Stripe-style timestamp binding ("<timestamp>.<body>")
    - encoding=hex_prefix: "sha256=<hex>" or "v1=<hex>" (GitHub, Stripe)
    - encoding=base64: raw base64-encoded digest (Shopify, SuperOffice)
    """
    key = secret.encode()
    payload = body

    if timestamp is not None:
        payload = f"{timestamp}.".encode() + body

    digest = hmac.new(key, payload, hashlib.sha256).digest()

    if encoding == SignatureEncoding.base64:
        computed = base64.b64encode(digest).decode()
        return hmac.compare_digest(computed, provided_sig)

    # hex_prefix: compare hex digest, stripping any "sha256=" / "v1=" prefix
    expected_hex = digest.hex()
    sig = provided_sig
    if version and sig.startswith(f"{version}="):
        sig = sig[len(version) + 1 :]
    elif sig.startswith("v1=") or sig.startswith("sha256="):
        sig = sig.split("=", 1)[1]

    return hmac.compare_digest(expected_hex, sig)


def _verify_signature(config: "WebhookConfig", body: bytes, headers: dict) -> bool:
    """Verify the request signature / auth according to the connector's config.

    Three modes:
    1. config.signature set → HMAC verification (supports rotation).
    2. config.auth_header_name set → constant-time header equality check (FEAT-WH-03).
    3. Neither set → accept (IP allowlist / registration-time auth handles it).
    """
    if config.signature:
        sig_cfg = config.signature
        header_val = headers.get(sig_cfg.header.lower(), "")
        if not header_val:
            logger.warning("webhook_missing_signature_header", header=sig_cfg.header)
            return False

        secret = resolve_credential(sig_cfg.credential_ref)
        if _verify_signature_with_secret(sig_cfg, secret, body, header_val):
            return True

        rotation_ref = getattr(sig_cfg, "rotation_credential_ref", None)
        if rotation_ref:
            try:
                rotation_secret = resolve_credential(rotation_ref)
                if _verify_signature_with_secret(sig_cfg, rotation_secret, body, header_val):
                    logger.info(
                        "webhook_signature_verified_with_rotation_secret",
                        credential_ref=rotation_ref,
                    )
                    return True
            except Exception as exc:
                logger.warning("webhook_rotation_secret_failed", error=str(exc))

        return False

    if config.auth_header_name and config.auth_header_credential_ref:
        # FEAT-WH-03: custom header equality (e.g. Tripletex forwards authHeaderValue).
        expected = resolve_credential(config.auth_header_credential_ref)
        received = headers.get(config.auth_header_name.lower(), "")
        if not hmac.compare_digest(received, expected):
            logger.warning("webhook_custom_header_auth_failed", header=config.auth_header_name)
            return False
        return True

    # No auth configured — accept (IP allowlist in the server middleware handles it).
    return True


def _verify_signature_with_secret(
    sig_cfg: Any,
    secret: str,
    body: bytes,
    header_val: str,
) -> bool:
    """Verify signature using a specific secret."""
    encoding = getattr(sig_cfg, "encoding", SignatureEncoding.hex_prefix)
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
                    return False
                timestamp = ts_raw

        return _verify_hmac_sha256(
            secret,
            body,
            sig,
            encoding=encoding,
            version=sig_cfg.version,
            timestamp=timestamp,
        )

    elif sig_cfg.algorithm == SignatureAlgorithm.hmac_sha1:
        key = secret.encode()
        expected = hmac.new(key, body, hashlib.sha1).hexdigest()
        sig = header_val
        if sig.startswith("sha1="):
            sig = sig[5:]
        return hmac.compare_digest(expected, sig)

    return False


def _route_event(
    webhook_cfg: WebhookConfig,
    payload: dict,
    headers: dict | None = None,
) -> str | None:
    """Return the target datatype for this event, or None if unmatched.

    FEAT-WH-08: if fan_out.discriminator_header is set, read the event name from
    the request headers instead of the payload body.
    """
    fan_out = webhook_cfg.fan_out
    if fan_out is None:
        return None
    if fan_out.discriminator_header and headers is not None:
        discriminator_value = headers.get(fan_out.discriminator_header.lower(), "")
    elif fan_out.discriminator:
        discriminator_value = str(payload.get(fan_out.discriminator, ""))
    else:
        return None
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

    # Signature / auth verification (FEAT-WH-03: also handles custom header auth)
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

    # Idempotency / deduplication (A5)
    if webhook_cfg.event_id_field:
        raw_event_id = payload.get(webhook_cfg.event_id_field)
        if raw_event_id is not None:
            event_id_str = str(raw_event_id)
            try:
                async with engine._pool.connection() as dedup_conn:
                    seen_row = await (
                        await dedup_conn.execute(
                            """
                        INSERT INTO inout_ops_webhook_seen (connector, event_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING event_id
                        """,
                            [connector.name, event_id_str],
                        )
                    ).fetchone()
                    await dedup_conn.commit()
                if seen_row is None:
                    # Row already existed — this is a duplicate event.
                    log.info("webhook_duplicate_discarded", event_id=event_id_str)
                    return JSONResponse({"status": "duplicate"}, status_code=200)
            except Exception:
                pass  # fail-open: if table doesn't exist yet, proceed normally

    # Fan-out routing (FEAT-WH-08: header discriminator supported via headers kwarg)
    datatype = _route_event(webhook_cfg, payload, headers=headers)
    if datatype is None:
        if webhook_cfg.fan_out is None:
            log.warning("webhook_no_fan_out_configured")
            return JSONResponse({"error": "no fan_out configured"}, status_code=400)
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

    # Fan-out route config for this datatype
    matched_route = None
    fan_out = webhook_cfg.fan_out
    if fan_out.discriminator_header and headers is not None:
        discriminator_value = headers.get(fan_out.discriminator_header.lower(), "")
    elif fan_out.discriminator:
        discriminator_value = str(payload.get(fan_out.discriminator, ""))
    else:
        discriminator_value = ""
    for route in fan_out.routes:
        if route.match == discriminator_value or discriminator_value.startswith(route.match):
            if route.datatype == datatype:
                matched_route = route
                break

    # If the payload is a notification (no data fields, just an event reference), do a
    # targeted single-record fetch (or fall back to full sync if no external_id).
    webhook_events_cfg = (
        ingestion_cfg.webhook_events if hasattr(ingestion_cfg, "webhook_events") else None
    )
    is_notification_only = (
        webhook_events_cfg is not None
        and getattr(webhook_events_cfg, "payload_type", "full") == "notification"
    ) or (matched_route is not None and matched_route.notification_only)

    # Out-of-order check for full-payload webhooks
    if not is_notification_only and webhook_events_cfg is not None:
        oo_cfg = getattr(webhook_events_cfg, "out_of_order", None)
        if oo_cfg is not None:
            from inandout.config.ingestion import OutOfOrderStrategy

            strategy = oo_cfg.strategy

            # T1 #35: buffer_and_reorder strategy
            if strategy == OutOfOrderStrategy.buffer_and_reorder:
                seq_field = oo_cfg.sequence_field
                if seq_field:
                    payload_seq = payload.get(seq_field)
                    if payload_seq is not None:
                        ext_id_for_oo = _extract_external_id(payload, ingestion_cfg.primary_key)
                        if ext_id_for_oo is not None:
                            try:
                                from inandout.ingestion.event_buffer import get_event_buffer

                                buffer = get_event_buffer(
                                    timeout_secs=oo_cfg.buffer_timeout_secs,
                                    max_size=oo_cfg.buffer_size,
                                )

                                # Convert sequence to int
                                try:
                                    seq_int = int(payload_seq)
                                except (ValueError, TypeError):
                                    # Non-numeric sequence - fall through to normal processing
                                    seq_int = None

                                if seq_int is not None:
                                    # Buffer the event and get any ready events
                                    ready_payloads = buffer.buffer_event(
                                        connector.name,
                                        datatype,
                                        ext_id_for_oo,
                                        seq_int,
                                        payload,
                                    )

                                    if not ready_payloads:
                                        # Event buffered, waiting for missing sequences
                                        log.debug(
                                            "webhook_event_buffered",
                                            external_id=ext_id_for_oo,
                                            sequence=seq_int,
                                        )
                                        return JSONResponse({"status": "buffered"}, status_code=202)

                                    # Process all ready payloads in sequence order
                                    for ready_payload in ready_payloads:
                                        await engine.ingest_record(
                                            connector,
                                            datatype,
                                            ready_payload,
                                            source="webhook",
                                            ingestion_cfg=ingestion_cfg,
                                        )

                                    return JSONResponse(
                                        {"status": "ok", "processed": len(ready_payloads)},
                                        status_code=200,
                                    )
                            except Exception as buffer_exc:
                                log.warning("event_buffer_error", error=str(buffer_exc))
                                # Fall through to normal processing on error

            elif strategy != OutOfOrderStrategy.ignore:
                ts_field = (
                    oo_cfg.timestamp_field
                    if strategy == OutOfOrderStrategy.accept_latest_timestamp
                    else oo_cfg.sequence_field
                )
                if ts_field:
                    payload_ts = payload.get(ts_field)
                    if payload_ts is not None:
                        ext_id_for_oo = _extract_external_id(payload, ingestion_cfg.primary_key)
                        if ext_id_for_oo is not None:
                            # For fan-out events, track sequence per-route to prevent
                            # cross-route ordering corruption (T1 #35).
                            route_key = (
                                matched_route.match if matched_route is not None else "__direct__"
                            )
                            try:
                                async with engine._pool.connection() as oo_conn:
                                    # Try per-route sequence table first (migration 019)
                                    use_route_seq = False
                                    try:
                                        oo_row = await (
                                            await oo_conn.execute(
                                                """
                                            SELECT last_seq
                                            FROM inout_ops_webhook_route_seq
                                            WHERE connector = %s
                                              AND datatype = %s
                                              AND route = %s
                                              AND external_id = %s
                                            """,
                                                [
                                                    connector.name,
                                                    datatype,
                                                    route_key,
                                                    ext_id_for_oo,
                                                ],
                                            )
                                        ).fetchone()
                                        use_route_seq = True
                                    except Exception:
                                        # Table not yet created; fall back to source table check
                                        oo_row = None
                                        use_route_seq = False

                                    if not use_route_seq:
                                        # Fallback: read last value from source table (non-fan-out path)
                                        src_table = source_table_name(connector.name, datatype)
                                        oo_row = await (
                                            await oo_conn.execute(
                                                f"SELECT data->>%s FROM {src_table} WHERE external_id = %s",
                                                [ts_field, ext_id_for_oo],
                                            )
                                        ).fetchone()

                                    stored_seq = oo_row[0] if oo_row else None
                                    if stored_seq is not None:
                                        try:
                                            _is_stale = float(payload_ts) <= float(stored_seq)
                                        except (ValueError, TypeError):
                                            _is_stale = str(payload_ts) <= str(stored_seq)
                                    if stored_seq is not None and _is_stale:
                                        log.info(
                                            "webhook_stale_event_discarded",
                                            external_id=ext_id_for_oo,
                                            route=route_key,
                                            payload_seq=str(payload_ts),
                                            stored_seq=str(stored_seq),
                                        )
                                        return JSONResponse(
                                            {"status": "stale_discarded"}, status_code=200
                                        )

                                    # Update per-route sequence so future events from this route
                                    # are compared against the value we just accepted.
                                    if use_route_seq:
                                        try:
                                            await oo_conn.execute(
                                                """
                                                INSERT INTO inout_ops_webhook_route_seq
                                                    (connector, datatype, route, external_id, last_seq, updated_at)
                                                VALUES (%s, %s, %s, %s, %s, NOW())
                                                ON CONFLICT (connector, datatype, route, external_id) DO UPDATE
                                                SET last_seq = EXCLUDED.last_seq,
                                                    updated_at = NOW()
                                                """,
                                                [
                                                    connector.name,
                                                    datatype,
                                                    route_key,
                                                    ext_id_for_oo,
                                                    str(payload_ts),
                                                ],
                                            )
                                            await oo_conn.commit()
                                        except Exception:
                                            pass  # Sequence update failure must not block processing
                            except Exception:
                                pass  # On error, accept and process (fail-open)

    payload_hash = hashlib.sha256(body).hexdigest()

    if is_notification_only:
        # Attempt targeted single-record fetch first
        ext_id_field = "id"
        if matched_route is not None:
            ext_id_field = matched_route.notification_external_id_field
        ext_id_from_payload = payload.get(ext_id_field)

        if ext_id_from_payload is not None:
            external_id_str = str(ext_id_from_payload)
            log.info(
                "webhook_notification_triggering_single_record_fetch", external_id=external_id_str
            )
            try:
                if hasattr(engine, "run_sync_single_record"):
                    result = await engine.run_sync_single_record(
                        connector, datatype, ingestion_cfg, external_id_str
                    )
                else:
                    result = await engine.run_sync(connector, datatype, ingestion_cfg)
                log.info("webhook_single_record_fetch_complete", status=result.status)
                await _log_webhook(
                    engine._pool,
                    connector.name,
                    datatype,
                    external_id_str,
                    payload_hash,
                    "single_record_fetch",
                    "processed",
                )
                return JSONResponse({"status": "triggered", "sync_status": result.status})
            except Exception as exc:
                log.error("webhook_single_record_fetch_failed", error=str(exc))
                await _log_webhook(
                    engine._pool,
                    connector.name,
                    datatype,
                    None,
                    payload_hash,
                    "single_record_fetch",
                    "failed",
                )
                return JSONResponse({"error": "fetch failed"}, status_code=500)
        else:
            log.warning("webhook_notification_missing_id_full_sync")
            try:
                result = await engine.run_sync(connector, datatype, ingestion_cfg)
                log.info(
                    "webhook_lookup_complete",
                    status=result.status,
                    inserted=result.records_inserted,
                )
                await _log_webhook(
                    engine._pool,
                    connector.name,
                    datatype,
                    None,
                    payload_hash,
                    "sync_triggered",
                    "processed",
                )
                return JSONResponse({"status": "triggered", "sync_status": result.status})
            except Exception as exc:
                log.error("webhook_lookup_failed", error=str(exc))
                await _log_webhook(
                    engine._pool,
                    connector.name,
                    datatype,
                    None,
                    payload_hash,
                    "sync_triggered",
                    "failed",
                )
                return JSONResponse({"error": "lookup failed"}, status_code=500)

    # Full-payload webhook: upsert the record directly.

    # FEAT-WH-04: Tripletex-style delete payload — "value" field is null.
    # The record identity is in the top-level ID field; synthesise a minimal
    # record dict so the upsert path can proceed with just the primary key.
    if "value" in payload and payload["value"] is None:
        ext_id_field = matched_route.notification_external_id_field if matched_route else "id"
        top_level_id = payload.get(ext_id_field)
        if top_level_id is None:
            log.warning("webhook_null_value_missing_id", keys=list(payload.keys()))
            return JSONResponse(
                {"error": "delete payload has null value and no id field"}, status_code=422
            )
        pk_field_name = (
            ingestion_cfg.primary_key
            if isinstance(ingestion_cfg.primary_key, str)
            else (ingestion_cfg.primary_key[0] if ingestion_cfg.primary_key else "id")
        )
        # Build a minimal record; upsert will keep the existing row if present,
        # or create a stub that downstream reconciliation can clean up.
        payload = {pk_field_name: top_level_id, "_deleted": True}
        log.info("webhook_null_value_delete_synthesised", external_id=str(top_level_id))

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
                inserted, updated, _resurrected = await _upsert_record(
                    conn, table, external_id, payload, raw_hash, run_id
                )

        log.info(
            "webhook_upserted",
            external_id=external_id,
            inserted=inserted,
            updated=updated,
        )
        await _log_webhook(
            engine._pool,
            connector.name,
            datatype,
            external_id,
            payload_hash,
            "direct_upsert",
            "processed",
        )
        return JSONResponse({"status": "ok", "inserted": inserted, "updated": updated})

    except Exception as exc:
        log.error("webhook_upsert_failed", error=str(exc))
        await _log_webhook(
            engine._pool, connector.name, datatype, None, payload_hash, "direct_upsert", "failed"
        )
        return JSONResponse({"error": "upsert failed"}, status_code=500)
