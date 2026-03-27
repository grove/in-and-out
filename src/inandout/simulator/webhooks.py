"""Outbound webhook dispatcher for the demo simulator.

When a record is mutated (create / update / delete), the simulator can
proactively POST a webhook event to the running ingest engine — simulating
what a real CRM would do.  This is the "cool demo effect" that makes data
flow visually in real time.

Webhook payloads are signed with HMAC-SHA256 using the same shared secret as
the engine (resolved from the environment via ``credential_ref``).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone

import httpx

from inandout.config.connector import ConnectorConfig


def _resolve_secret(credential_ref: str | None) -> str | None:
    if not credential_ref:
        return None
    # Match the engine's credential-resolution convention:
    # INOUT_CREDENTIAL_<UPPERCASED_REF> or plain env var of the same name.
    env_key = f"INOUT_CREDENTIAL_{credential_ref.upper()}"
    return os.environ.get(env_key) or os.environ.get(credential_ref.upper())


def _sign(payload_bytes: bytes, secret: str, algorithm: str, encoding: str = "hex_prefix") -> str:
    algo = algorithm.lower().replace("-", "")
    if "sha256" in algo:
        digest = hmac.new(secret.encode(), payload_bytes, hashlib.sha256).digest()
    elif "sha1" in algo:
        digest = hmac.new(secret.encode(), payload_bytes, hashlib.sha1).digest()  # noqa: S324 — legacy
    else:
        digest = hmac.new(secret.encode(), payload_bytes, hashlib.sha256).digest()

    if encoding == "base64":
        return base64.b64encode(digest).decode()
    # hex_prefix (default): "sha256=<hex>"
    prefix = "sha256" if "sha256" in algo else "sha1"
    return f"{prefix}={digest.hex()}"


class WebhookDispatcher:
    """Dispatch outbound webhook events to the engine's webhook endpoint."""

    def __init__(self, engine_url: str = "http://localhost:9090") -> None:
        self._engine_url = engine_url.rstrip("/")
        # Shared client — caller must call aclose() when done.
        # Use a short connect timeout so a missing engine doesn't stall the event loop.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=0.3, read=0.5, write=0.5, pool=0.5)
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def dispatch(
        self,
        connector: ConnectorConfig,
        datatype: str,
        operation: str,  # "create" | "update" | "delete"
        record_id: str,
        record: dict | None,
    ) -> dict | None:
        """POST a webhook event; returns a result dict or None if no webhook configured."""
        if not connector.webhooks:
            return None

        webhook_cfg = connector.webhooks
        fan_out = getattr(webhook_cfg, "fan_out", None)
        if not fan_out:
            return None

        # Find the event_type string for this (datatype, operation) pair.
        event_type: str | None = None
        for route in getattr(fan_out, "routes", []):
            if getattr(route, "datatype", None) == datatype:
                et = (getattr(route, "match", "") or "").lower()
                if operation == "create" and ("creation" in et or et.endswith(".create")):
                    event_type = route.match
                    break
                if operation == "update" and ("change" in et or et.endswith(".update")):
                    event_type = route.match
                    break
                if operation == "delete" and ("delet" in et or et.endswith(".delete")):
                    event_type = route.match
                    break

        if event_type is None:
            # Fall back to the first route matching the datatype.
            for route in getattr(fan_out, "routes", []):
                if getattr(route, "datatype", None) == datatype:
                    event_type = getattr(route, "match", None)
                    break

        if event_type is None:
            return None

        path = getattr(webhook_cfg, "path", None)
        if not path:
            return None

        url = f"{self._engine_url}{path}"
        headers: dict[str, str] = {"Content-Type": "application/json"}

        registration = getattr(webhook_cfg, "registration", None)
        per_route = registration and getattr(registration, "per_route_registration", False)

        if per_route:
            # FEAT-SIM-01: registration-based payload (e.g. Tripletex).
            # Shape: {"subscriptionId": 0, "event": "<route.match>",
            #         "id": <record_id>, "value": <record | null>}
            payload: dict = {
                "subscriptionId": 0,
                "event": event_type,
                "id": _coerce_id(record_id),
                "value": None if operation == "delete" else record,
            }
        else:
            # Legacy HubSpot-style fan-out payload.
            discriminator_field = getattr(fan_out, "discriminator", "eventType") or "eventType"
            payload = {discriminator_field: event_type}
            if record:
                payload.update(record)
            payload["_simulator_ts"] = datetime.now(timezone.utc).isoformat()

        payload_json = json.dumps(payload)
        payload_bytes = payload_json.encode()

        sig_cfg = getattr(webhook_cfg, "signature", None)
        if sig_cfg:
            secret = _resolve_secret(getattr(sig_cfg, "credential_ref", None))
            if secret:
                sig_header = getattr(sig_cfg, "header", "X-Simulator-Signature")
                algorithm = getattr(sig_cfg, "algorithm", "hmac-sha256")
                encoding = getattr(sig_cfg, "encoding", "hex_prefix")
                headers[sig_header] = _sign(payload_bytes, secret, algorithm, encoding)
        elif getattr(webhook_cfg, "auth_header_name", None) and getattr(
            webhook_cfg, "auth_header_credential_ref", None
        ):
            # FEAT-WH-03: custom header auth — forward the pre-configured value.
            secret = _resolve_secret(webhook_cfg.auth_header_credential_ref)
            if secret:
                headers[webhook_cfg.auth_header_name] = secret

        # FEAT-WH-08: add the discriminator header if configured so the engine
        # can route by header without parsing the body.
        discriminator_header = getattr(fan_out, "discriminator_header", None)
        if discriminator_header:
            headers[discriminator_header] = event_type

        try:
            t0 = time.monotonic()
            resp = await self._client.post(url, content=payload_bytes, headers=headers)
            duration_ms = int((time.monotonic() - t0) * 1000)
            return {
                "url": url,
                "status": resp.status_code,
                "duration_ms": duration_ms,
                "payload_json": payload_json,
                "connector": connector.name,
                "datatype": datatype,
                "operation": operation,
                "record_id": record_id,
            }
        except Exception as exc:
            return {
                "url": url,
                "status": 0,
                "duration_ms": 0,
                "payload_json": payload_json,
                "error": str(exc),
                "connector": connector.name,
                "datatype": datatype,
                "operation": operation,
                "record_id": record_id,
            }

    def dispatch_nowait(
        self,
        connector: ConnectorConfig,
        datatype: str,
        operation: str,
        record_id: str,
        record: dict | None,
    ) -> None:
        """Schedule dispatch as a background task — never blocks the caller."""
        import asyncio

        asyncio.create_task(self.dispatch(connector, datatype, operation, record_id, record))


def _coerce_id(record_id: str) -> int | str:
    """Return an int if record_id is numeric (Tripletex uses integer IDs)."""
    try:
        return int(record_id)
    except (ValueError, TypeError):
        return record_id
