"""Outbound webhook dispatcher for the demo simulator.

When a record is mutated (create / update / delete), the simulator can
proactively POST a webhook event to the running ingest engine — simulating
what a real CRM would do.  This is the "cool demo effect" that makes data
flow visually in real time.

Webhook payloads are signed with HMAC-SHA256 using the same shared secret as
the engine (resolved from the environment via ``credential_ref``).
"""

from __future__ import annotations

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


def _sign(payload_bytes: bytes, secret: str, algorithm: str) -> str:
    algo = algorithm.lower().replace("-", "")
    if "sha256" in algo:
        return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()
    if "sha1" in algo:
        return hmac.new(secret.encode(), payload_bytes, hashlib.sha1).hexdigest()  # noqa: S324 — legacy signature scheme
    return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()


class WebhookDispatcher:
    """Dispatch outbound webhook events to the engine's webhook endpoint."""

    def __init__(self, engine_url: str = "http://localhost:9090") -> None:
        self._engine_url = engine_url.rstrip("/")
        # Shared client — caller must call aclose() when done.
        self._client = httpx.AsyncClient(timeout=0.5)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def dispatch(
        self,
        connector: ConnectorConfig,
        datatype: str,
        operation: str,  # "create" | "update" | "delete"
        record_id: str,
        record: dict | None,
    ) -> None:
        """Fire-and-forget webhook POST.  Failures are silently logged."""
        if not connector.webhooks:
            return

        webhook_cfg = connector.webhooks
        fan_out = getattr(webhook_cfg, "fan_out", None)
        if not fan_out:
            return

        # Find the event_type string for this (datatype, operation) pair.
        discriminator_field = getattr(fan_out, "discriminator", "eventType")
        event_type: str | None = None
        for route in getattr(fan_out, "routes", []):
            if getattr(route, "datatype", None) == datatype:
                event_type = getattr(route, "match", None)
                if operation == "create" and "creation" in (event_type or "").lower():
                    break
                if operation in ("update", "create") and "change" in (event_type or "").lower():
                    break
                if operation == "delete" and "delet" in (event_type or "").lower():
                    break

        if event_type is None:
            # Fall back to the first route matching the datatype.
            for route in getattr(fan_out, "routes", []):
                if getattr(route, "datatype", None) == datatype:
                    event_type = getattr(route, "match", None)
                    break

        if event_type is None:
            return

        path = getattr(webhook_cfg, "path", None)
        if not path:
            return

        url = f"{self._engine_url}{path}"
        payload: dict = {discriminator_field: event_type}
        if record:
            payload.update(record)
        payload["_simulator_ts"] = datetime.now(timezone.utc).isoformat()

        payload_bytes = json.dumps(payload).encode()
        headers: dict[str, str] = {"Content-Type": "application/json"}

        sig_cfg = getattr(webhook_cfg, "signature", None)
        if sig_cfg:
            secret = _resolve_secret(getattr(sig_cfg, "credential_ref", None))
            if secret:
                sig_header = getattr(sig_cfg, "header", "X-Simulator-Signature")
                algorithm = getattr(sig_cfg, "algorithm", "hmac-sha256")
                headers[sig_header] = _sign(payload_bytes, secret, algorithm)

        try:
            await self._client.post(url, content=payload_bytes, headers=headers)
        except Exception:
            pass  # best-effort; demo tool only
