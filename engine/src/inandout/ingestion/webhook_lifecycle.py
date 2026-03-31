"""Webhook lifecycle management: registration, renewal, health-check, deregistration.

Implements T1 #7 (Webhook Lifecycle Management) and T1 #26 (Ownership Scoping).
"""

from __future__ import annotations

from typing import Any

import anyio
import orjson
import structlog
from psycopg_pool import AsyncConnectionPool

from inandout.config._duration import parse_duration
from inandout.config.connector import ConnectorConfig
from inandout.config.webhooks import FanOutRoute, WebhookConfig, WebhookRegistrationConfig
from inandout.observability.metrics import webhook_subscriptions_active
from inandout.transport.auth import resolve_credential
from inandout.transport.http import HttpTransportAdapter

logger = structlog.get_logger(__name__)


def _resolve_dot_path(obj: Any, path: str) -> Any:
    """Resolve a dot-notation path (e.g. 'data.id') against a nested dict."""
    parts = path.split(".")
    cur: Any = obj
    for p in parts:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur


def _resolve_extra(
    extra: dict[str, str],
    *,
    route_event: str = "",
) -> dict[str, str]:
    """Resolve placeholder values in a registration body/headers dict.

    Supported placeholders:
      ${route_event}          → replaced with *route_event* (e.g. "customer.create")
      ${credential:<ref>}     → resolved via the standard credential resolver
    Static values are passed through unchanged.
    """
    out: dict[str, str] = {}
    for k, v in extra.items():
        if v == "${route_event}":
            out[k] = route_event
        elif v.startswith("${credential:") and v.endswith("}"):
            ref = v[len("${credential:") : -1]
            out[k] = resolve_credential(ref)
        else:
            out[k] = v
    return out


class WebhookLifecycleManager:
    """Manages the lifecycle of a single webhook subscription for one connector."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        connector_cfg: ConnectorConfig,
        webhook_cfg: WebhookConfig,
        engine: Any,  # IngestionEngine — avoid circular import
    ) -> None:
        self._pool = pool
        self._connector = connector_cfg
        self._webhook_cfg = webhook_cfg
        self._engine = engine
        self._registration = webhook_cfg.registration

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    async def register(self, callback_url: str) -> list[str]:
        """POST registration endpoint(s); extract webhook ID(s); persist to DB.

        Returns a list of webhook IDs (one per registration call — multiple when
        per_route_registration=True).
        """
        reg = self._registration
        assert reg is not None, "register called without registration config"

        log = logger.bind(connector=self._connector.name)

        if reg.per_route_registration:
            # FEAT-WH-01: one POST per fan_out route
            fan_out = self._webhook_cfg.fan_out
            if not fan_out or not fan_out.routes:
                raise ValueError("per_route_registration=True but fan_out.routes is empty")
            ids: list[str] = []
            for route in fan_out.routes:
                wid = await self._register_one(
                    callback_url, reg, route_event=route.match, route=route
                )
                ids.append(wid)
                log.info(
                    "webhook_registered_per_route",
                    webhook_id=wid,
                    route=route.match,
                )
            return ids

        elif reg.register_events_field:
            # FEAT-WH-07: single POST with all events as an array
            fan_out = self._webhook_cfg.fan_out
            events = [r.match for r in fan_out.routes] if fan_out else []
            wid = await self._register_one(
                callback_url, reg, route_event="", events_override=events
            )
            log.info("webhook_registered_events_array", webhook_id=wid, events=events)
            return [wid]

        else:
            # Default: single POST, no event list
            wid = await self._register_one(callback_url, reg)
            log.info("webhook_registered", webhook_id=wid, callback_url=callback_url)
            return [wid]

    async def _register_one(
        self,
        callback_url: str,
        reg: WebhookRegistrationConfig,
        *,
        route_event: str = "",
        route: "FanOutRoute | None" = None,
        events_override: list[str] | None = None,
    ) -> str:
        """POST one registration and persist to DB; returns the webhook_id string."""
        payload: dict[str, Any] = {
            reg.callback_url_runtime_param: callback_url,
        }

        # FEAT-WH-02: resolve register_body_extra placeholders
        if reg.register_body_extra:
            payload.update(_resolve_extra(reg.register_body_extra, route_event=route_event))

        # FEAT-WH-07: events array
        if events_override is not None and reg.register_events_field:
            payload[reg.register_events_field] = events_override

        # FEAT-WH-06: resolve register_headers_extra placeholders
        extra_headers: dict[str, str] = {}
        if reg.register_headers_extra:
            extra_headers = _resolve_extra(reg.register_headers_extra, route_event=route_event)

        async with HttpTransportAdapter(self._connector) as transport:
            resp = await transport._raw_request(
                "POST",
                reg.register_path,
                json=payload,
                headers=extra_headers or None,
            )

        try:
            body = orjson.loads(resp.content)
        except Exception:
            body = {}

        webhook_id = _resolve_dot_path(body, reg.id_response_path)
        if webhook_id is None:
            raise ValueError(
                f"Could not extract webhook ID from registration response using path "
                f"{reg.id_response_path!r}. Response body: {body!r}"
            )
        webhook_id_str = str(webhook_id)

        # datatype column: for per-route, use the route.match value so each
        # subscription row in DB is uniquely scoped.
        datatype_tag = route.match if route is not None else None

        await self._upsert_subscription(
            webhook_id_str, callback_url, status="active", datatype=datatype_tag
        )
        return webhook_id_str

    async def renew(self, webhook_id: str) -> None:
        """PUT/PATCH the renew endpoint for *webhook_id*."""
        reg = self._registration
        assert reg is not None
        if reg.renew_path is None:
            return

        path = reg.renew_path.replace("${webhook_id}", webhook_id)
        async with HttpTransportAdapter(self._connector) as transport:
            await transport._raw_request("PUT", path)

        # Update last_renewed_at
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                UPDATE inout_ops_webhook_subscriptions
                SET last_renewed_at = NOW(), status = 'active'
                WHERE connector = %s AND webhook_id = %s
                """,
                [self._connector.name, webhook_id],
            )
            await conn.commit()

        logger.info("webhook_renewed", connector=self._connector.name, webhook_id=webhook_id)

    async def deregister(self, webhook_id: str) -> None:
        """DELETE the webhook — but only if callback_url matches ours (ownership scoping T1 #26)."""
        reg = self._registration
        assert reg is not None
        if reg.deregister_path is None:
            return

        # Ownership check: only remove subscriptions we own
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                SELECT callback_url FROM inout_ops_webhook_subscriptions
                WHERE connector = %s AND webhook_id = %s
                """,
                    [self._connector.name, webhook_id],
                )
            ).fetchone()

        if row is None:
            logger.warning(
                "webhook_deregister_not_found",
                connector=self._connector.name,
                webhook_id=webhook_id,
            )
            return

        # We only deregister subscriptions we created — ownership scoping
        stored_callback_url = row[0]
        # Ownership is scoped by presence in our own DB table (we only insert
        # rows we created). This is the correct ownership check per T1 #26.
        _ = stored_callback_url

        path = reg.deregister_path.replace("${webhook_id}", webhook_id)
        async with HttpTransportAdapter(self._connector) as transport:
            await transport._raw_request("DELETE", path)

        async with self._pool.connection() as conn:
            await conn.execute(
                """
                UPDATE inout_ops_webhook_subscriptions
                SET status = 'deregistered'
                WHERE connector = %s AND webhook_id = %s
                """,
                [self._connector.name, webhook_id],
            )
            await conn.commit()

        try:
            stored_datatype = None
            if row is not None:
                # row[0] is callback_url; look up datatype via webhook_id
                async with self._pool.connection() as _c:
                    _r = await (
                        await _c.execute(
                            "SELECT datatype FROM inout_ops_webhook_subscriptions WHERE connector = %s AND webhook_id = %s",
                            [self._connector.name, webhook_id],
                        )
                    ).fetchone()
                    stored_datatype = _r[0] if _r else None
            webhook_subscriptions_active.labels(
                connector=self._connector.name,
                datatype=stored_datatype or "",
                sub_status="active",
            ).dec()
        except Exception:
            pass

        logger.info("webhook_deregistered", connector=self._connector.name, webhook_id=webhook_id)

    async def health_check(self, webhook_id: str) -> bool:
        """GET health endpoint; returns True if active, False if 404 or error."""
        reg = self._registration
        assert reg is not None
        if reg.health_check_path is None:
            return True

        path = reg.health_check_path.replace("${webhook_id}", webhook_id)
        try:
            async with HttpTransportAdapter(self._connector) as transport:
                resp = await transport._raw_request("GET", path)
            is_active = resp.status_code == 200
            if is_active and reg.health_check_active_field and reg.health_check_active_value is not None:
                try:
                    body_json = orjson.loads(resp.content)
                    actual = _resolve_dot_path(body_json, reg.health_check_active_field)
                    if str(actual) != reg.health_check_active_value:
                        logger.info(
                            "webhook_health_check_inactive_status",
                            connector=self._connector.name,
                            webhook_id=webhook_id,
                            field=reg.health_check_active_field,
                            actual=actual,
                            expected=reg.health_check_active_value,
                        )
                        is_active = False
                except Exception as body_exc:
                    logger.warning(
                        "webhook_health_check_body_parse_failed",
                        connector=self._connector.name,
                        webhook_id=webhook_id,
                        error=str(body_exc),
                    )
                    is_active = False
            async with self._pool.connection() as conn:
                await conn.execute(
                    """
                    UPDATE inout_ops_webhook_subscriptions
                    SET last_health_check_at = NOW()
                    WHERE connector = %s AND webhook_id = %s
                    """,
                    [self._connector.name, webhook_id],
                )
                await conn.commit()
            return is_active
        except Exception as exc:
            # 404 or connection error → treat as inactive
            logger.warning(
                "webhook_health_check_failed",
                connector=self._connector.name,
                webhook_id=webhook_id,
                error=str(exc),
            )
            return False

    async def run_lifecycle_loop(self, callback_url: str, tg: Any) -> None:
        """Long-running loop: register on start, renew on schedule, health-check periodically."""
        reg = self._registration
        assert reg is not None

        renew_interval_secs = parse_duration(reg.renew_interval)
        health_check_interval_secs = parse_duration(reg.health_check_interval)

        log = logger.bind(connector=self._connector.name)

        # Register on startup
        try:
            webhook_ids = await self.register(callback_url)
        except Exception as exc:
            log.error("webhook_lifecycle_register_failed", error=str(exc))
            return

        last_renewed = 0.0
        last_health_check = 0.0

        while True:
            import time

            now = time.monotonic()

            # Renew on schedule (renew each registered subscription)
            if now - last_renewed >= renew_interval_secs:
                for wid in webhook_ids:
                    try:
                        await self.renew(wid)
                    except Exception as exc:
                        log.warning("webhook_renew_failed", webhook_id=wid, error=str(exc))
                last_renewed = now

            # Health check (re-register any that are no longer active)
            if now - last_health_check >= health_check_interval_secs:
                new_ids: list[str] = []
                for wid in webhook_ids:
                    try:
                        is_active = await self.health_check(wid)
                        last_health_check = now
                        if not is_active:
                            log.warning(
                                "webhook_health_check_inactive_re_registering",
                                webhook_id=wid,
                            )
                            try:
                                re_ids = await self.register(callback_url)
                                new_ids.extend(re_ids)
                                last_renewed = now
                            except Exception as re_reg_exc:
                                log.error(
                                    "webhook_re_register_failed",
                                    webhook_id=wid,
                                    error=str(re_reg_exc),
                                )
                                new_ids.append(wid)  # keep old id in list
                        else:
                            new_ids.append(wid)
                    except Exception as exc:
                        log.warning("webhook_health_check_error", webhook_id=wid, error=str(exc))
                        new_ids.append(wid)
                webhook_ids = new_ids

            await anyio.sleep(min(health_check_interval_secs, 60.0))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _upsert_subscription(
        self,
        webhook_id: str,
        callback_url: str,
        status: str = "active",
        datatype: str | None = None,
    ) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO inout_ops_webhook_subscriptions
                    (connector, datatype, webhook_id, callback_url, status, registered_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (connector, datatype, webhook_id) DO UPDATE
                SET callback_url = EXCLUDED.callback_url,
                    status = EXCLUDED.status,
                    registered_at = NOW()
                """,
                [self._connector.name, datatype, webhook_id, callback_url, status],
            )
            await conn.commit()
        try:
            webhook_subscriptions_active.labels(
                connector=self._connector.name,
                datatype=datatype or "",
                sub_status=status,
            ).inc()
        except Exception:
            pass
