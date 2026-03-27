"""Config-driven FastAPI router builder for the demo simulator.

Reads a ``ConnectorConfig`` and registers FastAPI routes for every endpoint
the engine is expected to call:

* List endpoint (GET) with full pagination support for all five strategies.
* Detail / lookup endpoint (GET /{record_id}).
* Write endpoints (POST / PATCH / PUT / DELETE) from writeback operations.
* OAuth2 token endpoint (POST).

All routes are scoped under a ``/{connector_name}`` prefix so multiple
connectors can be served by the same process without path collisions.
"""

from __future__ import annotations

import json
import time
from urllib.parse import urlparse
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from inandout.config.connector import ConnectorConfig
from inandout.config.pagination import PaginationStrategy
from inandout.simulator.events import EventBus
from inandout.simulator.store import RecordStore
from inandout.simulator.webhooks import WebhookDispatcher


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pk_field(primary_key) -> str:
    if isinstance(primary_key, str):
        return primary_key
    if isinstance(primary_key, list) and primary_key:
        return primary_key[0]
    return "id"


def _to_fa_path(path: str) -> str:
    """Convert ``${external_id}`` template tokens to FastAPI ``{record_id}``."""
    return path.replace("${external_id}", "{record_id}")


def _extract_url_path(full_url: str) -> str:
    """Return the path component of a full URL."""
    return urlparse(full_url).path or "/"


def _set_path(obj: dict, dot_path: str, value: Any) -> None:
    """Write *value* at a dot-notation *dot_path* inside *obj*."""
    parts = dot_path.split(".")
    for part in parts[:-1]:
        obj = obj.setdefault(part, {})
    obj[parts[-1]] = value


def _get_path(obj: dict, dot_path: str) -> Any:
    """Read a value at *dot_path* from *obj*; return None if absent."""
    parts = dot_path.split(".")
    cur: Any = obj
    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _get_incremental_params(ingestion_cfg) -> tuple[str | None, str | None, str | None]:
    """Return (cursor_field, filter_param, cursor_type) from ingestion config."""
    inc = ingestion_cfg.list.incremental
    if not inc or not inc.enabled:
        return None, None, None
    cursor_field = inc.cursor_field
    cursor_type = inc.cursor_type.value if inc.cursor_type else None
    filter_param: str | None = None
    if inc.request_filter:
        filter_param = getattr(inc.request_filter, "param", None)
    return cursor_field, filter_param, cursor_type


# ---------------------------------------------------------------------------
# OpenAPI example builder
# ---------------------------------------------------------------------------


def _build_openapi_extra(
    action: str,
    seed: list[dict],
    pk_field: str,
    selector: str = "results",
    pagination=None,
    filter_param: str | None = None,
) -> dict:
    """Return an ``openapi_extra`` dict enriching Swagger UI with seed-data examples."""
    extra: dict = {}

    # Pagination + incremental filter params — always emitted for list endpoints
    # so Swagger UI shows the correct query parameters even without seed data.
    if action == "list":
        params: list[dict] = []
        if filter_param:
            params.append(
                {
                    "name": filter_param,
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Incremental filter: return records modified after this watermark",
                    "example": "2026-01-01T00:00:00Z",
                }
            )
        if pagination is not None:
            strategy = pagination.strategy
            if strategy == PaginationStrategy.cursor and pagination.cursor:
                params.append(
                    {
                        "name": pagination.cursor.request_param,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "string"},
                        "description": "Cursor token for the next page (from previous response)",
                    }
                )
            elif strategy == PaginationStrategy.offset:
                off_cfg = pagination.offset or {}
                off_p = off_cfg.get("param", "offset") if isinstance(off_cfg, dict) else "offset"
                lim_p = (
                    off_cfg.get("limit_param", "limit") if isinstance(off_cfg, dict) else "limit"
                )
                params.append(
                    {
                        "name": off_p,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 0},
                        "description": "Offset (number of records to skip)",
                    }
                )
                params.append(
                    {
                        "name": lim_p,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 20},
                        "description": "Maximum records per page",
                    }
                )
            elif strategy == PaginationStrategy.page_number:
                pn_cfg = pagination.page_number or {}
                page_p = pn_cfg.get("page_param", "page") if isinstance(pn_cfg, dict) else "page"
                pp_p = (
                    pn_cfg.get("per_page_param", "per_page")
                    if isinstance(pn_cfg, dict)
                    else "per_page"
                )
                params.append(
                    {
                        "name": page_p,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 1},
                        "description": "Page number (1-based)",
                    }
                )
                params.append(
                    {
                        "name": pp_p,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 20},
                        "description": "Records per page",
                    }
                )
            elif strategy == PaginationStrategy.keyset and pagination.keyset:
                ks = pagination.keyset
                params.append(
                    {
                        "name": ks.request_param,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "string"},
                        "description": f"Keyset cursor: return records with {ks.keyset_field} > this value",
                    }
                )
                params.append(
                    {
                        "name": ks.page_size_param,
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": ks.page_size},
                        "description": "Page size",
                    }
                )
        if params:
            extra["parameters"] = params

    # Seed-specific response / request-body examples
    if not seed:
        return extra

    first = seed[0]
    first_id = str(first.get(pk_field, "1"))
    # Body sent to the API never includes the pk (server owns it).
    body_fields = {k: v for k, v in first.items() if k != pk_field}

    # Request-body example
    if action in ("insert", "update", "archive", "upsert"):
        extra["requestBody"] = {
            "content": {
                "application/json": {
                    "examples": {
                        "seed-example": {
                            "summary": f"Example {action} from seed data",
                            "value": body_fields,
                        }
                    }
                }
            }
        }

    # Response example
    if action == "list":
        extra.setdefault("responses", {})["200"] = {
            "content": {
                "application/json": {
                    "examples": {
                        "seed-example": {
                            "summary": "First page from seed data",
                            "value": {selector: seed[:3]},
                        }
                    }
                }
            }
        }
    elif action == "lookup":
        extra["responses"] = {
            "200": {
                "content": {
                    "application/json": {
                        "examples": {
                            "seed-example": {
                                "summary": "Single record from seed data",
                                "value": first,
                            }
                        }
                    }
                }
            }
        }
    elif action in ("update", "upsert", "archive"):
        extra.setdefault("responses", {})["200"] = {
            "content": {
                "application/json": {
                    "examples": {
                        "seed-example": {
                            "summary": "Updated record",
                            "value": first,
                        }
                    }
                }
            }
        }
    elif action == "insert":
        extra.setdefault("responses", {})["201"] = {
            "content": {
                "application/json": {
                    "examples": {
                        "seed-example": {
                            "summary": "Created record id",
                            "value": {pk_field: first_id},
                        }
                    }
                }
            }
        }

    return extra


# ---------------------------------------------------------------------------
# Route builder
# ---------------------------------------------------------------------------


def build_connector_router(
    connector: ConnectorConfig,
    store: RecordStore,
    event_bus: EventBus,
    dispatcher: WebhookDispatcher,
    default_page_size: int = 20,
    webhook_subscriptions_store: dict | None = None,
) -> APIRouter:
    """Return a FastAPI ``APIRouter`` with all routes for *connector*.

    Mount this router at ``prefix="/{connector.name}"`` in the main app.
    """
    router = APIRouter()
    # Track (method, path) pairs to avoid duplicate registrations.
    registered: set[tuple[str, str]] = set()

    def _add(
        path: str,
        methods: list[str],
        handler,
        openapi_extra: dict | None = None,
        summary: str | None = None,
    ) -> None:
        for m in methods:
            key = (m.upper(), path)
            if key not in registered:
                registered.add(key)
                router.add_api_route(
                    path,
                    handler,
                    methods=[m],
                    openapi_extra=openapi_extra or None,
                    summary=summary,
                )

    connector_name = connector.name

    # ------------------------------------------------------------------
    # Auth — OAuth2 token endpoint
    # ------------------------------------------------------------------
    auth = connector.auth
    if getattr(auth, "type", None) == "oauth2":
        token_url_path = _extract_url_path(auth.oauth2.token_url)

        def _make_token_handler():
            async def token_endpoint(request: Request) -> JSONResponse:
                return JSONResponse(
                    {
                        "access_token": f"sim_token_{connector_name}",
                        "token_type": "Bearer",
                        "expires_in": 7200,
                    }
                )

            token_endpoint.__name__ = f"token_{connector_name}"
            return token_endpoint

        _add(token_url_path, ["POST"], _make_token_handler(), summary="Token")

    # ------------------------------------------------------------------
    # Per-datatype routes
    # ------------------------------------------------------------------
    for dt_name, dt_cfg in connector.datatypes.items():
        pk_field = _pk_field(dt_cfg.ingestion.primary_key if dt_cfg.ingestion else "id")

        # ----------------------------------------------------------
        # Ingestion: list endpoint
        # ----------------------------------------------------------
        if dt_cfg.ingestion:
            list_path = dt_cfg.ingestion.list.path
            record_selector = dt_cfg.ingestion.list.record_selector or "results"
            pagination = dt_cfg.ingestion.list.pagination
            strategy = pagination.strategy
            cursor_field, filter_param, cursor_type = _get_incremental_params(dt_cfg.ingestion)

            def _make_list_handler(
                _dt_name=dt_name,
                _pk=pk_field,
                _selector=record_selector,
                _pagination=pagination,
                _strategy=strategy,
                _cursor_field=cursor_field,
                _filter_param=filter_param,
                _cursor_type=cursor_type,
                _list_path=list_path,
            ):
                async def list_endpoint(request: Request) -> JSONResponse:
                    t0 = time.monotonic()
                    params = request.query_params

                    # Determine watermark from incremental filter param.
                    watermark: str | None = None
                    if _filter_param and _cursor_field:
                        raw = params.get(_filter_param)
                        if raw and _cursor_type == "timestamp":
                            watermark = raw

                    all_records = await store.list_all(
                        connector_name,
                        _dt_name,
                        cursor_field=_cursor_field if watermark else None,
                        watermark=watermark,
                    )
                    total = len(all_records)

                    # --- Determine offset from pagination strategy ---
                    offset = 0
                    effective_page_size = default_page_size

                    if _strategy == PaginationStrategy.cursor:
                        cursor_param = _pagination.cursor.request_param
                        raw_cursor = params.get(cursor_param) if cursor_param else None
                        if raw_cursor and raw_cursor.lstrip("-").isdigit():
                            offset = int(raw_cursor)
                        elif raw_cursor:
                            # cursor-as-URL: last segment may be an offset token
                            seg = raw_cursor.rstrip("/").split("/")[-1]
                            if seg.lstrip("-").isdigit():
                                offset = int(seg)

                    elif _strategy == PaginationStrategy.offset:
                        off_cfg = _pagination.offset or {}
                        off_p = (
                            off_cfg.get("param", "offset")
                            if isinstance(off_cfg, dict)
                            else "offset"
                        )
                        lim_p = (
                            off_cfg.get("limit_param", "limit")
                            if isinstance(off_cfg, dict)
                            else "limit"
                        )
                        offset = int(params.get(off_p, 0) or 0)
                        effective_page_size = int(
                            params.get(lim_p, default_page_size) or default_page_size
                        )

                    elif _strategy == PaginationStrategy.page_number:
                        pn_cfg = _pagination.page_number or {}
                        page_p = (
                            pn_cfg.get("page_param", "page") if isinstance(pn_cfg, dict) else "page"
                        )
                        pp_p = (
                            pn_cfg.get("per_page_param", "per_page")
                            if isinstance(pn_cfg, dict)
                            else "per_page"
                        )
                        page_num = int(params.get(page_p, 1) or 1)
                        effective_page_size = int(
                            params.get(pp_p, default_page_size) or default_page_size
                        )
                        offset = (page_num - 1) * effective_page_size

                    elif _strategy == PaginationStrategy.keyset:
                        ks = _pagination.keyset
                        after = params.get(ks.request_param) if ks else None
                        effective_page_size = ks.page_size if ks else default_page_size
                        if after:
                            all_records = [
                                r for r in all_records if str(r.get(ks.keyset_field, "")) > after
                            ]
                        total = len(all_records)

                    # --- Slice page ---
                    page = all_records[offset : offset + effective_page_size]
                    next_offset = offset + effective_page_size
                    has_more = next_offset < total

                    # Strip internal meta-keys before returning to the caller.
                    page = [{k: v for k, v in r.items() if not k.startswith("__")} for r in page]

                    # --- Build response body ---
                    body: dict = {_selector: page}
                    headers_out: dict[str, str] = {}

                    if has_more:
                        if _strategy == PaginationStrategy.cursor:
                            _set_path(body, _pagination.cursor.response_path, str(next_offset))

                        elif _strategy == PaginationStrategy.offset:
                            off_cfg = _pagination.offset or {}
                            if isinstance(off_cfg, dict) and off_cfg.get("total_path"):
                                _set_path(body, off_cfg["total_path"], total)

                        elif _strategy == PaginationStrategy.page_number:
                            pn_cfg = _pagination.page_number or {}
                            if isinstance(pn_cfg, dict) and pn_cfg.get("total_pages_path"):
                                total_pages = (
                                    total + effective_page_size - 1
                                ) // effective_page_size
                                _set_path(body, pn_cfg["total_pages_path"], total_pages)

                        elif _strategy == PaginationStrategy.link_header:
                            lh_cfg = _pagination.link_header or {}
                            hdr = (
                                lh_cfg.get("header", "Link") if isinstance(lh_cfg, dict) else "Link"
                            )
                            next_url = f"/{connector_name}{_list_path}?after={next_offset}"
                            headers_out[hdr] = f'<{next_url}>; rel="next"'

                    ms = int((time.monotonic() - t0) * 1000)
                    event_bus.publish_request(
                        connector_name,
                        _dt_name,
                        "GET",
                        str(request.url.path),
                        200,
                        ms,
                        request_headers_json=json.dumps(dict(request.headers)),
                    )
                    return JSONResponse(body, headers=headers_out)

                list_endpoint.__name__ = f"list_{dt_name}"
                return list_endpoint

            _add(
                list_path,
                ["GET"],
                _make_list_handler(),
                openapi_extra=_build_openapi_extra(
                    "list",
                    dt_cfg.simulator.seed_data if dt_cfg.simulator else [],
                    pk_field,
                    selector=record_selector,
                    pagination=pagination,
                    filter_param=filter_param,
                ),
                summary=f"List {dt_name}",
            )

        # ----------------------------------------------------------
        # Writeback: lookup / insert / update / delete / archive / upsert
        # ----------------------------------------------------------
        if dt_cfg.writeback:
            ops = dt_cfg.writeback.operations
            cursor_field_wb: str | None = None
            if dt_cfg.ingestion and dt_cfg.ingestion.list.incremental:
                cursor_field_wb = dt_cfg.ingestion.list.incremental.cursor_field

            for action in ("lookup", "insert", "update", "delete", "archive", "upsert"):
                op_cfg = getattr(ops, action, None)
                if op_cfg is None:
                    continue
                method = op_cfg.method.upper()
                fa_path = _to_fa_path(op_cfg.path)
                has_id = "{record_id}" in fa_path

                def _make_write_handler(
                    _action=action,
                    _method=method,
                    _pk=pk_field,
                    _dt_name=dt_name,
                    _has_id=has_id,
                    _cursor_field=cursor_field_wb,
                    _fa_path=fa_path,
                    _seed=dt_cfg.simulator.seed_data if dt_cfg.simulator else [],
                ):
                    async def write_endpoint(
                        request: Request,
                        record_id: str | None = None,
                    ) -> Response:
                        t0 = time.monotonic()

                        # --- Handle each action ---
                        if _action == "delete":
                            rid = record_id or ""
                            deleted = await store.delete(
                                connector_name, _dt_name, rid, source="engine"
                            )
                            if deleted:
                                dispatcher.dispatch_nowait(connector, _dt_name, "delete", rid, None)
                                ev = await store.recent_mutations(connector_name, _dt_name, 1)
                                if ev:
                                    event_bus.publish_mutation(ev[0])
                            ms = int((time.monotonic() - t0) * 1000)
                            event_bus.publish_request(
                                connector_name,
                                _dt_name,
                                "DELETE",
                                str(request.url.path),
                                204 if deleted else 404,
                                ms,
                                request_headers_json=json.dumps(dict(request.headers)),
                            )
                            return Response(status_code=204 if deleted else 404)

                        if _action == "lookup":
                            rid = record_id or ""
                            rec = await store.get_by_id(connector_name, _dt_name, rid)
                            ms = int((time.monotonic() - t0) * 1000)
                            event_bus.publish_request(
                                connector_name,
                                _dt_name,
                                "GET",
                                str(request.url.path),
                                200 if rec else 404,
                                ms,
                                request_headers_json=json.dumps(dict(request.headers)),
                            )
                            if rec is None:
                                return JSONResponse({"error": "not found"}, status_code=404)
                            display = {k: v for k, v in rec.items() if not k.startswith("__")}
                            return JSONResponse(display)

                        if _action == "insert":
                            try:
                                body = await request.json()
                            except Exception:
                                body = {}
                            record = await store.create(
                                connector_name, _dt_name, body, pk_field=_pk, source="engine"
                            )
                            new_id = str(record.get(_pk, ""))
                            dispatcher.dispatch_nowait(
                                connector, _dt_name, "create", new_id, record
                            )
                            evs = await store.recent_mutations(connector_name, _dt_name, 1)
                            if evs:
                                event_bus.publish_mutation(evs[0])
                            ms = int((time.monotonic() - t0) * 1000)
                            event_bus.publish_request(
                                connector_name,
                                _dt_name,
                                "POST",
                                str(request.url.path),
                                201,
                                ms,
                                request_body_json=json.dumps(body),
                                request_headers_json=json.dumps(dict(request.headers)),
                            )
                            display = {k: v for k, v in record.items() if not k.startswith("__")}
                            return JSONResponse(display, status_code=201)

                        # update / archive / upsert
                        try:
                            body = await request.json()
                        except Exception:
                            body = {}
                        rid = record_id or str(body.get(_pk, ""))
                        if _cursor_field:
                            from datetime import datetime as _dt, timezone as _tz

                            body[_cursor_field] = _dt.now(_tz.utc).isoformat()
                        updated = await store.update(
                            connector_name, _dt_name, rid, body, source="engine"
                        )
                        if updated is None:
                            ms = int((time.monotonic() - t0) * 1000)
                            event_bus.publish_request(
                                connector_name,
                                _dt_name,
                                _method,
                                str(request.url.path),
                                404,
                                ms,
                                request_body_json=json.dumps(body),
                                request_headers_json=json.dumps(dict(request.headers)),
                            )
                            return JSONResponse({"error": "not found"}, status_code=404)
                        dispatcher.dispatch_nowait(connector, _dt_name, "update", rid, updated)
                        evs = await store.recent_mutations(connector_name, _dt_name, 1)
                        if evs:
                            event_bus.publish_mutation(evs[0])
                        ms = int((time.monotonic() - t0) * 1000)
                        event_bus.publish_request(
                            connector_name,
                            _dt_name,
                            _method,
                            str(request.url.path),
                            200,
                            ms,
                            request_body_json=json.dumps(body),
                            request_headers_json=json.dumps(dict(request.headers)),
                        )
                        display = {k: v for k, v in updated.items() if not k.startswith("__")}
                        return JSONResponse(display)

                    _extra = _build_openapi_extra(_action, _seed, _pk)
                    _action_labels = {
                        "lookup": "Get",
                        "insert": "Create",
                        "update": "Update",
                        "delete": "Delete",
                        "archive": "Archive",
                        "upsert": "Upsert",
                    }
                    _summary = f"{_action_labels.get(_action, _action.title())} {_dt_name}"

                    if _has_id:

                        async def _ep_with_id(request: Request, record_id: str) -> Response:
                            return await write_endpoint(request, record_id)

                        _ep_with_id.__name__ = f"{_action}_{_dt_name}"
                        _add(
                            _fa_path, [_method], _ep_with_id, openapi_extra=_extra, summary=_summary
                        )
                    else:
                        # No {record_id} in the path — register a wrapper WITHOUT the
                        # record_id parameter so FastAPI does not expose it as a query
                        # parameter in the Swagger UI.
                        async def _ep_no_id(request: Request) -> Response:
                            return await write_endpoint(request, None)

                        _ep_no_id.__name__ = f"{_action}_{_dt_name}"
                        _add(_fa_path, [_method], _ep_no_id, openapi_extra=_extra, summary=_summary)

                _make_write_handler()

    # ------------------------------------------------------------------
    # Webhook registration endpoints (derived from registration config)
    # ------------------------------------------------------------------
    # When the engine calls WebhookLifecycleManager.register() it POSTs to
    # the connector's registration.register_path.  In the simulator that
    # path lives on *this* process, so we generate matching handlers here.
    # Subscriptions are kept in a simple in-memory dict scoped to the router.
    if connector.webhooks and connector.webhooks.registration:
        _add_webhook_registration_routes(
            connector,
            _add,
            registered,
            webhook_subscriptions_store=webhook_subscriptions_store,
            event_bus=event_bus,
        )

    return router


def _wh_path_to_fa(path: str) -> str:
    """Convert ``${webhook_id}`` tokens to FastAPI ``{webhook_id}``."""
    return path.replace("${webhook_id}", "{webhook_id}")


def _build_registration_example(connector: ConnectorConfig) -> dict:
    """Build an OpenAPI example request body for the webhook registration POST."""
    reg = connector.webhooks.registration  # type: ignore[union-attr]
    wh = connector.webhooks  # type: ignore[union-attr]

    # Seed the body from register_body_extra, resolving placeholder tokens to
    # human-readable example strings so Swagger is self-documenting.
    body: dict = {}
    first_event: str | None = None
    if wh.fan_out and wh.fan_out.routes:
        first_event = wh.fan_out.routes[0].match

    for key, val_template in reg.register_body_extra.items():
        if "${route_event}" in val_template:
            body[key] = first_event or "<event_type>"
        elif val_template.startswith("${credential:"):
            cred_name = val_template[len("${credential:") :].rstrip("}")
            body[key] = f"<{cred_name}>"
        else:
            body[key] = val_template

    # Add the callback URL field last.
    webhook_path = wh.path if wh else "/webhooks/<connector>"
    body[reg.callback_url_runtime_param] = f"http://engine:9090{webhook_path}"

    return body


def _add_webhook_registration_routes(
    connector: ConnectorConfig,
    _add,
    registered: set[tuple[str, str]],
    webhook_subscriptions_store: dict | None = None,
    event_bus=None,
) -> None:
    """Register POST/DELETE/PUT/GET routes for the webhook lifecycle API."""
    import itertools

    reg = connector.webhooks.registration  # type: ignore[union-attr]
    id_path = reg.id_response_path  # e.g. "value.id"

    # Use the shared store when available so the UI and dispatcher can see
    # active subscriptions; fall back to a process-local dict.
    if webhook_subscriptions_store is not None:
        webhook_subscriptions_store.setdefault(connector.name, {})
        subscriptions: dict[int, dict] = webhook_subscriptions_store[connector.name]
    else:
        subscriptions = {}
    counter = itertools.count(1)

    # Build example body once — shown in Swagger so callers can see the expected shape.
    example_body = _build_registration_example(connector)
    id_example: dict = {}
    _set_path(id_example, id_path, 42)
    register_openapi_extra = {
        "summary": "Register webhook subscription",
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "register": {
                            "summary": "Registration payload",
                            "value": example_body,
                        }
                    }
                }
            }
        },
        "responses": {
            "200": {
                "content": {
                    "application/json": {
                        "examples": {
                            "created": {
                                "summary": "Subscription created",
                                "value": id_example,
                            }
                        }
                    }
                }
            }
        },
    }

    # ------------------------------------------------------------------
    # POST {register_path} — accept a new subscription, return the ID
    # ------------------------------------------------------------------
    register_fa_path = reg.register_path

    def _make_register():
        async def _register(request: Request) -> JSONResponse:
            import time

            t0 = time.monotonic()
            body = await request.json()
            sub_id = next(counter)
            subscriptions[sub_id] = {**body, "__id": sub_id, "__active": True}
            resp: dict = {}
            _set_path(resp, id_path, sub_id)
            elapsed = int((time.monotonic() - t0) * 1000)
            if event_bus is not None:
                event_bus.publish_request(
                    connector=connector.name,
                    datatype="webhook_subscription",
                    method="POST",
                    path=reg.register_path,
                    status=200,
                    duration_ms=elapsed,
                    request_body_json=json.dumps(body),
                    request_headers_json=json.dumps(dict(request.headers)),
                    record_id=str(sub_id),
                )
            return JSONResponse(resp, status_code=200)

        _register.__name__ = f"webhook_register_{connector.name}"
        return _register

    _add(
        register_fa_path,
        ["POST"],
        _make_register(),
        openapi_extra=register_openapi_extra,
        summary="Register webhook subscription",
    )

    # ------------------------------------------------------------------
    # DELETE {deregister_path} — remove a subscription
    # ------------------------------------------------------------------
    if reg.deregister_path:
        dereg_fa = _wh_path_to_fa(reg.deregister_path)

        def _make_deregister():
            async def _deregister(request: Request, webhook_id: int) -> JSONResponse:
                import time

                t0 = time.monotonic()
                subscriptions.pop(webhook_id, None)
                elapsed = int((time.monotonic() - t0) * 1000)
                if event_bus is not None:
                    event_bus.publish_request(
                        connector=connector.name,
                        datatype="webhook_subscription",
                        method="DELETE",
                        path=reg.deregister_path.replace("{webhook_id}", str(webhook_id)),
                        status=200,
                        duration_ms=elapsed,
                        record_id=str(webhook_id),
                    )
                return JSONResponse({}, status_code=200)

            _deregister.__name__ = f"webhook_deregister_{connector.name}"
            return _deregister

        _add(dereg_fa, ["DELETE"], _make_deregister(), summary="Deregister webhook subscription")

    # ------------------------------------------------------------------
    # PUT {renew_path} — renew / heartbeat (no-op, always 200)
    # ------------------------------------------------------------------
    if reg.renew_path:
        renew_fa = _wh_path_to_fa(reg.renew_path)

        def _make_renew():
            async def _renew(request: Request, webhook_id: int) -> JSONResponse:
                import time

                t0 = time.monotonic()
                if webhook_id in subscriptions:
                    subscriptions[webhook_id]["__active"] = True
                elapsed = int((time.monotonic() - t0) * 1000)
                if event_bus is not None:
                    event_bus.publish_request(
                        connector=connector.name,
                        datatype="webhook_subscription",
                        method="PUT",
                        path=reg.renew_path.replace("{webhook_id}", str(webhook_id)),
                        status=200,
                        duration_ms=elapsed,
                    )
                return JSONResponse({}, status_code=200)

            _renew.__name__ = f"webhook_renew_{connector.name}"
            return _renew

        _add(renew_fa, ["PUT"], _make_renew(), summary="Renew webhook subscription")

    # ------------------------------------------------------------------
    # GET {health_check_path} — verify subscription still exists
    # ------------------------------------------------------------------
    if reg.health_check_path:
        health_fa = _wh_path_to_fa(reg.health_check_path)

        def _make_health():
            async def _health(request: Request, webhook_id: int) -> JSONResponse:
                import time

                t0 = time.monotonic()
                if webhook_id not in subscriptions:
                    if event_bus is not None:
                        event_bus.publish_request(
                            connector=connector.name,
                            datatype="webhook_subscription",
                            method="GET",
                            path=reg.health_check_path.replace("{webhook_id}", str(webhook_id)),
                            status=404,
                            duration_ms=int((time.monotonic() - t0) * 1000),
                        )
                    return JSONResponse({"error": "not found"}, status_code=404)
                resp: dict = {}
                _set_path(resp, id_path, webhook_id)
                elapsed = int((time.monotonic() - t0) * 1000)
                if event_bus is not None:
                    event_bus.publish_request(
                        connector=connector.name,
                        datatype="webhook_subscription",
                        method="GET",
                        path=reg.health_check_path.replace("{webhook_id}", str(webhook_id)),
                        status=200,
                        duration_ms=elapsed,
                    )
                return JSONResponse(resp, status_code=200)

            _health.__name__ = f"webhook_health_{connector.name}"
            return _health

        _add(health_fa, ["GET"], _make_health(), summary="Webhook subscription health check")
