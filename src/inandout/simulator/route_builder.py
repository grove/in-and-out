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
    if not seed:
        return {}

    first = seed[0]
    first_id = str(first.get(pk_field, "1"))
    # Body sent to the API never includes the pk (server owns it).
    body_fields = {k: v for k, v in first.items() if k != pk_field}

    extra: dict = {}

    # Pagination + incremental filter query params for the list endpoint
    if action == "list":
        params: list[dict] = []
        if filter_param:
            params.append({
                "name": filter_param,
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": "Incremental filter: return records modified after this watermark",
                "example": "2026-01-01T00:00:00Z",
            })
        if pagination is not None:
            strategy = pagination.strategy
            if strategy == PaginationStrategy.cursor and pagination.cursor:
                params.append({
                    "name": pagination.cursor.request_param,
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": "Cursor token for the next page (from previous response)",
                })
            elif strategy == PaginationStrategy.offset:
                off_cfg = pagination.offset or {}
                off_p = off_cfg.get("param", "offset") if isinstance(off_cfg, dict) else "offset"
                lim_p = off_cfg.get("limit_param", "limit") if isinstance(off_cfg, dict) else "limit"
                params.append({"name": off_p, "in": "query", "required": False, "schema": {"type": "integer", "default": 0}, "description": "Offset (number of records to skip)"})
                params.append({"name": lim_p, "in": "query", "required": False, "schema": {"type": "integer", "default": 20}, "description": "Maximum records per page"})
            elif strategy == PaginationStrategy.page_number:
                pn_cfg = pagination.page_number or {}
                page_p = pn_cfg.get("page_param", "page") if isinstance(pn_cfg, dict) else "page"
                pp_p = pn_cfg.get("per_page_param", "per_page") if isinstance(pn_cfg, dict) else "per_page"
                params.append({"name": page_p, "in": "query", "required": False, "schema": {"type": "integer", "default": 1}, "description": "Page number (1-based)"})
                params.append({"name": pp_p, "in": "query", "required": False, "schema": {"type": "integer", "default": 20}, "description": "Records per page"})
            elif strategy == PaginationStrategy.keyset and pagination.keyset:
                ks = pagination.keyset
                params.append({"name": ks.request_param, "in": "query", "required": False, "schema": {"type": "string"}, "description": f"Keyset cursor: return records with {ks.keyset_field} > this value"})
                params.append({"name": ks.page_size_param, "in": "query", "required": False, "schema": {"type": "integer", "default": ks.page_size}, "description": "Page size"})
        if params:
            extra["parameters"] = params

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
) -> APIRouter:
    """Return a FastAPI ``APIRouter`` with all routes for *connector*.

    Mount this router at ``prefix="/{connector.name}"`` in the main app.
    """
    router = APIRouter()
    # Track (method, path) pairs to avoid duplicate registrations.
    registered: set[tuple[str, str]] = set()

    def _add(path: str, methods: list[str], handler, openapi_extra: dict | None = None) -> None:
        for m in methods:
            key = (m.upper(), path)
            if key not in registered:
                registered.add(key)
                router.add_api_route(
                    path, handler, methods=[m], openapi_extra=openapi_extra or None
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

        _add(token_url_path, ["POST"], _make_token_handler())

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
                    )
                    return JSONResponse(body, headers=headers_out)

                list_endpoint.__name__ = f"list_{connector_name}_{dt_name}"
                return list_endpoint

            _add(
                list_path,
                ["GET"],
                _make_list_handler(),
                openapi_extra=_build_openapi_extra(
                    "list",
                    dt_cfg.seed_data,
                    pk_field,
                    selector=record_selector,
                    pagination=pagination,
                    filter_param=filter_param,
                ),
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
                    _seed=dt_cfg.seed_data,
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
                                connector_name, _dt_name, "POST", str(request.url.path), 201, ms
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
                                connector_name, _dt_name, _method, str(request.url.path), 404, ms
                            )
                            return JSONResponse({"error": "not found"}, status_code=404)
                        dispatcher.dispatch_nowait(connector, _dt_name, "update", rid, updated)
                        evs = await store.recent_mutations(connector_name, _dt_name, 1)
                        if evs:
                            event_bus.publish_mutation(evs[0])
                        ms = int((time.monotonic() - t0) * 1000)
                        event_bus.publish_request(
                            connector_name, _dt_name, _method, str(request.url.path), 200, ms
                        )
                        display = {k: v for k, v in updated.items() if not k.startswith("__")}
                        return JSONResponse(display)

                    _extra = _build_openapi_extra(_action, _seed, _pk)

                    if _has_id:

                        async def _ep_with_id(request: Request, record_id: str) -> Response:
                            return await write_endpoint(request, record_id)

                        _ep_with_id.__name__ = f"{_action}_{connector_name}_{_dt_name}"
                        _add(_fa_path, [_method], _ep_with_id, openapi_extra=_extra)
                    else:
                        # No {record_id} in the path — register a wrapper WITHOUT the
                        # record_id parameter so FastAPI does not expose it as a query
                        # parameter in the Swagger UI.
                        async def _ep_no_id(request: Request) -> Response:
                            return await write_endpoint(request, None)

                        _ep_no_id.__name__ = f"{_action}_{connector_name}_{_dt_name}"
                        _add(_fa_path, [_method], _ep_no_id, openapi_extra=_extra)

                _make_write_handler()

    return router
