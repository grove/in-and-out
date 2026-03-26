"""UI page routes and admin CRUD API for the demo simulator."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

from inandout.simulator.events import SimulatorEvent
from inandout.simulator.store import RecordStore
from inandout.simulator.ui.sse import sse_endpoint

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def build_ui_router() -> APIRouter:
    router = APIRouter()

    # ------------------------------------------------------------------
    # SSE stream
    # ------------------------------------------------------------------
    router.add_api_route("/events", sse_endpoint, methods=["GET"])

    # ------------------------------------------------------------------
    # UI pages
    # ------------------------------------------------------------------

    @router.get("/ui", response_class=HTMLResponse)
    @router.get("/ui/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        connectors = request.app.state.connectors
        store: RecordStore = request.app.state.store
        counts: dict[str, dict[str, int]] = {}
        for conn in connectors:
            counts[conn.name] = {}
            for dt_name in conn.datatypes:
                counts[conn.name][dt_name] = await store.count(conn.name, dt_name)
        recent = request.app.state.event_bus.recent(limit=40)
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "connectors": connectors,
                "counts": counts,
                "recent_events": recent,
            },
        )

    @router.get("/ui/{connector_name}/{datatype}", response_class=HTMLResponse)
    async def table_view(request: Request, connector_name: str, datatype: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return HTMLResponse("<h1>Not Found</h1>", status_code=404)
        store: RecordStore = request.app.state.store
        dt_cfg = connector.datatypes[datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        records = await store.list_all(connector_name, datatype, include_deleted=True)
        live_count = await store.count(connector_name, datatype)
        return templates.TemplateResponse(
            request,
            "table.html",
            {
                "connector": connector,
                "datatype": datatype,
                "dt_cfg": dt_cfg,
                "pk_field": pk_field,
                "records": records,
                "live_count": live_count,
            },
        )

    # ------------------------------------------------------------------
    # Partial HTMX fragments — must be registered BEFORE {record_id} wildcard
    # ------------------------------------------------------------------

    @router.get("/ui/{connector_name}/{datatype}/_rows", response_class=HTMLResponse)
    async def table_rows_fragment(request: Request, connector_name: str, datatype: str):
        """Return all table rows (live + deleted) for HTMX refresh."""
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return HTMLResponse("")
        store: RecordStore = request.app.state.store
        dt_cfg = connector.datatypes[datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        records = await store.list_all(connector_name, datatype, include_deleted=True)
        columns = _columns_from_cfg(dt_cfg)
        if columns is None and records:
            first = next((r for r in records if "__deleted_at__" not in r), records[0])
            columns = [k for k in first if not k.startswith("__")]
        rows = "".join(_row_html(connector_name, datatype, pk_field, r, columns) for r in records)
        return HTMLResponse(rows)

    @router.get("/ui/{connector_name}/{datatype}/_count")
    async def table_count_fragment(request: Request, connector_name: str, datatype: str):
        """Return live (non-deleted) record count as plain text."""
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return Response("0", media_type="text/plain")
        store: RecordStore = request.app.state.store
        count = await store.count(connector_name, datatype)
        return Response(str(count), media_type="text/plain")

    @router.get(
        "/ui/{connector_name}/{datatype}/{record_id}/_mutations", response_class=HTMLResponse
    )
    async def record_mutations_fragment(
        request: Request, connector_name: str, datatype: str, record_id: str
    ):
        """Return only the mutation history list for live refresh."""
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return HTMLResponse("")
        store: RecordStore = request.app.state.store
        mutations = await store.recent_mutations(connector_name, datatype, limit=20)
        mutations = [m for m in mutations if m.record_id == record_id]
        return HTMLResponse(_mutations_html(mutations))

    @router.get("/ui/{connector_name}/{datatype}/{record_id}", response_class=HTMLResponse)
    async def record_view(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return HTMLResponse("<h1>Not Found</h1>", status_code=404)
        store: RecordStore = request.app.state.store
        record = await store.get_by_id(connector_name, datatype, record_id)
        if record is None:
            return HTMLResponse("<h1>Record Not Found</h1>", status_code=404)
        display = {k: v for k, v in record.items() if k != "__deleted_at__"}
        return templates.TemplateResponse(
            request,
            "record.html",
            {
                "connector": connector,
                "datatype": datatype,
                "record_id": record_id,
                "record": record,
                "record_json": json.dumps(display, indent=2),
                "etag": record.get("__modified_at__", ""),
            },
        )

    # ------------------------------------------------------------------
    # Admin CRUD API (called by the UI's HTMX actions)
    # ------------------------------------------------------------------

    @router.get("/admin/{connector_name}/{datatype}/{record_id}")
    async def admin_get(request: Request, connector_name: str, datatype: str, record_id: str):
        """Return the current record as JSON with an ETag header for optimistic locking."""
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return Response(status_code=404)
        store: RecordStore = request.app.state.store
        record = await store.get_by_id(connector_name, datatype, record_id)
        if record is None:
            return Response(status_code=404)
        etag = record.get("__modified_at__", "")
        display = {k: v for k, v in record.items() if not k.startswith("__")}
        return JSONResponse(display, headers={"ETag": etag})

    @router.post("/admin/{connector_name}/{datatype}", response_class=HTMLResponse)
    async def admin_create(request: Request, connector_name: str, datatype: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return Response(status_code=404)
        store: RecordStore = request.app.state.store
        try:
            body = await request.json()
        except Exception:
            body = {}
        dt_cfg = connector.datatypes[datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        record = await store.create(connector_name, datatype, body, pk_field=pk_field, source="ui")
        evs = await store.recent_mutations(connector_name, datatype, 1)
        if evs:
            request.app.state.event_bus.publish_mutation(evs[0])
        disp = request.app.state.dispatcher
        await disp.dispatch(connector, datatype, "create", str(record.get(pk_field, "")), record)
        columns = _columns_from_cfg(dt_cfg)
        return HTMLResponse(_row_html(connector_name, datatype, pk_field, record, columns), status_code=201)

    @router.put("/admin/{connector_name}/{datatype}/{record_id}", response_class=HTMLResponse)
    async def admin_update(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return Response(status_code=404)
        store: RecordStore = request.app.state.store
        # Optimistic locking: reject the update if the caller's version is stale.
        if_match = request.headers.get("if-match", "")
        if if_match:
            current = await store.get_by_id(connector_name, datatype, record_id)
            if current and current.get("__modified_at__", "") != if_match:
                return JSONResponse(
                    {"detail": "Conflict: record was modified since you last loaded it."},
                    status_code=412,
                )
        try:
            body = await request.json()
        except Exception:
            body = {}
        dt_cfg = connector.datatypes[datatype]
        cursor_field: str | None = None
        if dt_cfg.ingestion and dt_cfg.ingestion.list.incremental:
            cursor_field = dt_cfg.ingestion.list.incremental.cursor_field
        if cursor_field:
            from datetime import datetime, timezone

            body[cursor_field] = datetime.now(timezone.utc).isoformat()
        updated = await store.update(connector_name, datatype, record_id, body, source="ui")
        if updated is None:
            return Response(status_code=404)
        evs = await store.recent_mutations(connector_name, datatype, 1)
        if evs:
            request.app.state.event_bus.publish_mutation(evs[0])
        disp = request.app.state.dispatcher
        await disp.dispatch(connector, datatype, "update", record_id, updated)
        pk_field = _pk_from_cfg(dt_cfg)
        # Refetch so the record dict contains __modified_at__ / __created_at__ meta-keys
        # (store.update returns the raw merged data without them).
        hydrated = await store.get_by_id(connector_name, datatype, record_id) or updated
        display = {k: v for k, v in hydrated.items() if not k.startswith("__")}
        return HTMLResponse(
            _row_html(connector_name, datatype, pk_field, hydrated),
            headers={
                "X-Record-Json": json.dumps(display),
                "ETag": hydrated.get("__modified_at__", ""),
            },
        )

    @router.delete("/admin/{connector_name}/{datatype}/{record_id}")
    async def admin_delete(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return Response(status_code=404)
        store: RecordStore = request.app.state.store
        deleted = await store.delete(connector_name, datatype, record_id, source="ui")
        if not deleted:
            return Response(status_code=404)
        evs = await store.recent_mutations(connector_name, datatype, 1)
        if evs:
            request.app.state.event_bus.publish_mutation(evs[0])
        disp = request.app.state.dispatcher
        await disp.dispatch(connector, datatype, "delete", record_id, None)
        # Return a struck-through row with a Restore button instead of removing.
        dt_cfg = connector.datatypes[datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        record = await store.get_by_id(connector_name, datatype, record_id)
        return HTMLResponse(_row_html(connector_name, datatype, pk_field, record or {}))

    @router.post(
        "/admin/{connector_name}/{datatype}/{record_id}/restore", response_class=HTMLResponse
    )
    async def admin_restore(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c.name == connector_name), None)
        if connector is None or datatype not in connector.datatypes:
            return Response(status_code=404)
        store: RecordStore = request.app.state.store
        restored = await store.restore(connector_name, datatype, record_id, source="ui")
        if restored is None:
            return Response(status_code=404)
        evs = await store.recent_mutations(connector_name, datatype, 1)
        if evs:
            request.app.state.event_bus.publish_mutation(evs[0])
        disp = request.app.state.dispatcher
        await disp.dispatch(connector, datatype, "create", record_id, restored)
        dt_cfg = connector.datatypes[datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        return HTMLResponse(_row_html(connector_name, datatype, pk_field, restored))

    return router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pk_from_cfg(dt_cfg) -> str:
    pk = (dt_cfg.ingestion.primary_key if dt_cfg.ingestion else None) or "id"
    if isinstance(pk, str):
        return pk
    if isinstance(pk, list) and pk:
        return pk[0]
    return "id"


def _columns_from_cfg(dt_cfg) -> list[str] | None:
    """Return column names derived from seed data, or None if no seed data."""
    if dt_cfg.seed_data:
        return [k for k in dt_cfg.seed_data[0] if not k.startswith("__")]
    return None


def _mutations_html(mutations: list) -> str:
    import html as _html

    if not mutations:
        return '<p class="text-sm text-slate-400 italic">No recorded mutations yet.</p>'
    colours = {
        "create": "border-green-500 text-green-400",
        "update": "border-blue-500 text-blue-400",
        "delete": "border-red-500 text-red-400",
    }
    items = []
    for m in mutations:
        cls = colours.get(m.operation, "border-slate-500 text-slate-400")
        if m.source == "ui":
            src = "&#x1F9D1; You"
        elif m.source == "seed":
            src = "&#x1F331; seed"
        else:
            src = "&#x2699; Engine"

        diff_html = ""
        if m.operation == "update" and m.before and m.record:
            diff_rows = []
            all_keys = dict.fromkeys(list(m.before.keys()) + list(m.record.keys()))
            for k in all_keys:
                old_v = m.before.get(k)
                new_v = m.record.get(k)
                if old_v == new_v:
                    continue
                old_s = _html.escape(json.dumps(old_v) if old_v is not None else "—")
                new_s = _html.escape(json.dumps(new_v) if new_v is not None else "—")
                diff_rows.append(
                    f"<tr>"
                    f'<td class="pr-2 text-slate-400 font-mono">{_html.escape(k)}</td>'
                    f'<td class="pr-2 line-through text-red-400 font-mono">{old_s}</td>'
                    f'<td class="text-green-400 font-mono">{new_s}</td>'
                    f"</tr>"
                )
            if diff_rows:
                diff_html = '<table class="mt-2 text-xs w-full">' + "".join(diff_rows) + "</table>"
        elif m.operation == "delete" and m.before:
            snapshot = _html.escape(json.dumps(m.before, indent=2))
            diff_html = (
                f'<pre class="mt-2 text-xs text-red-300 bg-slate-900 rounded p-2'
                f' overflow-x-auto whitespace-pre-wrap line-through opacity-60">{snapshot}</pre>'
            )
        elif m.operation == "create" and m.record:
            snapshot = _html.escape(json.dumps(m.record, indent=2))
            diff_html = (
                f'<pre class="mt-2 text-xs text-green-300 bg-slate-900 rounded p-2'
                f' overflow-x-auto whitespace-pre-wrap">{snapshot}</pre>'
            )

        items.append(
            f'<li class="border-l-2 pl-3 {cls}">'
            f'<div class="flex items-center justify-between">'
            f'<span class="text-sm font-medium capitalize">{m.operation}</span>'
            f'<span class="text-xs text-slate-500">{src}</span>'
            f"</div>"
            f'<div class="text-xs text-slate-500 font-mono mt-0.5">{m.timestamp}</div>'
            + diff_html
            + f"</li>"
        )
    return '<ul class="flex flex-col gap-2">' + "".join(items) + "</ul>"


def _row_html(
    connector_name: str,
    datatype: str,
    pk_field: str,
    record: dict,
    columns: list[str] | None = None,
) -> str:
    rid = str(record.get(pk_field, ""))
    is_deleted = "__deleted_at__" in record
    modified_at = record.get("__modified_at__", "")
    created_at = record.get("__created_at__", "")
    was_updated = modified_at and created_at and modified_at != created_at
    display = {k: v for k, v in record.items() if not k.startswith("__")}
    col_list = columns if columns is not None else list(display.keys())
    row_cls = (
        "border-b border-slate-700 opacity-50 bg-red-950/20"
        if is_deleted
        else "border-b border-slate-700 hover:bg-slate-750 transition-colors"
    )
    cells = ""
    for k in col_list:
        v = display.get(k, "")
        val = str(v)
        if len(val) > 60:
            val = val[:57] + "…"
        td_cls = "px-3 py-2 text-sm text-slate-300 max-w-xs truncate" + (
            " line-through" if is_deleted else ""
        )
        cells += f'<td class="{td_cls}">{val}</td>'
    if is_deleted:
        action = (
            f"<button "
            f'  hx-post="/admin/{connector_name}/{datatype}/{rid}/restore" '
            f'  hx-target="closest tr" hx-swap="outerHTML" '
            f'  class="text-green-400 hover:text-green-300">Restore</button>'
        )
    else:
        action = (
            f'<a href="/ui/{connector_name}/{datatype}/{rid}" '
            f'   class="text-sky-400 hover:text-sky-300 mr-3">View</a>'
            f"<button "
            f'  hx-delete="/admin/{connector_name}/{datatype}/{rid}" '
            f'  hx-target="closest tr" hx-swap="outerHTML" '
            f'  class="text-red-400 hover:text-red-300">Delete</button>'
            + (
                f'<span id="recent-row-{rid}" data-ts="{modified_at}"'
                f' class="ml-2 text-xs font-medium"></span>'
                if was_updated
                else ""
            )
        )
    return (
        f'<tr id="row-{rid}" class="{row_cls}">'
        f"{cells}"
        f'<td class="px-3 py-2 text-sm whitespace-nowrap">{action}</td>'
        f"</tr>"
    )
