"""UI page routes and admin CRUD API for the demo simulator."""

from __future__ import annotations

import json
from html import escape as html_escape
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

from inandout.simulator.events import SimulatorEvent
from inandout.simulator.store import RecordStore
from inandout.simulator.ui.sse import sse_endpoint

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _pretty_json(value: str) -> str:
    """Jinja2 filter: parse a JSON string and re-format it with indentation."""
    if not value:
        return ""
    try:
        return json.dumps(json.loads(value), indent=2)
    except (ValueError, TypeError):
        return value


templates.env.filters["pretty_json"] = _pretty_json


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
            counts[conn["name"]] = {}
            for dt_name in conn["datatypes"]:
                counts[conn["name"]][dt_name] = await store.count(conn["name"], dt_name)
        recent = request.app.state.event_bus.recent(limit=40)
        connector_systems = {conn["name"]: conn["system"] for conn in connectors}
        webhook_subscriptions = getattr(request.app.state, "webhook_subscriptions", {})
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "connectors": connectors,
                "counts": counts,
                "recent_events": recent,
                "connector_systems": connector_systems,
                "webhook_subscriptions": webhook_subscriptions,
            },
        )

    @router.get("/ui/_requests", response_class=HTMLResponse)
    async def request_log_view(request: Request):
        connectors = request.app.state.connectors
        counts: dict[str, dict[str, int]] = {}
        store: RecordStore = request.app.state.store
        for conn in connectors:
            counts[conn["name"]] = {}
            for dt_name in conn["datatypes"]:
                counts[conn["name"]][dt_name] = await store.count(conn["name"], dt_name)
        connector_systems = {conn["name"]: conn["system"] for conn in connectors}
        all_events = request.app.state.event_bus.recent(limit=200)
        request_events = [e for e in all_events if e.event_type == "request"]
        return templates.TemplateResponse(
            request,
            "requests.html",
            {
                "connectors": connectors,
                "counts": counts,
                "connector_systems": connector_systems,
                "events": request_events,
            },
        )

    # ------------------------------------------------------------------
    # Registration management API (called by the UI via fetch)
    # ------------------------------------------------------------------

    @router.get("/ui/_registrations", response_class=HTMLResponse)
    async def registrations_view(request: Request):
        connectors = request.app.state.connectors
        counts: dict[str, dict[str, int]] = {}
        store: RecordStore = request.app.state.store
        for conn in connectors:
            counts[conn["name"]] = {}
            for dt_name in conn["datatypes"]:
                counts[conn["name"]][dt_name] = await store.count(conn["name"], dt_name)
        connector_systems = {conn["name"]: conn["system"] for conn in connectors}
        webhook_subscriptions = getattr(request.app.state, "webhook_subscriptions", {})
        return templates.TemplateResponse(
            request,
            "registrations.html",
            {
                "connectors": connectors,
                "counts": counts,
                "connector_systems": connector_systems,
                "webhook_subscriptions": webhook_subscriptions,
            },
        )

    @router.delete("/ui/_registrations/{connector_name}/{sub_id}")
    async def delete_registration(request: Request, connector_name: str, sub_id: int):
        """Delete a single webhook subscription from the shared store."""
        subs: dict = getattr(request.app.state, "webhook_subscriptions", {})
        connector_subs = subs.get(connector_name)
        if connector_subs is None:
            return Response(status_code=404)
        connector_subs.pop(sub_id, None)
        return Response(status_code=204)

    @router.delete("/ui/_registrations/{connector_name}")
    async def delete_all_registrations(request: Request, connector_name: str):
        """Delete all webhook subscriptions for a connector."""
        subs: dict = getattr(request.app.state, "webhook_subscriptions", {})
        if connector_name in subs:
            subs[connector_name].clear()
        return Response(status_code=204)

    @router.get("/ui/_webhooks", response_class=HTMLResponse)
    async def webhook_log_view(request: Request):
        connectors = request.app.state.connectors
        counts: dict[str, dict[str, int]] = {}
        store: RecordStore = request.app.state.store
        for conn in connectors:
            counts[conn["name"]] = {}
            for dt_name in conn["datatypes"]:
                counts[conn["name"]][dt_name] = await store.count(conn["name"], dt_name)
        connector_systems = {conn["name"]: conn["system"] for conn in connectors}
        all_events = request.app.state.event_bus.recent(limit=200)
        webhook_events = [e for e in all_events if e.event_type == "webhook"]
        webhook_subscriptions = getattr(request.app.state, "webhook_subscriptions", {})
        return templates.TemplateResponse(
            request,
            "webhooks.html",
            {
                "connectors": connectors,
                "counts": counts,
                "connector_systems": connector_systems,
                "events": webhook_events,
                "webhook_subscriptions": webhook_subscriptions,
            },
        )

    @router.get("/ui/{connector_name}/{datatype}", response_class=HTMLResponse)
    async def table_view(request: Request, connector_name: str, datatype: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
            return HTMLResponse("<h1>Not Found</h1>", status_code=404)
        store: RecordStore = request.app.state.store
        dt_cfg = connector["datatypes"][datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        records = await store.list_all(connector_name, datatype, include_deleted=True)
        live_count = await store.count(connector_name, datatype)
        counts: dict[str, dict[str, int]] = {}
        for conn in connectors:
            counts[conn["name"]] = {}
            for dt_name in conn["datatypes"]:
                counts[conn["name"]][dt_name] = await store.count(conn["name"], dt_name)
        return templates.TemplateResponse(
            request,
            "table.html",
            {
                "connector": connector,
                "connectors": connectors,
                "datatype": datatype,
                "dt_cfg": dt_cfg,
                "pk_field": pk_field,
                "records": records,
                "live_count": live_count,
                "counts": counts,
            },
        )

    # ------------------------------------------------------------------
    # Partial HTMX fragments — must be registered BEFORE {record_id} wildcard
    # ------------------------------------------------------------------

    @router.get("/ui/{connector_name}/{datatype}/_rows", response_class=HTMLResponse)
    async def table_rows_fragment(request: Request, connector_name: str, datatype: str):
        """Return all table rows (live + deleted) for HTMX refresh."""
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
            return HTMLResponse("")
        store: RecordStore = request.app.state.store
        dt_cfg = connector["datatypes"][datatype]
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
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
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
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
            return HTMLResponse("")
        store: RecordStore = request.app.state.store
        mutations = await store.recent_mutations(connector_name, datatype, limit=20)
        mutations = [m for m in mutations if m.record_id == record_id]
        return HTMLResponse(_mutations_html(mutations))

    @router.get("/ui/{connector_name}/{datatype}/{record_id}/_panel", response_class=HTMLResponse)
    async def record_panel_fragment(
        request: Request, connector_name: str, datatype: str, record_id: str
    ):
        """Compact record view for the side panel on the table page."""
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
            return HTMLResponse('<p class="text-red-400 text-sm">Not found.</p>', status_code=404)
        store: RecordStore = request.app.state.store
        record = await store.get_by_id(connector_name, datatype, record_id)
        if record is None:
            return HTMLResponse(
                '<p class="text-red-400 text-sm">Record not found.</p>', status_code=404
            )
        display = {k: v for k, v in record.items() if not k.startswith("__")}
        record_json = json.dumps(display, indent=2)
        etag = record.get("__modified_at__", "")
        mutations = await store.recent_mutations(connector_name, datatype, limit=20)
        mutations = [m for m in mutations if m.record_id == record_id]
        mut_html = _mutations_html(mutations)
        is_deleted = "__deleted_at__" in record
        deleted_html = ""
        if is_deleted:
            deleted_html = (
                '<div class="mb-3 bg-red-900/40 border border-red-700 rounded-lg p-3 flex items-center justify-between gap-3">'
                '<p class="text-red-300 text-xs font-semibold">Soft-deleted</p>'
                f'<button hx-post="/admin/{connector_name}/{datatype}/{record_id}/restore"'
                f" hx-on::after-request=\"document.getElementById('record-panel').classList.add('hidden')\""
                ' class="bg-green-700 hover:bg-green-600 text-white text-xs px-3 py-1 rounded">Restore</button>'
                "</div>"
            )
        full_link = f'<a href="/ui/{connector_name}/{datatype}/{record_id}" class="text-xs text-sky-400 hover:underline">Open full page ↗</a>'
        edit_disabled = 'style="opacity:.4;pointer-events:none"' if is_deleted else ""
        html = f"""
{deleted_html}
<div class="flex flex-col gap-3 h-full" style="min-height:0">

  <!-- Stale-data warning banner (shown when an external mutation arrives) -->
  <div id="panel-conflict-banner-{record_id}"
       class="hidden shrink-0 bg-amber-900/50 border border-amber-600 rounded-lg px-3 py-2
              flex items-center justify-between gap-3">
    <span class="text-amber-300 text-xs">&#x26A0; This record was changed externally. Your edits may conflict.</span>
    <button class="text-amber-400 hover:text-amber-200 text-xs underline shrink-0"
            onclick="reloadPanelFragment('{connector_name}','{datatype}','{record_id}')">Reload</button>
  </div>

  <!-- JSON editor -->
  <div class="flex flex-col gap-1 flex-1 min-h-0" {edit_disabled}>
    <div class="flex items-center justify-between shrink-0">
      <span class="text-xs font-semibold text-slate-400 uppercase tracking-wide">Edit</span>
      {full_link}
    </div>
    <textarea id="panel-edit-json-{record_id}"
      class="flex-1 w-full bg-slate-900 text-slate-100 font-mono text-xs rounded-lg p-3
             border border-slate-600 focus:border-sky-500 focus:outline-none resize-none min-h-0"
      >{html_escape(record_json)}</textarea>
    <div class="flex items-center gap-2 shrink-0">
      <button onclick="submitPanelEdit('{connector_name}','{datatype}','{record_id}')"
        class="bg-sky-600 hover:bg-sky-500 text-white text-xs px-3 py-1.5 rounded">Save</button>
      <button hx-delete="/admin/{connector_name}/{datatype}/{record_id}"
        hx-on::after-request="document.getElementById('record-panel').classList.add('hidden')"
        onclick="event.stopPropagation()"
        class="bg-red-700 hover:bg-red-600 text-white text-xs px-3 py-1.5 rounded">Delete</button>
      <p id="panel-edit-status-{record_id}" class="text-xs hidden"></p>
    </div>
  </div>

  <!-- History -->
  <div class="shrink-0">
    <p class="text-xs font-semibold text-slate-400 uppercase tracking-wide mb-2">History</p>
    <div id="panel-mutations-{record_id}" class="max-h-56 overflow-y-auto">{mut_html}</div>
  </div>

</div>
<script>
(function(){{
  let _panelEtag = '{etag}';
  const _c='{connector_name}', _d='{datatype}', _id='{record_id}';
  async function submitPanelEdit(c,d,id){{
    const ta=document.getElementById('panel-edit-json-'+id);
    let body; try{{body=JSON.parse(ta.value);}}catch(e){{alert('Invalid JSON: '+e.message);return;}}
    const headers={{'Content-Type':'application/json','If-Match':_panelEtag}};
    const r=await fetch('/admin/'+c+'/'+d+'/'+id,{{method:'PUT',headers,body:JSON.stringify(body)}});
    const st=document.getElementById('panel-edit-status-'+id);
    if(r.status===412){{st.textContent='Conflict — reload panel.';st.className='text-xs text-amber-400';st.classList.remove('hidden');return;}}
    if(r.ok){{
      const newEtag=r.headers.get('etag')||'';
      if(newEtag)_panelEtag=newEtag;
      const jsonHeader=r.headers.get('x-record-json');
      if(jsonHeader)ta.value=JSON.stringify(JSON.parse(jsonHeader),null,2);
      // Replace the table row with the fresh HTML returned by the PUT response body.
      const rowHtml=await r.text();
      const rowEl=document.getElementById('row-'+id);
      if(rowEl&&rowHtml.trim().startsWith('<tr')){{
        const tmp=document.createElement('tbody');
        tmp.innerHTML=rowHtml;
        const newRow=tmp.firstElementChild;
        if(newRow){{
          if(rowEl.classList.contains('row-selected'))newRow.classList.add('row-selected');
          rowEl.replaceWith(newRow);
          if(typeof stampModifiedCells==='function')stampModifiedCells();
        }}
      }}
      const banner=document.getElementById('panel-conflict-banner-'+id);
      if(banner)banner.classList.add('hidden');
      st.textContent='Saved.';st.className='text-xs text-green-400';st.classList.remove('hidden');
      setTimeout(()=>st.classList.add('hidden'),2000);
    }}
  }}
  window.submitPanelEdit=submitPanelEdit;
  function reloadPanelFragment(c,d,id){{
    const body=document.getElementById('record-panel-body');
    if(!body)return;
    body.innerHTML='<p class="text-slate-400 text-sm animate-pulse">Loading...</p>';
    fetch('/ui/'+c+'/'+d+'/'+id+'/_panel')
      .then(function(r){{return r.ok?r.text():'<p class="text-red-400 text-sm">Failed.</p>';  }})
      .then(function(html){{
        body.innerHTML=html;
        body.querySelectorAll('script').forEach(function(s){{
          var ns=document.createElement('script');ns.textContent=s.textContent;
          document.head.appendChild(ns).parentNode.removeChild(ns);
        }});
        htmx.process(body);
      }});
  }}
  window.reloadPanelFragment=reloadPanelFragment;
  document.addEventListener('simulator:mutation',function(e){{
    if(!e.detail||e.detail.record_id!==_id)return;
    // Refresh history list
    fetch('/ui/'+_c+'/'+_d+'/'+_id+'/_mutations').then(r=>r.text()).then(h=>{{
      const el=document.getElementById('panel-mutations-'+_id);
      if(el)el.innerHTML=h;
    }});
    // Show conflict banner — the record changed while the editor is open
    const banner=document.getElementById('panel-conflict-banner-'+_id);
    if(banner)banner.classList.remove('hidden');
  }});
}})();
</script>"""
        return HTMLResponse(html)

    @router.get("/ui/{connector_name}/{datatype}/{record_id}", response_class=HTMLResponse)
    async def record_view(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
            return HTMLResponse("<h1>Not Found</h1>", status_code=404)
        store: RecordStore = request.app.state.store
        record = await store.get_by_id(connector_name, datatype, record_id)
        if record is None:
            return HTMLResponse("<h1>Record Not Found</h1>", status_code=404)
        display = {k: v for k, v in record.items() if k != "__deleted_at__"}
        counts: dict[str, dict[str, int]] = {}
        for conn in connectors:
            counts[conn["name"]] = {}
            for dt_name in conn["datatypes"]:
                counts[conn["name"]][dt_name] = await store.count(conn["name"], dt_name)
        return templates.TemplateResponse(
            request,
            "record.html",
            {
                "connector": connector,
                "connectors": connectors,
                "datatype": datatype,
                "record_id": record_id,
                "record": record,
                "counts": counts,
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
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
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
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
            return Response(status_code=404)
        store: RecordStore = request.app.state.store
        try:
            body = await request.json()
        except Exception:
            body = {}
        dt_cfg = connector["datatypes"][datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        record = await store.create(connector_name, datatype, body, pk_field=pk_field, source="ui")
        evs = await store.recent_mutations(connector_name, datatype, 1)
        if evs:
            request.app.state.event_bus.publish_mutation(evs[0])
        disp = request.app.state.dispatcher
        await disp.dispatch(connector, datatype, "create", str(record.get(pk_field, "")), record)
        columns = _columns_from_cfg(dt_cfg)
        return HTMLResponse(
            _row_html(connector_name, datatype, pk_field, record, columns), status_code=201
        )

    @router.put("/admin/{connector_name}/{datatype}/{record_id}", response_class=HTMLResponse)
    async def admin_update(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
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
        dt_cfg = connector["datatypes"][datatype]
        cursor_field: str | None = None
        inc = ((dt_cfg.get("ingestion") or {}).get("list") or {}).get("incremental")
        if inc and inc.get("enabled"):
            cursor_field = inc.get("cursor_field")
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
        wh = await disp.dispatch(connector, datatype, "update", record_id, updated)
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
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
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
        dt_cfg = connector["datatypes"][datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        record = await store.get_by_id(connector_name, datatype, record_id)
        return HTMLResponse(_row_html(connector_name, datatype, pk_field, record or {}))

    @router.post(
        "/admin/{connector_name}/{datatype}/{record_id}/restore", response_class=HTMLResponse
    )
    async def admin_restore(request: Request, connector_name: str, datatype: str, record_id: str):
        connectors = request.app.state.connectors
        connector = next((c for c in connectors if c["name"] == connector_name), None)
        if connector is None or datatype not in connector["datatypes"]:
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
        dt_cfg = connector["datatypes"][datatype]
        pk_field = _pk_from_cfg(dt_cfg)
        return HTMLResponse(_row_html(connector_name, datatype, pk_field, restored))

    return router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pk_from_cfg(dt_cfg) -> str:
    pk = ((dt_cfg.get("ingestion") or {}).get("primary_key")) if isinstance(dt_cfg, dict) else None
    if isinstance(pk, str):
        return pk
    if isinstance(pk, list) and pk:
        return pk[0]
    return "id"


def _columns_from_cfg(dt_cfg) -> list[str] | None:
    """Return column names derived from seed data, or None if no seed data."""
    seed = (dt_cfg.get("simulator") or {}).get("seed_data", []) if isinstance(dt_cfg, dict) else []
    if seed:
        return [k for k in seed[0] if not k.startswith("__")]
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
            src = "&#x1F9D1; User"
        elif m.source == "seed":
            src = "&#x1F331; seed"
        else:
            src = "&#x2699; API"

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
    display = {k: v for k, v in record.items() if not k.startswith("__")}
    col_list = columns if columns is not None else list(display.keys())
    row_cls = (
        "border-b border-slate-700 opacity-50 bg-red-950/20"
        if is_deleted
        else "border-b border-slate-700 hover:bg-slate-750 transition-colors"
    )
    # Checkbox cell (only for live records)
    if is_deleted:
        checkbox_cell = '<td class="px-2 py-2 w-7"></td>'
    else:
        checkbox_cell = (
            f'<td class="px-2 py-2 w-7" onclick="event.stopPropagation()">'
            f'<input type="checkbox" class="row-checkbox accent-sky-500 cursor-pointer" data-record-id="{rid}">'
            f"</td>"
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
    # Last modified column — always visible, shows relative time
    modified_cell = (
        f'<td class="px-3 py-2 text-xs text-slate-500 whitespace-nowrap" data-modified-at="{modified_at}">'
        f"{modified_at[:10] if modified_at else ''}"
        f"</td>"
    )
    # Restore button for deleted rows (no delete in list — moved to panel)
    if is_deleted:
        action_cell = (
            '<td class="px-3 py-2 text-sm whitespace-nowrap">'
            f'<button hx-post="/admin/{connector_name}/{datatype}/{rid}/restore"'
            f' hx-target="closest tr" hx-swap="outerHTML"'
            f' class="text-green-400 hover:text-green-300 text-xs">Restore</button>'
            "</td>"
        )
    else:
        action_cell = '<td class="px-3 py-2 w-4"></td>'
    row_cls_full = row_cls + (" cursor-pointer" if not is_deleted else "")
    data_attrs = (
        f'data-record-id="{rid}" data-connector="{connector_name}" data-datatype="{datatype}"'
        if not is_deleted
        else ""
    )
    return (
        f'<tr id="row-{rid}" class="{row_cls_full}" {data_attrs}>'
        f"{checkbox_cell}"
        f"{cells}"
        f"{modified_cell}"
        f"{action_cell}"
        f"</tr>"
    )
