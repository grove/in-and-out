"""FastAPI application factory for the demo simulator."""

from __future__ import annotations

from pathlib import Path
from typing import Union

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from inandout.config.connector import ConnectorConfig
from inandout.config.loader import load_connector
from inandout.simulator.events import EventBus
from inandout.simulator.route_builder import build_connector_router
from inandout.simulator.seed import seed_from_connector
from inandout.simulator.store import RecordStore
from inandout.simulator.store.memory import MemoryStore
from inandout.simulator.webhooks import WebhookDispatcher


def _make_store(store_dsn: str) -> RecordStore:
    if store_dsn == "memory" or store_dsn.startswith(":memory:"):
        return MemoryStore()
    if store_dsn.startswith("sqlite:///"):
        from inandout.simulator.store.sqlite import SQLiteStore

        path = store_dsn[len("sqlite:///") :]
        return SQLiteStore(path)
    raise ValueError(f"Unknown store DSN: {store_dsn!r}.  Use 'memory' or 'sqlite:///path.db'.")


# Injected into every connector Swagger page to make 4xx/5xx responses visually prominent.
_SWAGGER_ERROR_SCRIPT = """
<style>
  .sim-err-row { background: rgba(239,68,68,.10) !important; outline: 1px solid rgba(239,68,68,.4); }
  .sim-err-status { color: #f87171 !important; font-weight: 700 !important; font-size: 1.05em; }
</style>
<script>
(function () {
  'use strict';
  function _highlight() {
    document.querySelectorAll(
      '.swagger-ui table.responses-table td.response-col_status'
    ).forEach(function (el) {
      if (el.dataset.simHighlighted) return;
      var status = parseInt(el.textContent.trim(), 10);
      if (status >= 400) {
        el.dataset.simHighlighted = '1';
        el.classList.add('sim-err-status');
        var row = el.closest('tr');
        if (row) row.classList.add('sim-err-row');
        el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  }
  var obs = new MutationObserver(_highlight);
  document.addEventListener('DOMContentLoaded', function () {
    obs.observe(document.body, { subtree: true, childList: true });
  });
})();
</script>
"""


def _make_swagger_endpoint(connector_name: str, title: str):
    """Return a custom /docs handler that injects the error-highlight script."""
    async def swagger_docs(request: Request) -> HTMLResponse:
        root_path = request.scope.get("root_path", "").rstrip("/")
        html = get_swagger_ui_html(
            openapi_url=root_path + "/openapi.json",
            title=title,
            swagger_favicon_url="",
        )
        body = html.body.decode()
        body = body.replace("</body>", _SWAGGER_ERROR_SCRIPT + "\n</body>")
        return HTMLResponse(content=body)

    swagger_docs.__name__ = f"swagger_docs_{connector_name}"
    return swagger_docs


def create_app(
    connector_paths: list[Union[str, Path]],
    *,
    store_dsn: str = "memory",
    engine_url: str = "http://localhost:9090",
    page_size: int = 20,
) -> FastAPI:
    """Build and return the simulator FastAPI application.

    Args:
        connector_paths: Paths to one or more connector YAML files.
        store_dsn: ``'memory'`` or ``'sqlite:///path.db'``.
        engine_url: Base URL of the running ingest daemon (for webhook dispatch).
        page_size: Default page size for list endpoints.
    """
    store = _make_store(store_dsn)
    event_bus = EventBus()

    # Shared webhook subscription registry.  route_builder writes into this when
    # the engine POSTs to registration endpoints; WebhookDispatcher reads it so
    # it only fires outbound webhooks for per_route connectors once subscriptions exist.
    webhook_subscriptions: dict[str, dict[int, dict]] = {}

    dispatcher = WebhookDispatcher(
        engine_url=engine_url,
        webhook_subscriptions=webhook_subscriptions,
        event_bus=event_bus,
    )

    connector_configs: list[ConnectorConfig] = []
    for path in connector_paths:
        connector_configs.append(load_connector(path).connector)

    # Build a summary for the parent app description.
    connector_list = "\n".join(
        f"- **[{c.system}](/{c.name}/docs)** — `/{c.name}`" for c in connector_configs
    )

    app = FastAPI(
        title="in-and-out Demo Simulator",
        description=(
            "Fake API server for connector testing and demos.\n\n"
            "Each connector has its own Swagger UI:\n\n" + connector_list
        ),
        version="0.1.0",
    )

    # Keep connector configs accessible to the UI router.
    app.state.connectors = []
    app.state.store = store
    app.state.event_bus = event_bus
    app.state.dispatcher = dispatcher
    app.state.page_size = page_size
    app.state.webhook_subscriptions = webhook_subscriptions

    for connector in connector_configs:
        app.state.connectors.append(connector)

        description = connector.description or f"Simulated {connector.system} API."

        # Each connector gets its own sub-application so it has a dedicated
        # Swagger UI at /{connector.name}/docs with the correct title/description.
        sub = FastAPI(
            title=f"{connector.system} Simulator",
            description=description,
            version="0.1.0",
            docs_url=None,       # custom /docs endpoint below (error highlighting)
            redoc_url="/redoc",
            servers=[
                {
                    "url": f"/{connector.name}",
                    "description": "Simulator",
                },
            ],
            root_path_in_servers=False,
        )
        sub.add_api_route(
            "/docs",
            _make_swagger_endpoint(connector.name, f"{connector.system} Simulator"),
            include_in_schema=False,
            response_class=HTMLResponse,
        )
        api_router = build_connector_router(
            connector,
            store,
            event_bus,
            dispatcher,
            default_page_size=page_size,
            webhook_subscriptions_store=webhook_subscriptions,
        )
        sub.include_router(api_router)
        app.mount(f"/{connector.name}", sub)

    # Mount the web UI + admin CRUD + SSE routes.
    from inandout.simulator.ui.router import build_ui_router

    _static_dir = Path(__file__).parent / "ui" / "static"
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")
    app.include_router(build_ui_router())

    @app.get("/", include_in_schema=False, response_class=HTMLResponse)
    async def _root(request: Request) -> HTMLResponse:
        connector_rows = "".join(
            f'<li class="mb-3">'
            f'<span class="font-semibold text-slate-200">{c.system}</span>'
            f'<span class="text-slate-500 font-mono text-sm ml-2">/{c.name}</span>'
            f'<div class="flex gap-4 mt-1 text-sm">'
            f'<a href="/{c.name}/docs" class="text-sky-400 hover:underline">Swagger UI</a>'
            f'<a href="/{c.name}/redoc" class="text-sky-400 hover:underline">ReDoc</a>'
            f"</div>"
            f"</li>"
            for c in request.app.state.connectors
        )
        return HTMLResponse(
            f"""<!doctype html><html lang="en">
<head><meta charset="utf-8"><title>in-and-out Demo Simulator</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="/static/tailwind.css"></head>
<body class="bg-slate-900 text-slate-100 min-h-screen">
<div class="max-w-lg mx-auto px-6 py-16">
  <h1 class="text-3xl font-bold mb-1">in-and-out Simulator</h1>
  <p class="text-slate-400 mb-8">Fake API server for connector testing and demos.</p>
  <div class="flex gap-4 mb-8">
    <a href="/ui/" class="bg-sky-600 hover:bg-sky-500 text-white font-medium px-5 py-2.5 rounded-lg text-sm">
      Open UI Dashboard
    </a>
    <a href="/docs" class="bg-slate-700 hover:bg-slate-600 text-slate-100 font-medium px-5 py-2.5 rounded-lg text-sm">
      API Docs (overview)
    </a>
  </div>
  <h2 class="text-base font-semibold text-slate-300 mb-3">Connectors</h2>
  <ul class="bg-slate-800 rounded-xl border border-slate-700 p-5">{connector_rows}</ul>
</div>
</body></html>"""
        )

    @app.on_event("startup")
    async def _seed() -> None:
        for connector in app.state.connectors:
            await seed_from_connector(store, connector)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await dispatcher.aclose()

    return app
