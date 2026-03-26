"""FastAPI application factory for the demo simulator."""

from __future__ import annotations

from pathlib import Path
from typing import Union

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

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
    dispatcher = WebhookDispatcher(engine_url=engine_url)

    connector_configs: list[ConnectorConfig] = []
    for path in connector_paths:
        connector_configs.append(load_connector(path).connector)

    # Build a summary for the parent app description.
    connector_list = "\n".join(
        f"- **[{c.system}](/{c.name}/docs)** — `/{c.name}`"
        for c in connector_configs
    )

    app = FastAPI(
        title="in-and-out Demo Simulator",
        description=(
            "Fake API server for connector testing and demos.\n\n"
            "Each connector has its own Swagger UI:\n\n"
            + connector_list
        ),
        version="0.1.0",
    )

    # Keep connector configs accessible to the UI router.
    app.state.connectors = []
    app.state.store = store
    app.state.event_bus = event_bus
    app.state.dispatcher = dispatcher
    app.state.page_size = page_size

    for connector in connector_configs:
        app.state.connectors.append(connector)

        # Each connector gets its own sub-application so it has a dedicated
        # Swagger UI at /{connector.name}/docs with the correct title/description.
        sub = FastAPI(
            title=f"{connector.system} Simulator",
            description=connector.description or f"Simulated {connector.system} API.",
            version="0.1.0",
            docs_url="/docs",
            redoc_url="/redoc",
        )
        api_router = build_connector_router(
            connector,
            store,
            event_bus,
            dispatcher,
            default_page_size=page_size,
        )
        sub.include_router(api_router)
        app.mount(f"/{connector.name}", sub)

    # Mount the web UI + admin CRUD + SSE routes.
    from inandout.simulator.ui.router import build_ui_router

    app.include_router(build_ui_router())

    @app.get("/", include_in_schema=False)
    async def _root() -> RedirectResponse:
        return RedirectResponse(url="/ui/")

    @app.on_event("startup")
    async def _seed() -> None:
        for connector in app.state.connectors:
            await seed_from_connector(store, connector)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await dispatcher.aclose()

    return app
