"""Standalone CLI for the inandout_simulator package.

Entry point: ``inandout-simulator``

Usage::

    inandout-simulator run --connector connectors/hubspot.example.yaml
"""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

app = typer.Typer(
    name="inandout-simulator",
    help="Demo simulator — stateful fake CRM with a reactive web UI.",
    no_args_is_help=True,
)

console = Console()


@app.command("run")
def sim_run(
    connector: list[Path] = typer.Option(
        ...,
        "--connector",
        "-c",
        help="Path to a connector YAML.  Repeatable for multi-connector mode.",
        exists=True,
        readable=True,
    ),
    listen: str = typer.Option(
        "0.0.0.0:6100",
        "--listen",
        "-l",
        help="Bind address in host:port format.",
        show_default=True,
    ),
    store: str = typer.Option(
        "memory",
        "--store",
        help="Storage backend: 'memory', 'sqlite:///path.db', or a postgres:// DSN.",
        show_default=True,
        envvar="INOUT_SIMULATOR_STORE",
    ),
    engine_url: str = typer.Option(
        "http://localhost:9090",
        "--engine-url",
        help="Base URL of the running ingest daemon (for outbound webhook dispatch).",
        show_default=True,
        envvar="INOUT_ENGINE_URL",
    ),
    page_size: int = typer.Option(
        20,
        "--page-size",
        help="Default page size for list endpoints.",
        show_default=True,
    ),
    log_level: str = typer.Option(
        "info",
        "--log-level",
        help="Uvicorn log level.",
        show_default=True,
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        help="Enable uvicorn auto-reload (dev only; resets in-memory state on each reload).",
    ),
) -> None:
    """Start the stateful demo simulator server."""
    import uvicorn
    from inandout_simulator.app import create_app

    host, _, port_str = listen.rpartition(":")
    host = host or "0.0.0.0"
    port = int(port_str or 6100)

    connector_names = ", ".join(p.stem for p in connector)
    console.print(
        f"[bold cyan]in-and-out Demo Simulator[/bold cyan]  "
        f"connectors=[bold]{connector_names}[/bold]  "
        f"store=[bold]{store}[/bold]  "
        f"http://{host}:{port}"
    )

    if reload:
        import json
        import os

        os.environ["_SIM_CONNECTORS"] = json.dumps([str(p) for p in connector])
        os.environ["_SIM_STORE"] = store
        os.environ["_SIM_ENGINE_URL"] = engine_url
        os.environ["_SIM_PAGE_SIZE"] = str(page_size)
        uvicorn.run(
            "inandout_simulator.app:_reload_app",
            host=host,
            port=port,
            log_level=log_level,
            reload=True,
            reload_dirs=["simulator/src"],
        )
    else:
        application = create_app(
            connector_paths=connector,
            store_dsn=store,
            engine_url=engine_url,
            page_size=page_size,
        )
        uvicorn.run(application, host=host, port=port, log_level=log_level)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
