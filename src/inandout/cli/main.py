"""CLI commands for in-and-out."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="inandout",
    help="In-and-Out: declarative MDM HTTP API synchronization tools.",
    no_args_is_help=True,
)

ingest_app = typer.Typer(
    name="ingest",
    help="Ingestion daemon commands.",
    no_args_is_help=True,
)

writeback_app = typer.Typer(
    name="writeback",
    help="Writeback daemon commands.",
    no_args_is_help=True,
)

db_app = typer.Typer(
    name="db",
    help="Database migration commands.",
    no_args_is_help=True,
)

app.add_typer(ingest_app, name="ingest")
app.add_typer(writeback_app, name="writeback")
app.add_typer(db_app, name="db")

console = Console()
err_console = Console(stderr=True, style="bold red")


@app.command()
def version() -> None:
    """Print the installed version."""
    from inandout import __version__
    typer.echo(__version__)


# ---------------------------------------------------------------------------
# ingest sub-commands
# ---------------------------------------------------------------------------

@ingest_app.command("run")
def ingest_run(
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
        show_default=True,
    ),
) -> None:
    """Start the ingestion daemon (blocking)."""
    import anyio
    from inandout.ingestion.daemon import run_ingestion_daemon

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    console.print(f"[green]Starting ingestion daemon[/green] — config: {cfg_path}")
    anyio.run(run_ingestion_daemon, cfg_path)


@ingest_app.command("validate")
def ingest_validate(
    connectors_dir: str = typer.Option(
        "connectors/",
        "--connectors-dir", "-d",
        help="Directory containing connector YAML files.",
        show_default=True,
    ),
    strict: bool = typer.Option(False, "--strict", help="Exit 1 on any warning."),
) -> None:
    """Validate all connector configurations in a directory."""
    from inandout.config.loader import load_connector

    dir_path = Path(connectors_dir)
    if not dir_path.exists():
        err_console.print(f"Connectors directory not found: {dir_path}")
        raise typer.Exit(code=1)

    yaml_files = sorted(dir_path.glob("*.yaml"))
    if not yaml_files:
        console.print(f"[yellow]No .yaml files found in {dir_path}[/yellow]")
        raise typer.Exit(code=0)

    table = Table(title="Connector Validation Results")
    table.add_column("File", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details")

    errors = 0
    for yaml_path in yaml_files:
        try:
            cfg = load_connector(yaml_path)
            connector = cfg.connector
            datatypes = ", ".join(connector.datatypes.keys())
            table.add_row(yaml_path.name, "[green]OK[/green]", f"{connector.name} ({datatypes})")
        except Exception as exc:
            table.add_row(yaml_path.name, "[red]FAIL[/red]", str(exc))
            errors += 1

    console.print(table)

    if errors:
        err_console.print(f"\n{errors} connector(s) failed validation.")
        raise typer.Exit(code=1)

    console.print(f"\n[green]All {len(yaml_files)} connector(s) valid.[/green]")


@ingest_app.command("validate-connector")
def ingest_validate_connector(
    connector: str = typer.Option(
        ...,
        "--connector",
        help="Path to a single connector YAML file.",
    ),
) -> None:
    """Validate a single connector YAML against the Pydantic schema."""
    from inandout.config.loader import load_connector

    connector_path = Path(connector)
    table = Table(title="Connector Validation")
    table.add_column("File", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details")

    if not connector_path.exists():
        table.add_row(connector_path.name, "[red]FAIL[/red]", "File not found")
        console.print(table)
        raise typer.Exit(code=1)

    try:
        cfg = load_connector(connector_path)
        conn = cfg.connector
        datatypes = ", ".join(conn.datatypes.keys())
        table.add_row(
            connector_path.name,
            "[green]OK[/green]",
            f"{conn.name} — datatypes: {datatypes}",
        )
        console.print(table)
    except Exception as exc:
        table.add_row(connector_path.name, "[red]FAIL[/red]", str(exc))
        console.print(table)
        err_console.print(f"\nValidation failed: {exc}")
        raise typer.Exit(code=1)


@ingest_app.command("dry-run")
def ingest_dry_run(
    connector: str = typer.Option(
        ...,
        "--connector",
        help="Path to a connector YAML file.",
    ),
    datatype: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--datatype",
        help="Specific datatype to test (default: all).",
    ),
    limit: int = typer.Option(
        10,
        "--limit",
        help="Maximum number of records to preview per datatype.",
    ),
) -> None:
    """Fetch ONE page from the real API and preview records without writing to DB."""
    import anyio

    from inandout.config.loader import load_connector

    connector_path = Path(connector)
    if not connector_path.exists():
        err_console.print(f"Connector file not found: {connector_path}")
        raise typer.Exit(code=1)

    try:
        cfg = load_connector(connector_path)
    except Exception as exc:
        err_console.print(f"Invalid connector config: {exc}")
        raise typer.Exit(code=1)

    connector_cfg = cfg.connector

    # Determine which datatypes to test
    if datatype:
        if datatype not in connector_cfg.datatypes:
            err_console.print(f"Datatype '{datatype}' not found in connector.")
            raise typer.Exit(code=1)
        dtype_names = [datatype]
    else:
        dtype_names = [
            name
            for name, dc in connector_cfg.datatypes.items()
            if dc.ingestion is not None
        ]

    async def _run_dry_run() -> list[dict]:
        """Run the dry-run fetches and return preview rows."""
        from inandout.plugins.hooks import apply_hooks
        from inandout.transport.http import HttpTransportAdapter

        previews: list[dict] = []

        async with HttpTransportAdapter(connector_cfg) as transport:
            for dtype_name in dtype_names:
                dtype_cfg = connector_cfg.datatypes[dtype_name]
                if dtype_cfg.ingestion is None:
                    continue
                ingestion_cfg = dtype_cfg.ingestion

                # Fetch ONLY one page (no pagination follow-through)
                page_count = 0
                async for page in transport.fetch_pages(ingestion_cfg.list, watermark=None):
                    records = page[:limit]
                    for record in records:
                        # Apply transform/filter hooks (dry-run mode — no pool)
                        result = await apply_hooks(record, connector_cfg.name, pool=None)
                        if result is None:
                            action = "filtered"
                            rec = record
                        else:
                            action = "insert"
                            rec = result

                        preview_fields = list(rec.keys())[:3]
                        preview_vals = {k: rec[k] for k in preview_fields}

                        from inandout.ingestion.engine import _extract_external_id
                        ext_id = _extract_external_id(rec, ingestion_cfg.primary_key)

                        previews.append({
                            "dtype": dtype_name,
                            "external_id": ext_id or "(unknown)",
                            "action": action,
                            "field_count": len(rec),
                            "preview": str(preview_vals),
                        })

                    page_count += 1
                    break  # Only one page

        return previews

    try:
        previews = anyio.run(_run_dry_run)
    except Exception as exc:
        err_console.print(f"Dry-run failed: {exc}")
        raise typer.Exit(code=1)

    # Display results
    for dtype_name in dtype_names:
        dtype_previews = [p for p in previews if p["dtype"] == dtype_name]
        dtype_cfg = connector_cfg.datatypes.get(dtype_name)
        if dtype_cfg is None or dtype_cfg.ingestion is None:
            continue

        from inandout.postgres.schema import source_table_name
        table_name = source_table_name(connector_cfg.name, dtype_name)

        preview_table = Table(title=f"Dry-run: {connector_cfg.name} / {dtype_name}")
        preview_table.add_column("external_id", style="cyan")
        preview_table.add_column("action", style="bold")
        preview_table.add_column("field_count")
        preview_table.add_column("preview (first 3 fields)")

        for p in dtype_previews:
            color = "green" if p["action"] == "insert" else "yellow"
            preview_table.add_row(
                str(p["external_id"]),
                f"[{color}]{p['action']}[/{color}]",
                str(p["field_count"]),
                p["preview"],
            )

        console.print(preview_table)
        inserts = sum(1 for p in dtype_previews if p["action"] == "insert")
        console.print(
            f"[bold]Would insert {inserts} record(s) into [cyan]{table_name}[/cyan][/bold]"
        )


# ---------------------------------------------------------------------------
# writeback sub-commands
# ---------------------------------------------------------------------------

@writeback_app.command("run")
def writeback_run(
    config: str = typer.Option(
        "config/writeback.yaml",
        "--config", "-c",
        help="Path to writeback tool config YAML.",
        show_default=True,
    ),
) -> None:
    """Start the writeback daemon (blocking)."""
    import anyio
    from inandout.writeback.daemon import run_writeback_daemon

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    console.print(f"[green]Starting writeback daemon[/green] — config: {cfg_path}")
    anyio.run(run_writeback_daemon, cfg_path)


# ---------------------------------------------------------------------------
# db sub-commands
# ---------------------------------------------------------------------------

@db_app.command("upgrade")
def db_upgrade(
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Tool config YAML (for database URL).",
        show_default=True,
    ),
    revision: str = typer.Argument(default="head", help="Target Alembic revision."),
) -> None:
    """Run Alembic migrations up to the target revision (default: head)."""
    import os
    from alembic import command as alembic_cmd
    from alembic.config import Config as AlembicConfig
    from inandout.config.loader import load_ingestion_tool_config

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)
    os.environ["INOUT_DATABASE_URL"] = tool_cfg.database.dsn

    alembic_cfg = AlembicConfig("alembic.ini")
    console.print(f"[green]Running migrations[/green] → {revision}")
    alembic_cmd.upgrade(alembic_cfg, revision)
    console.print("[green]Migrations complete.[/green]")


@db_app.command("downgrade")
def db_downgrade(
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Tool config YAML (for database URL).",
        show_default=True,
    ),
    revision: str = typer.Argument(help="Target Alembic revision (e.g. '-1' or a revision ID)."),
) -> None:
    """Roll back Alembic migrations to the target revision."""
    import os
    from alembic import command as alembic_cmd
    from alembic.config import Config as AlembicConfig
    from inandout.config.loader import load_ingestion_tool_config

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)
    os.environ["INOUT_DATABASE_URL"] = tool_cfg.database.dsn

    alembic_cfg = AlembicConfig("alembic.ini")
    console.print(f"[yellow]Rolling back migrations[/yellow] → {revision}")
    alembic_cmd.downgrade(alembic_cfg, revision)
    console.print("[yellow]Rollback complete.[/yellow]")


@db_app.command("status")
def db_status(
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Tool config YAML (for database URL).",
        show_default=True,
    ),
) -> None:
    """Show current Alembic migration status."""
    import os
    from alembic import command as alembic_cmd
    from alembic.config import Config as AlembicConfig
    from inandout.config.loader import load_ingestion_tool_config

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)
    os.environ["INOUT_DATABASE_URL"] = tool_cfg.database.dsn

    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cmd.current(alembic_cfg)


if __name__ == "__main__":
    app()
