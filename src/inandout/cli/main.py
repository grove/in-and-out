"""CLI commands for in-and-out."""
from __future__ import annotations

import sys
from pathlib import Path

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
