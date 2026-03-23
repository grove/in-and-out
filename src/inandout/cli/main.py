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

connector_app = typer.Typer(
    name="connector",
    help="Connector marketplace commands.",
    no_args_is_help=True,
)

webhook_app = typer.Typer(
    name="webhook",
    help="Webhook management commands.",
    no_args_is_help=True,
)

api_app = typer.Typer(
    name="api",
    help="API specification and client SDK generation commands.",
    no_args_is_help=True,
)

app.add_typer(ingest_app, name="ingest")
app.add_typer(writeback_app, name="writeback")
app.add_typer(db_app, name="db")
app.add_typer(connector_app, name="connector")
app.add_typer(webhook_app, name="webhook")
app.add_typer(api_app, name="api")

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


# ---------------------------------------------------------------------------
# connector sub-commands (marketplace / registry)
# ---------------------------------------------------------------------------

_DEFAULT_INDEX_URL = (
    "https://raw.githubusercontent.com/grove/in-and-out-connectors/main/index.json"
)


@connector_app.command("list")
def connector_list(
    index: str = typer.Option(
        _DEFAULT_INDEX_URL,
        "--index",
        help="URL of the connector index JSON.",
        show_default=False,
    ),
) -> None:
    """Fetch and print the connector index as a rich table."""
    import anyio
    from inandout.registry import fetch_index

    async def _run() -> None:
        idx = await fetch_index(index)
        table = Table(title="Available Connectors")
        table.add_column("Name", style="cyan")
        table.add_column("Version", style="bold")
        table.add_column("Description")
        for entry in idx.connectors:
            table.add_row(entry.name, entry.version, entry.description)
        console.print(table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Failed to fetch connector index: {exc}")
        raise typer.Exit(code=1)


@connector_app.command("install")
def connector_install(
    name: str = typer.Argument(help="Name of the connector to install."),
    index: str = typer.Option(
        _DEFAULT_INDEX_URL,
        "--index",
        help="URL of the connector index JSON.",
        show_default=False,
    ),
    dest: str = typer.Option(
        "./connectors",
        "--dest",
        help="Destination directory for the installed connector.",
        show_default=True,
    ),
) -> None:
    """Install a connector by name from the index."""
    import anyio
    from inandout.registry import fetch_index, install_connector

    async def _run() -> None:
        idx = await fetch_index(index)
        matches = [e for e in idx.connectors if e.name == name]
        if not matches:
            err_console.print(f"Connector '{name}' not found in index.")
            raise typer.Exit(code=1)
        entry = matches[0]
        dest_path = Path(dest)
        yaml_path = await install_connector(entry, dest_path)
        console.print(f"[green]Installed[/green] {entry.name} v{entry.version} → {yaml_path}")

    try:
        anyio.run(_run)
    except typer.Exit:
        raise
    except Exception as exc:
        err_console.print(f"Install failed: {exc}")
        raise typer.Exit(code=1)


@connector_app.command("search")
def connector_search(
    query: str = typer.Argument(help="Search query (substring match on name/description)."),
    index: str = typer.Option(
        _DEFAULT_INDEX_URL,
        "--index",
        help="URL of the connector index JSON.",
        show_default=False,
    ),
) -> None:
    """Fuzzy search connectors by name or description."""
    import anyio
    from inandout.registry import fetch_index, search_connectors

    async def _run() -> None:
        idx = await fetch_index(index)
        results = search_connectors(idx, query)
        if not results:
            console.print(f"[yellow]No connectors matching '{query}'.[/yellow]")
            return
        table = Table(title=f"Search results for '{query}'")
        table.add_column("Name", style="cyan")
        table.add_column("Version", style="bold")
        table.add_column("Description")
        for entry in results:
            table.add_row(entry.name, entry.version, entry.description)
        console.print(table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Search failed: {exc}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# webhook sub-commands (Step 41)
# ---------------------------------------------------------------------------

def _parse_duration_to_seconds(duration: str) -> int:
    """Parse a simple duration string like '1h', '30m', '7d' to seconds."""
    from inandout.config._duration import parse_duration
    return int(parse_duration(duration))


@webhook_app.command("replay")
def webhook_replay(
    connector: str = typer.Option(..., "--connector", help="Connector name."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name."),
    since: Optional[str] = typer.Option(  # noqa: UP007
        "1h",
        "--since",
        help="Time window (e.g. '1h', '30m', '7d'). Default: 1h.",
    ),
    limit: int = typer.Option(100, "--limit", help="Maximum rows to replay."),
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
    ),
) -> None:
    """Replay webhook events from the audit log for a connector/datatype."""
    import anyio
    from inandout.config.loader import load_ingestion_tool_config
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)
    since_secs = _parse_duration_to_seconds(since or "1h")

    async def _run() -> None:
        pool = await create_pool(tool_cfg.database)
        try:
            async with pool.connection() as conn:
                rows = await (await conn.execute(
                    """
                    SELECT id, connector, datatype, external_id, received_at,
                           payload_hash, action, status
                    FROM inout_ops_webhook_log
                    WHERE connector = %s AND datatype = %s
                      AND received_at >= NOW() - INTERVAL '1 second' * %s
                    ORDER BY received_at DESC
                    LIMIT %s
                    """,
                    [connector, datatype, since_secs, limit],
                )).fetchall()
        finally:
            await pool.close()

        table = Table(title=f"Webhook Replay — {connector}/{datatype}")
        table.add_column("ID", style="cyan")
        table.add_column("Received At")
        table.add_column("Action")
        table.add_column("Status")
        table.add_column("External ID")
        table.add_column("Replay")

        for row in rows:
            wl_id, conn_name, dtype, ext_id, recv_at, phash, action, status = row
            if action == "direct_upsert":
                replay_status = "[green]replayed[/green]"
                # Note: actual re-trigger would need the original payload; log only
            else:
                replay_status = "[yellow]not supported (notification-only)[/yellow]"

            if action == "sync_triggered":
                console.print(
                    f"[yellow]Warning:[/yellow] row {wl_id} has action='sync_triggered' — "
                    "replay not supported (original payload unavailable)"
                )

            table.add_row(
                str(wl_id),
                str(recv_at),
                action or "",
                status or "",
                ext_id or "",
                replay_status,
            )

        console.print(table)
        console.print(f"[bold]{len(rows)} row(s) found.[/bold]")

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Replay failed: {exc}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# connector status command (Step 46)
# ---------------------------------------------------------------------------

@connector_app.command("status")
def connector_status(
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
    ),
) -> None:
    """Show deployed connector versions from the database."""
    import anyio
    from inandout.config.loader import load_ingestion_tool_config
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)

    async def _run() -> None:
        pool = await create_pool(tool_cfg.database)
        try:
            async with pool.connection() as conn:
                rows = await (await conn.execute(
                    """
                    SELECT connector, deployed_version, updated_at
                    FROM inout_ops_connector_version
                    ORDER BY connector
                    """
                )).fetchall()
        finally:
            await pool.close()

        table = Table(title="Connector Version Status")
        table.add_column("Connector", style="cyan")
        table.add_column("Deployed Version", style="bold")
        table.add_column("Last Updated")

        for row in rows:
            table.add_row(str(row[0]), str(row[1]), str(row[2]))

        console.print(table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Failed to fetch connector status: {exc}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# api sub-commands (Step 54)
# ---------------------------------------------------------------------------


@api_app.command("spec")
def api_spec(
    output: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--output", "-o",
        help="Path to write OpenAPI JSON (default: stdout).",
    ),
) -> None:
    """Dump the OpenAPI spec as JSON to file or stdout."""
    import json

    from fastapi import FastAPI
    from inandout.api import build_api_router

    # Build a minimal FastAPI app without a real pool
    spec_app = FastAPI(title="in-and-out management API", version="0.1.0")
    router = build_api_router(pool=None)
    spec_app.include_router(router, prefix="/api")

    spec = spec_app.openapi()
    spec_json = json.dumps(spec, indent=2)

    if output:
        out_path = Path(output)
        out_path.write_text(spec_json)
        console.print(f"[green]OpenAPI spec written to[/green] {out_path}")
    else:
        typer.echo(spec_json)


@api_app.command("generate-sdk")
def api_generate_sdk(
    lang: str = typer.Option(
        ...,
        "--lang",
        help="Target language: python|typescript|go",
    ),
    output: str = typer.Option(
        ...,
        "--output",
        help="Output directory for generated SDK.",
    ),
    config: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--config",
        help="Path to openapi-generator config file.",
    ),
) -> None:
    """Generate a client SDK from the OpenAPI spec using openapi-generator-cli."""
    import json
    import shutil
    import subprocess
    import tempfile

    from fastapi import FastAPI
    from inandout.api import build_api_router

    # Check for openapi-generator-cli
    generator = shutil.which("openapi-generator-cli")
    if generator is None:
        err_console.print(
            "openapi-generator-cli not found on PATH.\n"
            "Install it via: npm install -g @openapitools/openapi-generator-cli\n"
            "Or: brew install openapi-generator"
        )
        raise typer.Exit(code=1)

    # Build spec
    spec_app = FastAPI(title="in-and-out management API", version="0.1.0")
    router = build_api_router(pool=None)
    spec_app.include_router(router, prefix="/api")
    spec = spec_app.openapi()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump(spec, tmp, indent=2)
        tmp_path = tmp.name

    try:
        cmd = [
            generator,
            "generate",
            "-i", tmp_path,
            "-g", f"{lang}-experimental",
            "-o", output,
        ]
        if config:
            cmd.extend(["-c", config])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            err_console.print(f"SDK generation failed:\n{result.stderr}")
            raise typer.Exit(code=1)

        console.print(f"[green]SDK generated successfully[/green] → {output}")
    finally:
        import os
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


if __name__ == "__main__":
    app()
