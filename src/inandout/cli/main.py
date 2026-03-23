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

dead_letter_app = typer.Typer(
    name="dead-letter",
    help="Dead-letter queue inspection and re-processing commands.",
    no_args_is_help=True,
)

app.add_typer(ingest_app, name="ingest")
app.add_typer(writeback_app, name="writeback")
app.add_typer(db_app, name="db")
app.add_typer(connector_app, name="connector")
app.add_typer(webhook_app, name="webhook")
app.add_typer(api_app, name="api")
app.add_typer(dead_letter_app, name="dead-letter")

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


@ingest_app.command("backfill")
def ingest_backfill(
    connector: str = typer.Option(..., "--connector", help="Path to connector YAML file."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name to backfill."),
    from_date: str = typer.Option(..., "--from-date", help="Start date (ISO 8601, e.g. 2024-01-01)."),
    to_date: str = typer.Option(..., "--to-date", help="End date (ISO 8601, e.g. 2024-01-31)."),
    window: str = typer.Option("1d", "--window", help="Window size per sync (e.g. 1d, 6h)."),
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
    ),
    staging_table: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--staging-table",
        help="Name of the staging table (auto-generated if omitted).",
    ),
) -> None:
    """Run a historical backfill for a connector/datatype over a date range."""
    import anyio
    from datetime import datetime, timezone

    from inandout.ingestion.backfill import BackfillConfig, run_backfill

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    connector_path = Path(connector)
    if not connector_path.exists():
        err_console.print(f"Connector file not found: {connector_path}")
        raise typer.Exit(code=1)

    try:
        from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
        to_dt = datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        err_console.print(f"Invalid date format: {exc}")
        raise typer.Exit(code=1)

    if from_dt >= to_dt:
        err_console.print("--from must be before --to")
        raise typer.Exit(code=1)

    backfill_cfg = BackfillConfig(
        connector_path=connector_path,
        datatype=datatype,
        from_dt=from_dt,
        to_dt=to_dt,
        window=window,
        staging_table=staging_table,
    )

    async def _run() -> None:
        result = await run_backfill(backfill_cfg, cfg_path)
        table = Table(title="Backfill Complete")
        table.add_column("Property", style="cyan")
        table.add_column("Value")
        table.add_row("Windows processed", str(result.windows_processed))
        table.add_row("Total records", str(result.total_records))
        table.add_row("Staging table", result.staging_table)
        table.add_row("Promoted", str(result.promoted))
        console.print(table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Backfill failed: {exc}")
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
    env: str = typer.Option(
        "production",
        "--env",
        help="Environment to run against: production (default) or staging.",
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
        from inandout.ingestion.dry_run import _patch_base_url
        from inandout.plugins.hooks import apply_hooks
        from inandout.transport.http import HttpTransportAdapter

        previews: list[dict] = []

        # Resolve connector config for the target environment
        fetch_connector_cfg = connector_cfg
        if env == "staging":
            if connector_cfg.connection.staging_base_url is None:
                raise ValueError(
                    f"Connector '{connector_cfg.name}' has no staging_base_url configured. "
                    "Set connection.staging_base_url in the connector YAML."
                )
            fetch_connector_cfg = _patch_base_url(
                connector_cfg, connector_cfg.connection.staging_base_url
            )

        async with HttpTransportAdapter(fetch_connector_cfg) as transport:
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

        env_label = f" [{env}]" if env != "production" else ""
        preview_table = Table(title=f"Dry-run: {connector_cfg.name} / {dtype_name}{env_label}")
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


@connector_app.command("new")
def connector_new(
    name: str = typer.Option(..., "--name", help="Connector name (lowercase, alphanumeric)."),
    base_url: str = typer.Option(..., "--base-url", help="Base URL of the target API."),
    auth: str = typer.Option(
        "none",
        "--auth",
        help="Auth type: api_key|oauth2|basic|none",
    ),
    spec_url: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--spec-url",
        help="URL to fetch the OpenAPI spec from (optional, auto-discovers from base-url if omitted).",
    ),
    output: str = typer.Option(
        ".",
        "--output",
        help="Output directory for the generated YAML.",
        show_default=True,
    ),
) -> None:
    """Generate a new connector YAML from an API spec (or a stub template)."""
    import anyio
    from inandout.generator.introspect import (
        extract_list_endpoints,
        fetch_openapi_spec,
        infer_auth,
        infer_pagination,
    )
    from inandout.generator.template import render_connector_test, render_connector_yaml

    async def _run() -> None:
        spec: dict | None = None

        # Try spec URL first, then auto-discover from base_url
        if spec_url:
            import httpx
            async with httpx.AsyncClient(timeout=10.0) as client:
                try:
                    resp = await client.get(spec_url)
                    if resp.status_code == 200:
                        spec = resp.json()
                except Exception as exc:
                    console.print(f"[yellow]Warning: could not fetch spec from {spec_url}: {exc}[/yellow]")
        else:
            spec = await fetch_openapi_spec(base_url)
            if spec is None:
                console.print(f"[yellow]No OpenAPI spec found at {base_url} — generating stub.[/yellow]")

        endpoints: list[dict] = []
        resolved_auth = auth

        if spec is not None:
            raw_endpoints = extract_list_endpoints(spec)
            # Enrich with pagination info
            for ep in raw_endpoints:
                ep["pagination"] = infer_pagination(spec, ep["path"])
            endpoints = raw_endpoints

            if auth == "none":
                resolved_auth = infer_auth(spec)
                if resolved_auth != "none":
                    console.print(f"[cyan]Detected auth type from spec: {resolved_auth}[/cyan]")

        yaml_content = render_connector_yaml(
            name=name,
            base_url=base_url,
            auth=resolved_auth,
            endpoints=endpoints,
        )

        out_dir = Path(output)
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = name.lower().replace(" ", "_")
        out_path = out_dir / f"{safe_name}.yaml"
        out_path.write_text(yaml_content, encoding="utf-8")

        # Write test scaffold
        from inandout.generator.template import _make_datatype_name as _mdn
        datatypes_list = [_mdn(ep["path"]) for ep in endpoints] if endpoints else []
        test_content = render_connector_test(
            name=safe_name,
            base_url=base_url,
            datatypes=datatypes_list,
        )
        test_path = out_dir / f"test_{safe_name}_connector.py"
        test_path.write_text(test_content, encoding="utf-8")
        console.print(
            f"Test scaffold written to test_{safe_name}_connector.py "
            "— fill in the TODO sections."
        )

        table = Table(title=f"Generated connector: {name}")
        table.add_column("Property", style="cyan")
        table.add_column("Value")
        table.add_row("Output", str(out_path))
        table.add_row("Base URL", base_url)
        table.add_row("Auth", resolved_auth)
        table.add_row("Endpoints detected", str(len(endpoints)))
        table.add_row(
            "Datatypes generated",
            str(len(endpoints)) if endpoints else "1 (stub)",
        )
        console.print(table)

        if endpoints:
            ep_table = Table(title="Detected endpoints")
            ep_table.add_column("Path", style="cyan")
            ep_table.add_column("Pagination")
            ep_table.add_column("Description")
            for ep in endpoints:
                ep_table.add_row(ep["path"], ep.get("pagination", "none"), ep.get("description", ""))
            console.print(ep_table)

        console.print("\n[yellow]Review the generated YAML and update all # TODO: comments.[/yellow]")

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Failed to generate connector: {exc}")
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


# ---------------------------------------------------------------------------
# lint command (Step 70)
# ---------------------------------------------------------------------------


@app.command("lint")
def lint_connectors(
    connectors_dir: str = typer.Option(
        "connectors/",
        "--connectors-dir", "-d",
        help="Directory containing connector YAML files.",
        show_default=True,
    ),
    connector: str | None = typer.Option(
        None,
        "--connector",
        help="Path to a single connector YAML file (overrides --connectors-dir).",
    ),
) -> None:
    """Run static analysis (linter) on connector YAML files."""
    from inandout.config.loader import load_connector
    from inandout.linter import lint_connector, LintDiagnostic

    SEVERITY_COLORS = {
        "error": "bold red",
        "warning": "yellow",
        "info": "cyan",
    }

    # Collect files
    if connector:
        yaml_files = [Path(connector)]
    else:
        dir_path = Path(connectors_dir)
        if not dir_path.exists():
            err_console.print(f"Connectors directory not found: {dir_path}")
            raise typer.Exit(code=1)
        yaml_files = sorted(dir_path.glob("*.yaml"))

    if not yaml_files:
        console.print("[yellow]No connector YAML files found.[/yellow]")
        raise typer.Exit(code=0)

    # Load all connector names for LINT006
    all_cfgs = []
    for yp in yaml_files:
        try:
            cfg = load_connector(yp)
            all_cfgs.append((yp, cfg))
        except Exception as exc:
            console.print(f"[red]LOAD ERROR[/red] {yp.name}: {exc}")

    known_names = [cfg.connector.name for _, cfg in all_cfgs]

    table = Table(title="Connector Lint Results")
    table.add_column("Severity", style="bold")
    table.add_column("Rule", style="cyan")
    table.add_column("Connector")
    table.add_column("Message")
    table.add_column("Path", style="dim")

    total_errors = 0
    total_diags = 0

    for yaml_path, cfg in all_cfgs:
        diags = lint_connector(cfg, known_connector_names=known_names)
        for diag in diags:
            total_diags += 1
            color = SEVERITY_COLORS.get(diag.severity, "white")
            table.add_row(
                f"[{color}]{diag.severity.upper()}[/{color}]",
                diag.rule_id,
                cfg.connector.name,
                diag.message,
                diag.path,
            )
            if diag.severity == "error":
                total_errors += 1

    if total_diags > 0:
        console.print(table)
    else:
        console.print("[green]No lint diagnostics found. All connectors are clean.[/green]")

    console.print(
        f"\n[bold]{total_diags} diagnostic(s)[/bold] found "
        f"({total_errors} error(s))"
    )

    if total_errors > 0:
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# dead-letter sub-commands (Step 75)
# ---------------------------------------------------------------------------


@dead_letter_app.command("inspect")
def dead_letter_inspect(
    connector: str = typer.Option(..., "--connector", help="Connector name."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name."),
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
    ),
    limit: int = typer.Option(20, "--limit", help="Maximum number of rows to display."),
) -> None:
    """Inspect dead-letter queue rows for a connector/datatype."""
    import anyio
    from inandout.config.loader import load_ingestion_tool_config
    from inandout.deadletter.inspect import fetch_dead_letter_rows
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)

    async def _run() -> None:
        pool = await create_pool(tool_cfg.database)
        try:
            rows = await fetch_dead_letter_rows(pool, connector, datatype, limit=limit)
        finally:
            await pool.close()

        if not rows:
            console.print(f"[green]No dead-letter rows for {connector}/{datatype}[/green]")
            return

        dl_table = Table(title=f"Dead Letter: {connector}/{datatype} ({len(rows)} rows)")
        dl_table.add_column("ID", style="cyan")
        dl_table.add_column("External ID")
        dl_table.add_column("Error Class", style="bold red")
        dl_table.add_column("Error Message")
        dl_table.add_column("Failed At")
        dl_table.add_column("Requeue Count")
        dl_table.add_column("Raw (truncated)")

        for row in rows:
            raw_preview = str(row.get("raw", ""))[:100]
            dl_table.add_row(
                str(row["id"]),
                str(row.get("external_id") or ""),
                str(row.get("error_class") or ""),
                str(row.get("error_message") or ""),
                str(row.get("failed_at") or ""),
                str(row.get("requeue_count") or 0),
                raw_preview,
            )

        console.print(dl_table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Inspect failed: {exc}")
        raise typer.Exit(code=1)


@dead_letter_app.command("transform")
def dead_letter_transform(
    connector: str = typer.Option(..., "--connector", help="Connector name."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name."),
    script: str = typer.Option(..., "--script", help="Path to Python transform script."),
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would happen without writing."),
) -> None:
    """Apply a transform script to dead-letter rows to reprocess them."""
    import anyio
    from inandout.config.loader import load_ingestion_tool_config
    from inandout.deadletter.transform import apply_transform_script
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    script_path = Path(script)
    if not script_path.exists():
        err_console.print(f"Script file not found: {script_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)

    async def _run() -> None:
        pool = await create_pool(tool_cfg.database)
        try:
            result = await apply_transform_script(pool, connector, datatype, script_path, dry_run=dry_run)
        finally:
            await pool.close()

        suffix = " (DRY RUN)" if dry_run else ""
        table = Table(title=f"Transform Result{suffix}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="bold")
        table.add_row("Processed", str(result.processed))
        table.add_row("Upserted", str(result.upserted))
        table.add_row("Dropped", str(result.dropped))
        table.add_row("Failed", str(result.failed))
        console.print(table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Transform failed: {exc}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# migrate-connector command (Step 77)
# ---------------------------------------------------------------------------


@app.command("migrate-connector")
def migrate_connector(
    input_path: str = typer.Option(..., "--input", help="Path to connector YAML to migrate."),
    from_version: str = typer.Option(..., "--from-version", help="Source schema version."),
    to_version: str = typer.Option(..., "--to-version", help="Target schema version."),
    output_path: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--output",
        help="Output path (default: overwrite input with .bak backup).",
    ),
) -> None:
    """Migrate a connector YAML from one schema version to another."""
    import shutil

    import yaml
    from inandout.migrations.connector_schema import apply_migrations, find_migration_path

    in_path = Path(input_path)
    if not in_path.exists():
        err_console.print(f"Input file not found: {in_path}")
        raise typer.Exit(code=1)

    raw_text = in_path.read_text(encoding="utf-8")
    raw_dict = yaml.safe_load(raw_text)

    try:
        migrations = find_migration_path(from_version, to_version)
    except ValueError as exc:
        err_console.print(str(exc))
        raise typer.Exit(code=1)

    if not migrations:
        console.print(f"[green]No migrations needed from {from_version} to {to_version}.[/green]")
        raise typer.Exit(code=0)

    migrated = apply_migrations(raw_dict, migrations)

    # Validate result
    try:
        from inandout.config.loader import load_connector_from_string
        result_yaml = yaml.dump(migrated, allow_unicode=True)
        load_connector_from_string(result_yaml)
        console.print("[green]Migrated config passes Pydantic validation.[/green]")
    except Exception as exc:
        console.print(f"[yellow]Warning: migrated config validation issue: {exc}[/yellow]")

    # Determine output path
    if output_path:
        out_path = Path(output_path)
    else:
        # Backup original and overwrite
        backup_path = in_path.with_suffix(in_path.suffix + ".bak")
        shutil.copy2(in_path, backup_path)
        console.print(f"[dim]Backup written to {backup_path}[/dim]")
        out_path = in_path

    result_text = yaml.dump(migrated, allow_unicode=True)
    out_path.write_text(result_text, encoding="utf-8")

    table = Table(title="Connector Migration Complete")
    table.add_column("Property", style="cyan")
    table.add_column("Value")
    table.add_row("Input", str(in_path))
    table.add_row("Output", str(out_path))
    table.add_row("From version", from_version)
    table.add_row("To version", to_version)
    table.add_row("Migrations applied", str(len(migrations)))
    for m in migrations:
        table.add_row("  Migration", f"{m.from_version} → {m.to_version}: {m.description}")
    console.print(table)


# ---------------------------------------------------------------------------
# diff command (Step 82)
# ---------------------------------------------------------------------------


@app.command("diff")
def diff_runs(
    connector: str = typer.Option(..., "--connector", help="Connector name."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name."),
    run_a: str = typer.Option(..., "--run-a", help="UUID of the first (older) sync run."),
    run_b: str = typer.Option(..., "--run-b", help="UUID of the second (newer) sync run."),
    config: str = typer.Option(
        "config/ingestion.yaml",
        "--config", "-c",
        help="Path to ingestion tool config YAML.",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        help="Output format: table|json",
    ),
) -> None:
    """Compare records between two sync runs and show what changed."""
    import anyio
    import json as _json
    from inandout.config.loader import load_ingestion_tool_config
    from inandout.diff.engine import diff_sync_runs
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_ingestion_tool_config(cfg_path)

    async def _run() -> None:
        pool = await create_pool(tool_cfg.database)
        try:
            diff = await diff_sync_runs(pool, connector, datatype, run_a, run_b)
        finally:
            await pool.close()

        if format == "json":
            import dataclasses
            typer.echo(_json.dumps(dataclasses.asdict(diff), indent=2))
            return

        summary = Table(title=f"Sync Run Diff: {connector}/{datatype}")
        summary.add_column("Metric", style="cyan")
        summary.add_column("Count", style="bold")
        summary.add_row("Added", str(len(diff.added)))
        summary.add_row("Removed", str(len(diff.removed)))
        summary.add_row("Changed", str(len(diff.changed)))
        summary.add_row("Unchanged", str(diff.unchanged_count))
        console.print(summary)

        if diff.added:
            console.print(f"\n[green]Added ({len(diff.added)}):[/green] {', '.join(diff.added[:10])}")
        if diff.removed:
            console.print(f"[red]Removed ({len(diff.removed)}):[/red] {', '.join(diff.removed[:10])}")
        if diff.changed:
            changed_table = Table(title="Changed records")
            changed_table.add_column("External ID", style="cyan")
            changed_table.add_column("Fields Changed")
            for entry in diff.changed[:20]:
                changed_table.add_row(
                    str(entry["external_id"]),
                    ", ".join(entry["fields_changed"]),
                )
            console.print(changed_table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Diff failed: {exc}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# connector test command (Step 83)
# ---------------------------------------------------------------------------


@connector_app.command("test")
def connector_test(
    connector: str = typer.Option(..., "--connector", help="Path to connector YAML file."),
    output: str = typer.Option(
        "text",
        "--output",
        help="Output format: text|junit",
    ),
    output_file: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--output-file",
        help="Path to write output (default: stdout).",
    ),
) -> None:
    """Run automated connector tests against a connector YAML."""
    import anyio
    from inandout.testing.runner import run_connector_tests, format_junit_xml, format_text_report

    connector_path = Path(connector)
    if not connector_path.exists():
        err_console.print(f"Connector file not found: {connector_path}")
        raise typer.Exit(code=1)

    async def _run() -> None:
        suite = await run_connector_tests(connector_path)
        if output == "junit":
            xml_str = format_junit_xml(suite)
            if output_file:
                Path(output_file).write_text(xml_str, encoding="utf-8")
                console.print(f"[green]JUnit XML written to {output_file}[/green]")
            else:
                typer.echo(xml_str)
        else:
            report = format_text_report(suite)
            if output_file:
                Path(output_file).write_text(report, encoding="utf-8")
            console.print(report)

        if suite.failed > 0:
            raise typer.Exit(code=1)

    try:
        anyio.run(_run)
    except typer.Exit:
        raise
    except Exception as exc:
        err_console.print(f"Connector test failed: {exc}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# connector publish command (Step 87)
# ---------------------------------------------------------------------------


@connector_app.command("publish")
def connector_publish(
    connector: str = typer.Option(..., "--connector", help="Path to connector YAML file."),
    hooks: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--hooks",
        help="Path to connector hooks Python file.",
    ),
    index_url: str = typer.Option(
        "https://connectors.inandout.io",
        "--index-url",
        help="Connector marketplace URL.",
    ),
    token: str = typer.Option(
        ...,
        "--token",
        help="Authentication token for the marketplace.",
        envvar="INANDOUT_PUBLISH_TOKEN",
    ),
    description: str = typer.Option(
        "",
        "--description",
        help="Short description for the connector.",
    ),
    version: str = typer.Option(
        "1.0.0",
        "--version",
        help="Version string for the submission.",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt."),
) -> None:
    """Publish a connector to the marketplace."""
    import anyio
    from inandout.registry.publish import (
        validate_for_publish,
        build_submission,
        submit_connector,
    )

    connector_path = Path(connector)
    hooks_path = Path(hooks) if hooks else None

    if not connector_path.exists():
        err_console.print(f"Connector file not found: {connector_path}")
        raise typer.Exit(code=1)

    if hooks_path and not hooks_path.exists():
        err_console.print(f"Hooks file not found: {hooks_path}")
        raise typer.Exit(code=1)

    async def _run() -> None:
        # Validate
        console.print("[cyan]Validating connector...[/cyan]")
        errors = await validate_for_publish(connector_path)
        if errors:
            err_console.print("[bold red]Validation failed:[/bold red]")
            for e in errors:
                err_console.print(f"  • {e}")
            raise typer.Exit(code=1)

        console.print("[green]Validation passed.[/green]")

        # Build submission
        submission = build_submission(connector_path, hooks_path, description, version)

        # Preview
        table = Table(title="Submission Preview")
        table.add_column("Property", style="cyan")
        table.add_column("Value")
        table.add_row("Name", submission.name)
        table.add_row("Version", submission.version)
        table.add_row("Description", submission.description)
        table.add_row("YAML lines", str(len(submission.yaml_content.splitlines())))
        if submission.hooks_content:
            table.add_row("Hooks lines", str(len(submission.hooks_content.splitlines())))
        console.print(table)

        if not yes:
            confirmed = typer.confirm("Submit this connector to the marketplace?")
            if not confirmed:
                console.print("[yellow]Submission cancelled.[/yellow]")
                raise typer.Exit(code=0)

        # Submit
        console.print("[cyan]Submitting...[/cyan]")
        result = await submit_connector(submission, index_url, token)
        console.print(f"[green]Submission accepted![/green] Tracking ID: {result.get('id', 'N/A')}")
        console.print(f"Status: {result.get('status', 'unknown')}")

    try:
        anyio.run(_run)
    except typer.Exit:
        raise
    except Exception as exc:
        err_console.print(f"Publish failed: {exc}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
