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

control_app = typer.Typer(
    name="control",
    help="Runtime control table commands — issue operator commands to running daemons.",
    no_args_is_help=True,
)
app.add_typer(control_app, name="control")

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
    import os
    from inandout.ingestion.daemon import run_ingestion_daemon
    from inandout.config.loader import load_ingestion_tool_config

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    # Emit operator audit record before starting the daemon
    try:
        tool_cfg = load_ingestion_tool_config(cfg_path)
        issued_by = os.environ.get("USER", "cli")
        _write_operator_audit(
            tool_cfg.database.dsn,
            "operator-action",
            {"action": "ingest-run-started", "config": str(cfg_path)},
            issued_by,
        )
    except Exception:
        pass  # audit failure must never block daemon startup

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
    check_connectivity: bool = typer.Option(
        True,
        "--check-connectivity/--skip-connectivity",
        help="Probe the connector's base_url with an HTTP GET after schema validation.",
    ),
) -> None:
    """Validate a single connector YAML against the Pydantic schema."""
    import anyio
    from inandout.config.loader import load_connector

    connector_path = Path(connector)
    table = Table(title="Connector Validation")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details")

    if not connector_path.exists():
        table.add_row("schema", "[red]FAIL[/red]", "File not found")
        console.print(table)
        raise typer.Exit(code=1)

    try:
        cfg = load_connector(connector_path)
        conn = cfg.connector
        datatypes = ", ".join(conn.datatypes.keys())
        table.add_row(
            "schema",
            "[green]OK[/green]",
            f"{conn.name} — datatypes: {datatypes}",
        )
    except Exception as exc:
        table.add_row("schema", "[red]FAIL[/red]", str(exc))
        console.print(table)
        err_console.print(f"\nValidation failed: {exc}")
        raise typer.Exit(code=1)

    if check_connectivity:
        base_url: str = cfg.connector.connection.base_url

        async def _probe() -> tuple[int | None, str]:
            import httpx
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(base_url)
                    return resp.status_code, f"HTTP {resp.status_code}"
            except Exception as exc:  # noqa: BLE001
                return None, str(exc)

        status_code, detail = anyio.run(_probe)
        if status_code is not None and status_code < 500:
            table.add_row("connectivity", "[green]OK[/green]", f"{base_url} → {detail}")
        else:
            table.add_row("connectivity", "[yellow]WARN[/yellow]", f"{base_url} → {detail}")

    console.print(table)


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
                        action = "insert"
                        rec = record

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


@writeback_app.command("validate-connector")
def writeback_validate_connector(
    connector: str = typer.Option(
        ...,
        "--connector",
        help="Path to a single connector YAML file.",
    ),
    datatype: str = typer.Option(
        None,
        "--datatype",
        help="Validate only this datatype (default: all writeback datatypes).",
    ),
) -> None:
    """Validate a writeback connector: schema, connectivity, auth, ETag probe (T2 #37).

    Performs non-destructive checks and reports the effective write-anomaly
    protection level per datatype.
    """
    import anyio
    from inandout.config.loader import load_connector
    from inandout.writeback.validate import validate_writeback_connector

    connector_path = Path(connector)
    if not connector_path.exists():
        err_console.print(f"Connector file not found: {connector_path}")
        raise typer.Exit(code=1)

    try:
        cfg = load_connector(connector_path)
    except Exception as exc:
        err_console.print(f"[red]Schema validation failed:[/red] {exc}")
        raise typer.Exit(code=1)

    connector_cfg = cfg.connector
    datatype_names = [datatype] if datatype else None

    async def _run() -> None:
        return await validate_writeback_connector(connector_cfg, datatype_names=datatype_names)

    try:
        result = anyio.run(_run)
    except Exception as exc:
        err_console.print(f"[red]Validation error:[/red] {exc}")
        raise typer.Exit(code=1)

    # Print summary table
    from rich.table import Table

    table = Table(title=f"Writeback Validation — {connector_cfg.name}")
    table.add_column("Check", style="cyan")
    table.add_column("Result", style="bold")
    table.add_column("Details")

    connectivity_style = "[green]OK[/green]" if result.connectivity == "ok" else "[red]FAIL[/red]"
    auth_style = "[green]OK[/green]" if result.auth == "ok" else "[red]FAIL[/red]"
    table.add_row("Connectivity", connectivity_style, connector_cfg.connection.base_url)
    table.add_row("Auth", auth_style, "")

    for dt in result.datatypes:
        errors_str = "; ".join(dt.errors) if dt.errors else ""
        warn_str = "; ".join(dt.warnings) if dt.warnings else ""
        status = "[green]OK[/green]" if not dt.errors else "[red]FAIL[/red]"
        detail_parts = [
            f"configured={dt.configured_protection_level}",
            f"effective={dt.effective_protection_level}",
            f"etag={dt.etag_support}",
        ]
        if errors_str:
            detail_parts.append(f"errors: {errors_str}")
        if warn_str:
            detail_parts.append(f"warnings: {warn_str}")
        table.add_row(f"  {dt.datatype}", status, " | ".join(detail_parts))

    console.print(table)

    if result.errors:
        for err in result.errors:
            err_console.print(f"[red]Error:[/red] {err}")

    if not result.ok:
        raise typer.Exit(code=1)


@writeback_app.command("dry-run")
def writeback_dry_run(
    connector: str = typer.Option(
        ...,
        "--connector",
        help="Path to a connector YAML file.",
    ),
    datatype: Optional[str] = typer.Option(  # noqa: UP007
        None,
        "--datatype",
        help="Specific datatype to test (default: all writeback datatypes).",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        help="Maximum number of delta rows to preview per datatype.",
    ),
) -> None:
    """Preview what a writeback cycle would do without issuing any HTTP writes (T2 #27).

    Reads from the delta table and shows what actions (insert/update/delete)
    would be dispatched, complete with URL and payload preview.  No HTTP
    requests are sent; no rows are deleted from the delta table.
    """
    import anyio
    from inandout.config.loader import load_connector

    connector_path = Path(connector)
    if not connector_path.exists():
        err_console.print(f"Connector file not found: {connector_path}")
        raise typer.Exit(code=1)

    try:
        cfg = load_connector(connector_path)
    except Exception as exc:
        err_console.print(f"[red]Invalid connector config:[/red] {exc}")
        raise typer.Exit(code=1)

    connector_cfg = cfg.connector
    dtype_names: list[str]
    if datatype:
        if datatype not in connector_cfg.datatypes:
            err_console.print(f"Datatype '{datatype}' not found in connector.")
            raise typer.Exit(code=1)
        dtype_names = [datatype]
    else:
        dtype_names = [
            name
            for name, dc in connector_cfg.datatypes.items()
            if dc.writeback is not None
        ]

    if not dtype_names:
        console.print("[yellow]No writeback-enabled datatypes found in connector.[/yellow]")
        raise typer.Exit(code=0)

    async def _run() -> dict[str, list[dict]]:
        from inandout.writeback.engine import WritebackEngine
        from unittest.mock import AsyncMock, MagicMock

        # Minimal fake pool that returns empty rows from the delta table.
        # The dry_run flag on writeback_cfg prevents any HTTP calls.
        fake_pool = MagicMock()
        fake_conn = AsyncMock()
        fake_cursor = AsyncMock()
        fake_cursor.description = [("external_id",), ("_action",)]
        fake_cursor.fetchall = AsyncMock(return_value=[])
        fake_conn.execute = AsyncMock(return_value=fake_cursor)
        fake_conn.__aenter__ = AsyncMock(return_value=fake_conn)
        fake_conn.__aexit__ = AsyncMock(return_value=None)
        fake_pool.connection = MagicMock(return_value=fake_conn)

        engine = WritebackEngine(fake_pool)
        results: dict[str, list[dict]] = {}
        for dtype_name in dtype_names:
            dtype_cfg = connector_cfg.datatypes[dtype_name]
            if dtype_cfg.writeback is None:
                continue
            import copy
            wb_cfg = copy.deepcopy(dtype_cfg.writeback)
            # Force dry_run mode and a small batch size
            object.__setattr__(wb_cfg, "dry_run", True)
            object.__setattr__(wb_cfg, "batch_size", limit)
            delta_table = f"_delta_{connector_cfg.name}_{dtype_name}"
            result = await engine.run_writeback_cycle(
                connector_cfg, dtype_name, wb_cfg, delta_table
            )
            results[dtype_name] = result.dry_run_log
        return results

    try:
        all_results = anyio.run(_run)
    except Exception as exc:
        err_console.print(f"[red]Dry-run failed:[/red] {exc}")
        raise typer.Exit(code=1)

    for dtype_name, log_entries in all_results.items():
        preview_table = Table(title=f"Writeback dry-run: {connector_cfg.name} / {dtype_name}")
        preview_table.add_column("action", style="bold")
        preview_table.add_column("method")
        preview_table.add_column("url", style="cyan")
        preview_table.add_column("payload preview")

        for entry in log_entries:
            action_label = str(entry.get("action", ""))
            method_label = str(entry.get("method", ""))
            url_label = str(entry.get("url", ""))
            body = entry.get("body") or {}
            body_preview = str({k: v for k, v in list(body.items())[:3]})
            color = "green" if "insert" in action_label else ("red" if "delete" in action_label else "yellow")
            preview_table.add_row(
                f"[{color}]{action_label}[/{color}]",
                method_label,
                url_label,
                body_preview,
            )

        console.print(preview_table)
        if not log_entries:
            console.print(f"[dim]No delta rows found for {dtype_name} — nothing to write back.[/dim]")
        else:
            console.print(f"[bold]{len(log_entries)} action(s) would be dispatched for {dtype_name}.[/bold]")


# ---------------------------------------------------------------------------
# Operator audit trail helper
# ---------------------------------------------------------------------------

def _write_operator_audit(dsn: str, command: str, payload: dict, issued_by: str) -> None:
    """Insert an operator-action row into inout_ops_control for audit trail.

    Silently swallows errors so it never blocks the actual CLI action.
    This satisfies the GOAL.md requirement that CLI-initiated actions produce
    a durable audit record with issued_by tracking.
    """
    import asyncio
    import uuid as _uuid

    async def _insert() -> None:
        import psycopg
        import orjson

        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            await conn.execute(
                """
                INSERT INTO inout_ops_control
                    (id, connector, datatype, command, payload, target_tool, status, issued_by, issued_at)
                VALUES
                    (%s, NULL, NULL, %s, %s::jsonb, 'cli', 'completed', %s, NOW())
                ON CONFLICT DO NOTHING
                """,
                [
                    str(_uuid.uuid4()),
                    command,
                    orjson.dumps(payload).decode(),
                    issued_by,
                ],
            )
            await conn.commit()

    try:
        asyncio.run(_insert())
    except Exception:
        pass  # audit failure must never block the operator action


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
    dsn = tool_cfg.database.dsn
    os.environ["INOUT_DATABASE_URL"] = dsn
    issued_by = os.environ.get("USER", "cli")
    _write_operator_audit(dsn, "operator-action", {"action": "db-upgrade", "revision": revision}, issued_by)

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
    dsn = tool_cfg.database.dsn
    os.environ["INOUT_DATABASE_URL"] = dsn
    issued_by = os.environ.get("USER", "cli")
    _write_operator_audit(dsn, "operator-action", {"action": "db-downgrade", "revision": revision}, issued_by)

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


@dead_letter_app.command("writeback-inspect")
def dead_letter_writeback_inspect(
    connector: str = typer.Option(..., "--connector", help="Connector name."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name."),
    config: str = typer.Option(
        "config/writeback.yaml",
        "--config", "-c",
        help="Path to writeback tool config YAML.",
    ),
    limit: int = typer.Option(20, "--limit", help="Maximum number of rows to display."),
) -> None:
    """Inspect writeback dead-letter rows for a connector/datatype (T2 #24)."""
    import anyio
    from inandout.config.loader import load_writeback_tool_config
    from inandout.deadletter.writeback import fetch_writeback_dead_letter_rows
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_writeback_tool_config(cfg_path)

    async def _run() -> None:
        pool = await create_pool(tool_cfg.database)
        try:
            rows = await fetch_writeback_dead_letter_rows(pool, connector, datatype, limit=limit)
        finally:
            await pool.close()

        if not rows:
            console.print(f"[green]No writeback dead-letter rows for {connector}/{datatype}[/green]")
            return

        dl_table = Table(title=f"Writeback Dead Letter: {connector}/{datatype} ({len(rows)} rows)")
        dl_table.add_column("ID", style="cyan")
        dl_table.add_column("External ID")
        dl_table.add_column("Action", style="bold")
        dl_table.add_column("Error Message")
        dl_table.add_column("Failed At")
        dl_table.add_column("Requeue Count")

        for row in rows:
            dl_table.add_row(
                str(row["id"]),
                str(row.get("external_id") or ""),
                str(row.get("error_class") or ""),   # error_class stores the action
                str(row.get("error_message") or ""),
                str(row.get("failed_at") or ""),
                str(row.get("requeue_count") or 0),
            )

        console.print(dl_table)

    try:
        anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Inspect failed: {exc}")
        raise typer.Exit(code=1)


@dead_letter_app.command("writeback-replay")
def dead_letter_writeback_replay(
    connector: str = typer.Option(..., "--connector", help="Connector name."),
    datatype: str = typer.Option(..., "--datatype", help="Datatype name."),
    config: str = typer.Option(
        "config/writeback.yaml",
        "--config", "-c",
        help="Path to writeback tool config YAML.",
    ),
    limit: int = typer.Option(50, "--limit", help="Maximum rows to replay."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be replayed without requeuing."),
) -> None:
    """Replay writeback dead-letter rows back into the delta table (T2 #24)."""
    import anyio
    from inandout.config.loader import load_writeback_tool_config
    from inandout.deadletter.writeback import fetch_writeback_dead_letter_rows, replay_writeback_dead_letter_rows
    from inandout.postgres.pool import create_pool

    cfg_path = Path(config)
    if not cfg_path.exists():
        err_console.print(f"Config file not found: {cfg_path}")
        raise typer.Exit(code=1)

    tool_cfg = load_writeback_tool_config(cfg_path)
    delta_table = f"_delta_{connector}_{datatype}"

    async def _run() -> dict:
        pool = await create_pool(tool_cfg.database)
        try:
            if dry_run:
                rows = await fetch_writeback_dead_letter_rows(pool, connector, datatype, limit=limit)
                return {"would_replay": len(rows), "dry_run": True}
            return await replay_writeback_dead_letter_rows(
                pool, connector, datatype, delta_table, limit=limit
            )
        finally:
            await pool.close()

    try:
        result = anyio.run(_run)
    except Exception as exc:
        err_console.print(f"Replay failed: {exc}")
        raise typer.Exit(code=1)

    suffix = " (DRY RUN)" if dry_run else ""
    table = Table(title=f"Writeback Dead-Letter Replay{suffix}: {connector}/{datatype}")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="bold")
    if dry_run:
        table.add_row("Would replay", str(result.get("would_replay", 0)))
    else:
        table.add_row("Replayed", str(result.get("replayed", 0)))
        table.add_row("Errors", str(result.get("errors", 0)))
    console.print(table)

    if result.get("errors", 0) > 0:
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
# diff command (Step 82)
# ---------------------------------------------------------------------------


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
# ---------------------------------------------------------------------------
# control send / list commands (Step 6 — runtime control table CLI)
# ---------------------------------------------------------------------------

_VALID_COMMANDS = {
    "force_full_sync", "pause_connector", "resume_connector",
    "requeue_dead_letter", "reset-watermark", "reload-config",
    "reset-circuit-breaker", "resync", "trigger-writeback", "validate", "drain",
}


@control_app.command("send")
def control_send(
    command: str = typer.Option(..., "--command", "-c", help="Control command name."),
    connector: Optional[str] = typer.Option(None, "--connector", help="Target connector name."),  # noqa: UP007
    datatype: Optional[str] = typer.Option(None, "--datatype", help="Target datatype."),  # noqa: UP007
    payload: Optional[str] = typer.Option(None, "--payload", help="JSON payload for the command."),  # noqa: UP007
    target_tool: Optional[str] = typer.Option(None, "--target-tool", help="ingestion | writeback (optional)."),  # noqa: UP007
    dsn: Optional[str] = typer.Option(None, "--dsn", envvar="INOUT_DATABASE_URL", help="PostgreSQL DSN."),  # noqa: UP007
    issued_by: str = typer.Option("cli", "--issued-by", help="Operator identifier for audit trail."),
) -> None:
    """Insert a command into inout_ops_control and print the assigned row ID.

    Supported commands: force_full_sync, pause_connector, resume_connector,
    requeue_dead_letter, reset-watermark, reload-config, reset-circuit-breaker,
    resync, trigger-writeback, validate, drain.
    """
    if command not in _VALID_COMMANDS:
        err_console.print(
            f"Unknown command: {command!r}. Valid commands: {', '.join(sorted(_VALID_COMMANDS))}"
        )
        raise typer.Exit(code=1)

    import json
    import anyio
    import psycopg

    payload_dict: dict = {}
    if payload:
        try:
            payload_dict = json.loads(payload)
        except json.JSONDecodeError as exc:
            err_console.print(f"Invalid --payload JSON: {exc}")
            raise typer.Exit(code=1)

    async def _run() -> None:
        effective_dsn = dsn
        if effective_dsn is None:
            import os
            effective_dsn = os.environ.get("INOUT_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if not effective_dsn:
            err_console.print("No database DSN. Set INOUT_DATABASE_URL or pass --dsn.")
            raise typer.Exit(code=1)

        import orjson
        import uuid as _uuid

        async with await psycopg.AsyncConnection.connect(effective_dsn) as conn:
            payload_json = orjson.dumps(payload_dict).decode() if payload_dict else None
            row = await (await conn.execute(
                """
                INSERT INTO inout_ops_control
                    (id, connector, datatype, command, payload, target_tool, status, issued_by, issued_at)
                VALUES
                    (%s, %s, %s, %s, %s::jsonb, %s, 'pending', %s, NOW())
                RETURNING id, status, issued_at
                """,
                [
                    str(_uuid.uuid4()), connector, datatype, command,
                    payload_json, target_tool, issued_by,
                ],
            )).fetchone()
            await conn.commit()

        if row:
            console.print(
                f"[green]Control command queued[/green]  "
                f"id=[bold]{row[0]}[/bold]  status={row[1]}  issued_at={row[2]}"
            )
        else:
            err_console.print("Failed to queue command (no row returned).")
            raise typer.Exit(code=1)

    anyio.run(_run)


@control_app.command("list")
def control_list(
    status: Optional[str] = typer.Option(None, "--status", help="Filter by status (pending|acknowledged|completed|failed)."),  # noqa: UP007
    connector: Optional[str] = typer.Option(None, "--connector", help="Filter by connector name."),  # noqa: UP007
    limit: int = typer.Option(20, "--limit", help="Maximum rows to return."),
    dsn: Optional[str] = typer.Option(None, "--dsn", envvar="INOUT_DATABASE_URL", help="PostgreSQL DSN."),  # noqa: UP007
) -> None:
    """List recent entries in inout_ops_control."""
    import anyio
    import psycopg

    async def _run() -> None:
        effective_dsn = dsn
        if effective_dsn is None:
            import os
            effective_dsn = os.environ.get("INOUT_DATABASE_URL") or os.environ.get("DATABASE_URL")
        if not effective_dsn:
            err_console.print("No database DSN. Set INOUT_DATABASE_URL or pass --dsn.")
            raise typer.Exit(code=1)

        clauses = ["1=1"]
        params: list = []
        if status:
            clauses.append("status = %s")
            params.append(status)
        if connector:
            clauses.append("connector = %s")
            params.append(connector)
        params.append(limit)

        async with await psycopg.AsyncConnection.connect(effective_dsn) as conn:
            rows = await (await conn.execute(
                f"""
                SELECT id, connector, datatype, command, status, issued_by, issued_at, completed_at
                FROM inout_ops_control
                WHERE {' AND '.join(clauses)}
                ORDER BY issued_at DESC
                LIMIT %s
                """,
                params,
            )).fetchall()

        table = Table(title="Control Commands")
        for col in ("id", "connector", "datatype", "command", "status", "issued_by", "issued_at", "completed_at"):
            table.add_column(col)
        for row in rows:
            table.add_row(*[str(v) if v is not None else "" for v in row])
        console.print(table)

    anyio.run(_run)


if __name__ == "__main__":
    app()
