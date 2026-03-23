"""CLI commands for in-and-out."""

from __future__ import annotations

import typer

app = typer.Typer(
    name="inandout",
    help="In-and-Out: declarative MDM HTTP API synchronization tools.",
    no_args_is_help=True,
)

ingest_app = typer.Typer(
    name="ingest",
    help="Run the ingestion daemon.",
    no_args_is_help=True,
)

writeback_app = typer.Typer(
    name="writeback",
    help="Run the writeback daemon.",
    no_args_is_help=True,
)

app.add_typer(ingest_app, name="ingest")
app.add_typer(writeback_app, name="writeback")


@app.command()
def version() -> None:
    """Print the installed version."""
    from inandout import __version__

    typer.echo(__version__)


@ingest_app.command("run")
def ingest_run(
    config: str = typer.Option("config/ingestion.yaml", help="Path to ingestion tool config."),
) -> None:
    """Start the ingestion daemon."""
    typer.echo(f"Starting ingestion daemon with config: {config}")
    raise NotImplementedError("Ingestion daemon not yet implemented.")


@ingest_app.command("validate")
def ingest_validate(
    connector: str = typer.Argument(help="Connector name or path to connector YAML."),
    connectors_dir: str = typer.Option("connectors/", help="Connector config directory."),
) -> None:
    """Validate a connector configuration (connectivity + auth dry-run)."""
    typer.echo(f"Validating connector: {connector}")
    raise NotImplementedError("Connector validation not yet implemented.")


@writeback_app.command("run")
def writeback_run(
    config: str = typer.Option("config/writeback.yaml", help="Path to writeback tool config."),
) -> None:
    """Start the writeback daemon."""
    typer.echo(f"Starting writeback daemon with config: {config}")
    raise NotImplementedError("Writeback daemon not yet implemented.")


if __name__ == "__main__":
    app()
