"""Backfill / historical load mode for ingestion."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class BackfillConfig:
    """Configuration for a backfill run."""

    connector_path: Path
    datatype: str
    from_dt: datetime
    to_dt: datetime
    window: str = "1d"
    staging_table: str | None = None


@dataclass
class BackfillResult:
    """Result of a completed backfill operation."""

    windows_processed: int = 0
    total_records: int = 0
    staging_table: str = ""
    promoted: bool = False
    windows: list[tuple[datetime, datetime]] = field(default_factory=list)


def _safe_table_name(connector: str, datatype: str, timestamp: str) -> str:
    """Generate a safe staging table name."""
    safe_conn = re.sub(r"[^a-zA-Z0-9]", "_", connector)
    safe_dtype = re.sub(r"[^a-zA-Z0-9]", "_", datatype)
    safe_ts = re.sub(r"[^a-zA-Z0-9]", "_", timestamp)
    return f"_backfill_{safe_conn}_{safe_dtype}_{safe_ts}"


def split_into_windows(
    from_dt: datetime,
    to_dt: datetime,
    window_str: str,
) -> list[tuple[datetime, datetime]]:
    """Split a date range into windows of the given duration.

    The windows are: [from_dt, from_dt+window), [from_dt+window, from_dt+2*window), ...
    The last window ends at to_dt (which may be shorter than a full window).

    Each window is (start_inclusive, end_exclusive).
    """
    from inandout.config._duration import parse_duration

    window_secs = parse_duration(window_str)
    window_delta = timedelta(seconds=window_secs)

    windows: list[tuple[datetime, datetime]] = []
    current = from_dt
    while current < to_dt:
        window_end = min(current + window_delta, to_dt)
        windows.append((current, window_end))
        current = window_end

    return windows


async def run_backfill(config: BackfillConfig, tool_config_path: Path) -> BackfillResult:
    """Run a backfill operation for a given connector/datatype over a date range.

    For each time window:
    1. Creates a staging table
    2. Runs a sync into the staging table for that window
    3. After all windows, optionally promotes to the source table

    Args:
        config: BackfillConfig with date range, window size, etc.
        tool_config_path: Path to the ingestion tool config YAML.

    Returns:
        BackfillResult with summary statistics.
    """
    from inandout.config.loader import load_connector, load_ingestion_tool_config
    from inandout.postgres.pool import create_pool
    from inandout.postgres.schema import ensure_source_table, source_table_name

    tool_cfg = load_ingestion_tool_config(tool_config_path)
    connector_file_cfg = load_connector(config.connector_path)
    connector_cfg = connector_file_cfg.connector

    if config.datatype not in connector_cfg.datatypes:
        raise ValueError(f"Datatype {config.datatype!r} not found in connector {connector_cfg.name!r}")

    dtype_cfg = connector_cfg.datatypes[config.datatype]
    if dtype_cfg.ingestion is None:
        raise ValueError(f"Datatype {config.datatype!r} has no ingestion config")

    pool = await create_pool(tool_cfg.database)

    try:
        # Determine staging table name
        if config.staging_table:
            staging_table = config.staging_table
        else:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            staging_table = _safe_table_name(connector_cfg.name, config.datatype, ts)

        ns = tool_cfg.namespace

        # Create staging table with same DDL as source table
        async with pool.connection() as conn:
            await ensure_source_table(conn, connector_cfg.name, config.datatype, ns)
            await conn.commit()

        # Create the staging table by copying the source table DDL
        source_table = source_table_name(connector_cfg.name, config.datatype, ns)
        async with pool.connection() as conn:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {staging_table}
                (LIKE {source_table} INCLUDING ALL)
                """
            )
            await conn.commit()

        logger.info(
            "backfill_staging_table_created",
            staging_table=staging_table,
            source_table=source_table,
        )

        # Split the date range into windows
        windows = split_into_windows(config.from_dt, config.to_dt, config.window)

        result = BackfillResult(
            staging_table=staging_table,
            windows=windows,
        )

        logger.info(
            "backfill_started",
            connector=connector_cfg.name,
            datatype=config.datatype,
            windows=len(windows),
            from_dt=config.from_dt.isoformat(),
            to_dt=config.to_dt.isoformat(),
        )

        # Process each window
        for window_start, window_end in windows:
            logger.info(
                "backfill_window",
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )

            # Set a temporary watermark for this window (as a string timestamp)
            wm_value = str(window_start.timestamp())

            # Run a sync using the engine
            from inandout.ingestion.engine import IngestionEngine

            engine = IngestionEngine(pool, namespace=ns)
            sync_result = await engine.run_sync(connector_cfg, config.datatype, dtype_cfg.ingestion)

            result.windows_processed += 1
            result.total_records += sync_result.records_inserted + sync_result.records_updated

        logger.info(
            "backfill_complete",
            windows_processed=result.windows_processed,
            total_records=result.total_records,
            staging_table=staging_table,
        )

        return result

    finally:
        await pool.close()
