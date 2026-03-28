"""Dead-letter transform script runner."""
from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class TransformResult:
    """Result of applying a transform script to dead-letter records."""

    processed: int = 0
    upserted: int = 0
    dropped: int = 0
    failed: int = 0


def _load_transform_function(script_path: Path) -> Any:
    """Load the 'transform' async function from the given Python script file.

    The script must define: async def transform(record: dict) -> dict | None

    Returns:
        The transform callable.

    Raises:
        AttributeError: If the script doesn't define a 'transform' function.
    """
    spec = importlib.util.spec_from_file_location("_dl_transform", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load script: {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    transform_fn = getattr(module, "transform", None)
    if transform_fn is None:
        raise AttributeError(
            f"Script {script_path} must define 'async def transform(record: dict) -> dict | None'"
        )
    return transform_fn


async def apply_transform_script(
    pool: Any,
    connector: str,
    datatype: str,
    script_path: Path,
    dry_run: bool = False,
) -> TransformResult:
    """Apply a transform script to all dead-letter rows for a connector/datatype.

    The script must export: async def transform(record: dict) -> dict | None
    - If transform() returns a dict: the record is upserted into the source table.
    - If transform() returns None: the record is dropped (not upserted).
    - Dead-letter rows that are successfully processed are marked as requeued in the DB.

    Args:
        pool: AsyncConnectionPool
        connector: Connector name.
        datatype: Datatype name.
        script_path: Path to the Python transform script.
        dry_run: If True, show what would happen without writing.

    Returns:
        TransformResult with counts.
    """
    from inandout.deadletter.inspect import fetch_dead_letter_rows
    from inandout.postgres.schema import dead_letter_table_name, source_table_name

    result = TransformResult()

    # Load the transform function
    transform_fn = _load_transform_function(script_path)

    # Fetch dead-letter rows
    rows = await fetch_dead_letter_rows(pool, connector, datatype, limit=10000)

    dl_table = dead_letter_table_name("ingestion", connector, datatype)
    src_table = source_table_name(connector, datatype)

    for row in rows:
        result.processed += 1
        row_id = row["id"]

        # Parse raw JSON
        raw = row["raw"]
        if isinstance(raw, str):
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                record = {"_raw": raw}
        elif isinstance(raw, dict):
            record = raw
        else:
            record = {}

        try:
            transformed = await transform_fn(record)
        except Exception as exc:
            logger.warning(
                "dl_transform_script_error",
                row_id=row_id,
                connector=connector,
                datatype=datatype,
                error=str(exc),
            )
            result.failed += 1
            continue

        if transformed is None:
            result.dropped += 1
            logger.debug("dl_transform_dropped", row_id=row_id)
            if not dry_run:
                # Mark as requeued with drop status
                try:
                    async with pool.connection() as conn:
                        await conn.execute(
                            f"UPDATE {dl_table} SET requeue_count = requeue_count + 1 "
                            f"WHERE id = %s",
                            [row_id],
                        )
                        await conn.commit()
                except Exception as exc:
                    logger.warning("dl_mark_requeued_error", row_id=row_id, error=str(exc))
        else:
            result.upserted += 1
            logger.debug("dl_transform_upsert", row_id=row_id)
            if not dry_run:
                try:
                    import orjson
                    external_id = str(row.get("external_id") or row_id)
                    data = orjson.dumps(transformed).decode()
                    async with pool.connection() as conn:
                        await conn.execute(
                            f"""
                            INSERT INTO {src_table}
                                (external_id, data, raw, _ingested_at, _raw_hash)
                            VALUES (%s, %s, %s, NOW(), 'dl_reprocessed')
                            ON CONFLICT (external_id) DO UPDATE
                            SET data = EXCLUDED.data,
                                raw = EXCLUDED.raw,
                                _ingested_at = NOW(),
                                _deleted_at = NULL
                            """,
                            [external_id, data, data],
                        )
                        await conn.execute(
                            f"UPDATE {dl_table} SET requeue_count = requeue_count + 1 "
                            f"WHERE id = %s",
                            [row_id],
                        )
                        await conn.commit()
                except Exception as exc:
                    logger.warning("dl_upsert_error", row_id=row_id, error=str(exc))
                    result.upserted -= 1
                    result.failed += 1

    logger.info(
        "dl_transform_complete",
        connector=connector,
        datatype=datatype,
        processed=result.processed,
        upserted=result.upserted,
        dropped=result.dropped,
        failed=result.failed,
        dry_run=dry_run,
    )
    return result
