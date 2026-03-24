"""Dry-run connector execution — fetches one page but writes nothing to DB."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

from inandout.config.connector import ConnectorConfig, DatatypeConfig

logger = structlog.get_logger(__name__)


@dataclass
class DryRunResult:
    """Result of a dry-run connector execution."""

    datatype: str
    records: list[dict] = field(default_factory=list)
    applied_mappings: int = 0
    applied_hooks: int = 0
    would_insert: int = 0
    env: str = "production"


async def dry_run_connector(
    connector_cfg: ConnectorConfig,
    datatype: str,
    limit: int = 10,
    env: str = "production",
) -> DryRunResult:
    """Fetch one page from the connector API and return a DryRunResult.

    No data is written to the database.

    Parameters
    ----------
    connector_cfg:
        The connector configuration object.
    datatype:
        Name of the datatype to test.
    limit:
        Maximum number of records to fetch.
    env:
        'production' uses ``connection.base_url``; 'staging' uses
        ``connection.staging_base_url`` (raises if not configured).

    Returns
    -------
    DryRunResult
    """
    if datatype not in connector_cfg.datatypes:
        raise ValueError(f"Datatype '{datatype}' not found in connector '{connector_cfg.name}'")

    dtype_cfg: DatatypeConfig = connector_cfg.datatypes[datatype]
    if dtype_cfg.ingestion is None:
        raise ValueError(f"Datatype '{datatype}' has no ingestion configuration")

    ingestion_cfg = dtype_cfg.ingestion

    # Resolve environment base URL
    if env == "staging":
        if connector_cfg.connection.staging_base_url is None:
            raise ValueError(
                f"Connector '{connector_cfg.name}' has no staging_base_url configured. "
                "Set connection.staging_base_url in the connector YAML."
            )
        # Build a patched connector config using the staging URL
        connector_for_fetch = _patch_base_url(
            connector_cfg, connector_cfg.connection.staging_base_url
        )
    else:
        connector_for_fetch = connector_cfg

    from inandout.ingestion.field_mapper import apply_field_mappings
    from inandout.ingestion.engine import _extract_external_id
    from inandout.transport.http import HttpTransportAdapter

    result = DryRunResult(datatype=datatype, env=env)

    async with HttpTransportAdapter(connector_for_fetch) as transport:
        async for page in transport.fetch_pages(ingestion_cfg.list, watermark=None):
            records = page[:limit]
            for record in records:
                original_keys = set(record.keys())

                # Apply field mappings
                if dtype_cfg.field_mappings:
                    record = apply_field_mappings(
                        record,
                        dtype_cfg.field_mappings,
                        strict=dtype_cfg.strict_field_mapping,
                    )
                    result.applied_mappings += 1

                # Apply plugin hooks (no DB pool in dry-run)
                result.records.append(record)

                ext_id = _extract_external_id(record, ingestion_cfg.primary_key)
                if ext_id is not None:
                    result.would_insert += 1

            break  # Only one page in dry-run

    logger.info(
        "dry_run_complete",
        connector=connector_cfg.name,
        datatype=datatype,
        env=env,
        records=len(result.records),
        applied_mappings=result.applied_mappings,
        applied_hooks=result.applied_hooks,
        would_insert=result.would_insert,
    )
    return result


def _patch_base_url(connector_cfg: ConnectorConfig, new_base_url: str) -> ConnectorConfig:
    """Return a shallow copy of *connector_cfg* with a different base_url."""
    from inandout.config.connector import ConnectionConfig

    patched_connection = connector_cfg.connection.model_copy(
        update={"base_url": new_base_url}
    )
    return connector_cfg.model_copy(update={"connection": patched_connection})
