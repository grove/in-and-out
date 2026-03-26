"""Seed the demo simulator store from connector manifest seed_data."""

from __future__ import annotations

from inandout.config.connector import ConnectorConfig
from inandout.simulator.store import RecordStore


def _pk_field(primary_key) -> str:
    if isinstance(primary_key, str):
        return primary_key
    if isinstance(primary_key, list) and primary_key:
        return primary_key[0]
    return "id"


async def seed_from_connector(
    store: RecordStore,
    connector: ConnectorConfig,
) -> None:
    """Load seed_data declared in each datatype's connector manifest."""
    for dt_name, dt_cfg in connector.datatypes.items():
        if not dt_cfg.seed_data:
            continue
        pk_field = _pk_field(dt_cfg.ingestion.primary_key if dt_cfg.ingestion else "id")
        cursor_field: str | None = None
        if dt_cfg.ingestion and dt_cfg.ingestion.list.incremental:
            cursor_field = dt_cfg.ingestion.list.incremental.cursor_field
        await store.seed(
            connector.name,
            dt_name,
            dt_cfg.seed_data,
            pk_field=pk_field,
            cursor_field=cursor_field,
        )
