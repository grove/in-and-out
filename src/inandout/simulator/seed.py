"""Seed the demo simulator store from connector manifest seed_data."""

from __future__ import annotations

import copy
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from inandout.config.connector import ConnectorConfig
from inandout.simulator.store import RecordStore


def _pk_field(primary_key) -> str:
    if isinstance(primary_key, str):
        return primary_key
    if isinstance(primary_key, list) and primary_key:
        return primary_key[0]
    return "id"


def _bump_value(value: Any, index: int) -> Any:
    """Return a variant of *value* offset by *index* for synthetic expansion."""
    if isinstance(value, str):
        # ISO timestamps: add index days
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            bumped = dt + timedelta(days=index)
            # Preserve the original format (with Z suffix if present)
            if value.endswith("Z"):
                return bumped.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            return bumped.isoformat()
        except ValueError:
            pass
        # Numeric string ID (e.g. "1001"): increment
        if re.fullmatch(r"\d+", value):
            return str(int(value) + index)
    if isinstance(value, dict):
        return {k: _bump_value(v, index) for k, v in value.items()}
    if isinstance(value, list):
        return [_bump_value(v, index) for v in value]
    if isinstance(value, (int, float)):
        return value + index
    return value


def _expand_seed(template: dict[str, Any], pk_field: str, count: int) -> list[dict[str, Any]]:
    """Return *count* records derived from *template* by bumping numeric / timestamp fields."""
    records = [template]
    for i in range(1, count):
        rec = _bump_value(copy.deepcopy(template), i)
        records.append(rec)
    return records


async def seed_from_connector(
    store: RecordStore,
    connector: ConnectorConfig,
) -> None:
    """Load seed_data declared in each datatype's connector manifest."""
    for dt_name, dt_cfg in connector.datatypes.items():
        sim = dt_cfg.simulator
        if not sim or not sim.seed_data:
            continue
        pk_field = _pk_field(dt_cfg.ingestion.primary_key if dt_cfg.ingestion else "id")
        cursor_field: str | None = None
        if dt_cfg.ingestion and dt_cfg.ingestion.list.incremental:
            cursor_field = dt_cfg.ingestion.list.incremental.cursor_field

        records = sim.seed_data
        # Auto-expand when there is exactly one template record and seed_count > 1
        if len(records) == 1 and sim.seed_count > 1:
            records = _expand_seed(records[0], pk_field, sim.seed_count)

        await store.seed(
            connector.name,
            dt_name,
            records,
            pk_field=pk_field,
            cursor_field=cursor_field,
        )
