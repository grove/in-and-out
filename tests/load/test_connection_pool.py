"""
Tests connection pool behaviour under concurrent load.
"""
from __future__ import annotations

import uuid

import anyio
import pytest

from .conftest import _docker_available

pytestmark = [
    pytest.mark.skipif(not _docker_available(), reason="Docker not available"),
    pytest.mark.load,
]


def _make_records(count: int, offset: int = 0) -> list[dict]:
    return [
        {"id": str(offset + i), "data": f"record_{offset + i}"}
        for i in range(count)
    ]


@pytest.mark.anyio
async def test_concurrent_connectors_share_pool(pool, run_migrations):
    """5 concurrent connectors each doing a 1k-record sync should not exhaust pool."""
    from inandout.postgres.bulk_upsert import bulk_upsert_records
    from inandout.postgres.schema import ensure_source_table

    n_connectors = 5
    records_per_connector = 1_000

    # Set up tables
    for i in range(n_connectors):
        async with pool.connection() as conn:
            await ensure_source_table(conn, f"load_concurrent_{i}", "items")
            await conn.commit()

    errors = []
    completed = []

    async def _sync_one(connector_idx: int) -> None:
        connector = f"load_concurrent_{connector_idx}"
        datatype = "items"
        table = f"inout_src_{connector}_{datatype}"
        run_id = uuid.uuid4()
        records = _make_records(records_per_connector, offset=connector_idx * records_per_connector)

        try:
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i: i + batch_size]
                async with pool.connection() as conn:
                    await bulk_upsert_records(conn, table, batch, "id", run_id)
                    await conn.commit()
            completed.append(connector_idx)
        except Exception as exc:
            errors.append((connector_idx, str(exc)))

    # Run all 5 connectors concurrently
    async with anyio.create_task_group() as tg:
        for idx in range(n_connectors):
            tg.start_soon(_sync_one, idx)

    assert not errors, f"Errors during concurrent sync: {errors}"
    assert len(completed) == n_connectors, (
        f"Only {len(completed)}/{n_connectors} connectors completed"
    )
