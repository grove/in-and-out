"""Measures ingestion throughput: records/second for bulk upsert.
Uses a respx mock server returning synthetic pages.
"""
from __future__ import annotations

import json
import time
import uuid

import pytest

from .conftest import _docker_available

pytestmark = [
    pytest.mark.skipif(not _docker_available(), reason="Docker not available"),
    pytest.mark.load,
]


def _make_synthetic_records(count: int) -> list[dict]:
    return [
        {
            "external_id": str(i),
            "data": json.dumps({"id": str(i), "name": f"Record {i}", "value": i * 1.5}),
            "raw": json.dumps({"id": str(i)}),
        }
        for i in range(count)
    ]


@pytest.mark.anyio
async def test_ingestion_throughput_10k_records(pool, run_migrations):
    """Ingest 10,000 records across 100 pages of 100. Assert > 500 records/sec."""
    from inandout.ingestion.engine import _upsert_record
    from inandout.postgres.schema import ensure_source_table

    connector = "load_test_throughput"
    datatype = "widgets"
    run_id = uuid.uuid4()

    async with pool.connection() as conn:
        await ensure_source_table(conn, connector, datatype)
        await conn.commit()

    table = f"inout_src_{connector}_{datatype}"

    # Generate 10k records
    records = _make_synthetic_records(10_000)

    start_time = time.perf_counter()

    # Use bulk_upsert_records for realistic throughput measurement
    from inandout.postgres.bulk_upsert import bulk_upsert_records

    batch_size = 100
    total_inserted = 0
    total_updated = 0

    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        async with pool.connection() as conn:
            inserted, updated = await bulk_upsert_records(
                conn, table, batch, "external_id", run_id
            )
            await conn.commit()
        total_inserted += inserted
        total_updated += updated

    elapsed = time.perf_counter() - start_time
    total_records = total_inserted + total_updated
    throughput = total_records / elapsed if elapsed > 0 else float("inf")

    assert total_records == 10_000, f"Expected 10k records, got {total_records}"
    assert throughput > 500, (
        f"Throughput {throughput:.0f} rec/s < 500 rec/s target "
        f"(elapsed={elapsed:.2f}s)"
    )


@pytest.mark.anyio
async def test_ingestion_memory_stable_large_sync(pool, run_migrations):
    """Memory should not grow unboundedly during a 10k-record sync."""
    import tracemalloc

    from inandout.ingestion.engine import _upsert_record
    from inandout.postgres.schema import ensure_source_table

    connector = "load_test_memory"
    datatype = "records"
    run_id = uuid.uuid4()

    async with pool.connection() as conn:
        await ensure_source_table(conn, connector, datatype)
        await conn.commit()

    table = f"inout_src_{connector}_{datatype}"
    records = _make_synthetic_records(10_000)

    tracemalloc.start()

    from inandout.postgres.bulk_upsert import bulk_upsert_records

    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        async with pool.connection() as conn:
            await bulk_upsert_records(conn, table, batch, "external_id", run_id)
            await conn.commit()

    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak / (1024 * 1024)
    assert peak_mb < 50, (
        f"Peak memory {peak_mb:.1f} MB exceeds 50 MB budget"
    )
