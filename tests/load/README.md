# Load Tests

These tests measure throughput, memory, and connection-pool behaviour under sustained load.
They require Docker (PostgreSQL) and are excluded from regular CI.

Run with: pytest tests/load/ -v --timeout=300

## Requirements

- Docker available and running
- PostgreSQL testcontainer (auto-provisioned)
- At least 4 GB free RAM for the 50 MB memory budget test

## What these tests verify

- `test_ingestion_throughput.py` — records/second for bulk upsert at 10k scale
- `test_connection_pool.py` — connection pool behaviour under concurrent connector load

## CI exclusion

These tests are marked with `pytest.mark.load` and are excluded from standard CI.
Run them manually before major releases or infrastructure changes.
