"""Integration tests for PostgresStore using testcontainers.

A Docker daemon is required.  The tests are skipped automatically when Docker
is not available in the current environment (e.g. a dev container without the
Docker socket mounted).

Run explicitly::

    pytest simulator/tests/test_simulator_postgres_store.py -v
"""

from __future__ import annotations

import asyncio
import os

import pytest

from inandout_simulator.store.postgres import PostgresStore

C = "acme"
D = "contacts"


# ---------------------------------------------------------------------------
# Session-scoped container — one PG instance shared across the whole test run
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container():
    """Start a PostgreSQL 16 container.  Skips if Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer(image="postgres:16-alpine", driver=None)
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker unavailable — skipping PostgresStore tests: {exc}")

    yield container
    container.stop()


@pytest.fixture
async def store(pg_container):
    """Isolated store: each test gets its own schema, dropped afterwards."""
    from psycopg import AsyncConnection

    base_dsn = pg_container.get_connection_url()  # driver=None → plain postgresql://

    # Unique schema per test so parallel/sequential tests don't collide.
    schema = "sim_test_" + os.urandom(4).hex()
    conn = await AsyncConnection.connect(base_dsn, autocommit=True)
    try:
        await conn.execute(f"CREATE SCHEMA {schema}")
    finally:
        await conn.close()

    # psycopg accepts ?options= to set search_path without altering the DB.
    dsn = f"{base_dsn}?options=-csearch_path%3D{schema}"
    s = PostgresStore(dsn)
    yield s

    # Teardown: close the pool, then drop the schema.
    if s._pool is not None:
        await s._pool.close()
    conn = await AsyncConnection.connect(base_dsn, autocommit=True)
    try:
        await conn.execute(f"DROP SCHEMA {schema} CASCADE")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


async def test_create_returns_record_with_pk(store: PostgresStore) -> None:
    record = await store.create(C, D, {"id": "1", "name": "Alice"})
    assert record["id"] == "1"
    assert record["name"] == "Alice"


async def test_create_autogenerates_id_when_missing(store: PostgresStore) -> None:
    record = await store.create(C, D, {"name": "Bob"})
    assert "id" in record
    assert record["id"]


async def test_create_emits_mutation_event(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1", "name": "Alice"})
    mutations = await store.recent_mutations(C, D)
    assert len(mutations) == 1
    assert mutations[0].operation == "create"
    assert mutations[0].record_id == "1"


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------


async def test_get_by_id_returns_record(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "42", "name": "Carol"})
    record = await store.get_by_id(C, D, "42")
    assert record is not None
    assert record["name"] == "Carol"


async def test_get_by_id_returns_none_for_missing(store: PostgresStore) -> None:
    result = await store.get_by_id(C, D, "nonexistent")
    assert result is None


async def test_get_by_id_injects_meta_keys(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    record = await store.get_by_id(C, D, "1")
    assert record is not None
    assert "__created_at__" in record
    assert "__modified_at__" in record
    assert "__deleted_at__" not in record


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


async def test_update_merges_fields(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1", "name": "Alice", "email": "a@b.com"})
    result = await store.update(C, D, "1", {"name": "Alicia"})
    assert result is not None
    assert result["name"] == "Alicia"
    assert result["email"] == "a@b.com"


async def test_update_returns_none_for_missing(store: PostgresStore) -> None:
    result = await store.update(C, D, "ghost", {"name": "Nope"})
    assert result is None


async def test_update_emits_mutation_with_before_state(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1", "score": 10})
    await store.update(C, D, "1", {"score": 20})
    mutations = await store.recent_mutations(C, D)
    update_ev = next(m for m in mutations if m.operation == "update")
    assert update_ev.before is not None
    assert update_ev.before["score"] == 10
    assert update_ev.record is not None
    assert update_ev.record["score"] == 20


# ---------------------------------------------------------------------------
# delete / restore
# ---------------------------------------------------------------------------


async def test_delete_soft_deletes(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    deleted = await store.delete(C, D, "1")
    assert deleted is True
    record = await store.get_by_id(C, D, "1")
    assert record is not None
    assert "__deleted_at__" in record


async def test_delete_returns_false_for_missing(store: PostgresStore) -> None:
    assert await store.delete(C, D, "ghost") is False


async def test_delete_returns_false_when_already_deleted(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.delete(C, D, "1")
    assert await store.delete(C, D, "1") is False


async def test_restore_clears_deleted_at(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.delete(C, D, "1")
    result = await store.restore(C, D, "1")
    assert result is not None
    record = await store.get_by_id(C, D, "1")
    assert record is not None
    assert "__deleted_at__" not in record


async def test_restore_returns_none_for_missing(store: PostgresStore) -> None:
    result = await store.restore(C, D, "nonexistent")
    assert result is None


# ---------------------------------------------------------------------------
# list_all
# ---------------------------------------------------------------------------


async def test_list_all_excludes_deleted_by_default(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create(C, D, {"id": "2"})
    await store.delete(C, D, "1")
    records = await store.list_all(C, D)
    ids = [r["id"] for r in records]
    assert "2" in ids
    assert "1" not in ids


async def test_list_all_includes_deleted_when_requested(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.delete(C, D, "1")
    records = await store.list_all(C, D, include_deleted=True)
    assert any(r["id"] == "1" and "__deleted_at__" in r for r in records)


async def test_list_all_watermark_filter(store: PostgresStore) -> None:
    await store.seed(
        C,
        D,
        [
            {"id": "1", "updated_at": "2024-01-01T00:00:00"},
            {"id": "2", "updated_at": "2024-06-01T00:00:00"},
            {"id": "3", "updated_at": "2024-12-01T00:00:00"},
        ],
        cursor_field="updated_at",
    )
    records = await store.list_all(
        C, D, cursor_field="updated_at", watermark="2024-03-01T00:00:00"
    )
    ids = [r["id"] for r in records]
    assert "1" not in ids
    assert "2" in ids
    assert "3" in ids


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


async def test_count_excludes_deleted(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create(C, D, {"id": "2"})
    await store.delete(C, D, "1")
    assert await store.count(C, D) == 1


# ---------------------------------------------------------------------------
# seed (idempotent)
# ---------------------------------------------------------------------------


async def test_seed_is_idempotent(store: PostgresStore) -> None:
    records = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    await store.seed(C, D, records)
    await store.seed(C, D, records)  # second seed must not duplicate
    assert await store.count(C, D) == 2


# ---------------------------------------------------------------------------
# recent_mutations
# ---------------------------------------------------------------------------


async def test_recent_mutations_newest_first(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await asyncio.sleep(0.01)
    await store.create(C, D, {"id": "2"})
    mutations = await store.recent_mutations(C, D)
    assert mutations[0].record_id == "2"
    assert mutations[1].record_id == "1"


async def test_recent_mutations_filter_by_connector(store: PostgresStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create("other", D, {"id": "2"})
    mutations = await store.recent_mutations(connector=C)
    assert all(m.connector == C for m in mutations)
