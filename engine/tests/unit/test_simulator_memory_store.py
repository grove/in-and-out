"""Unit tests for the simulator MemoryStore."""

from __future__ import annotations

import asyncio

import pytest

from inandout.simulator.store.memory import MemoryStore

C = "acme"
D = "contacts"


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


async def test_create_returns_record_with_pk(store: MemoryStore) -> None:
    record = await store.create(C, D, {"id": "1", "name": "Alice"})
    assert record["id"] == "1"
    assert record["name"] == "Alice"


async def test_create_autogenerates_id_when_missing(store: MemoryStore) -> None:
    record = await store.create(C, D, {"name": "Bob"})
    assert "id" in record
    assert record["id"]  # non-empty


async def test_create_emits_mutation_event(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1", "name": "Alice"})
    mutations = await store.recent_mutations(C, D)
    assert len(mutations) == 1
    assert mutations[0].operation == "create"
    assert mutations[0].record_id == "1"


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------


async def test_get_by_id_returns_record(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "42", "name": "Carol"})
    record = await store.get_by_id(C, D, "42")
    assert record is not None
    assert record["name"] == "Carol"


async def test_get_by_id_returns_none_for_missing(store: MemoryStore) -> None:
    result = await store.get_by_id(C, D, "nonexistent")
    assert result is None


async def test_get_by_id_injects_meta_keys(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    record = await store.get_by_id(C, D, "1")
    assert record is not None
    assert "__created_at__" in record
    assert "__modified_at__" in record
    assert "__deleted_at__" not in record


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


async def test_update_merges_fields(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1", "name": "Alice", "email": "a@b.com"})
    result = await store.update(C, D, "1", {"name": "Alicia"})
    assert result is not None
    assert result["name"] == "Alicia"
    assert result["email"] == "a@b.com"  # unchanged field preserved


async def test_update_returns_none_for_missing(store: MemoryStore) -> None:
    result = await store.update(C, D, "ghost", {"name": "Nope"})
    assert result is None


async def test_update_preserves_created_at(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    before = await store.get_by_id(C, D, "1")
    assert before is not None
    created_at = before["__created_at__"]

    await store.update(C, D, "1", {"name": "Updated"})
    after = await store.get_by_id(C, D, "1")
    assert after is not None
    assert after["__created_at__"] == created_at


async def test_update_advances_modified_at(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    before = await store.get_by_id(C, D, "1")
    assert before is not None

    await asyncio.sleep(0.01)  # ensure clock advances
    await store.update(C, D, "1", {"flag": True})
    after = await store.get_by_id(C, D, "1")
    assert after is not None
    assert after["__modified_at__"] >= before["__modified_at__"]


async def test_update_emits_mutation_with_before_state(store: MemoryStore) -> None:
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


async def test_delete_soft_deletes(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    deleted = await store.delete(C, D, "1")
    assert deleted is True
    record = await store.get_by_id(C, D, "1")
    assert record is not None
    assert "__deleted_at__" in record


async def test_delete_returns_false_for_missing(store: MemoryStore) -> None:
    assert await store.delete(C, D, "ghost") is False


async def test_delete_returns_false_when_already_deleted(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.delete(C, D, "1")
    assert await store.delete(C, D, "1") is False


async def test_restore_clears_deleted_at(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.delete(C, D, "1")
    restored = await store.restore(C, D, "1")
    assert restored is not None
    record = await store.get_by_id(C, D, "1")
    assert record is not None
    assert "__deleted_at__" not in record


async def test_restore_returns_none_for_missing(store: MemoryStore) -> None:
    result = await store.restore(C, D, "ghost")
    assert result is None


# ---------------------------------------------------------------------------
# list_all
# ---------------------------------------------------------------------------


async def test_list_all_excludes_deleted_by_default(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create(C, D, {"id": "2"})
    await store.delete(C, D, "2")
    records = await store.list_all(C, D)
    ids = {r["id"] for r in records}
    assert ids == {"1"}


async def test_list_all_includes_deleted_with_flag(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create(C, D, {"id": "2"})
    await store.delete(C, D, "2")
    records = await store.list_all(C, D, include_deleted=True)
    assert len(records) == 2


async def test_list_all_incremental_cursor_filter(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1", "updated_at": "2024-01-01T00:00:00"})
    await store.create(C, D, {"id": "2", "updated_at": "2024-06-01T00:00:00"})
    await store.create(C, D, {"id": "3", "updated_at": "2024-12-01T00:00:00"})
    records = await store.list_all(C, D, cursor_field="updated_at", watermark="2024-03-01T00:00:00")
    ids = {r["id"] for r in records}
    assert ids == {"2", "3"}


async def test_list_all_injects_meta_keys(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    records = await store.list_all(C, D)
    assert len(records) == 1
    assert "__modified_at__" in records[0]
    assert "__created_at__" in records[0]


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


async def test_count_excludes_deleted(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create(C, D, {"id": "2"})
    await store.delete(C, D, "1")
    assert await store.count(C, D) == 1


async def test_count_is_zero_for_empty_namespace(store: MemoryStore) -> None:
    assert await store.count(C, D) == 0


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------


async def test_seed_loads_records(store: MemoryStore) -> None:
    await store.seed(C, D, [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}])
    records = await store.list_all(C, D)
    assert len(records) == 2


async def test_seed_is_idempotent(store: MemoryStore) -> None:
    batch = [{"id": "1", "name": "Alice"}]
    await store.seed(C, D, batch)
    await store.seed(C, D, batch)  # second call must not duplicate
    assert await store.count(C, D) == 1


async def test_seed_uses_cursor_field_as_modified_at(store: MemoryStore) -> None:
    ts = "2024-07-15T12:00:00+00:00"
    await store.seed(C, D, [{"id": "1", "updated_at": ts}], cursor_field="updated_at")
    # Incremental filter against that watermark must return nothing (ts is not > ts).
    records = await store.list_all(C, D, cursor_field="updated_at", watermark=ts)
    assert records == []


# ---------------------------------------------------------------------------
# recent_mutations
# ---------------------------------------------------------------------------


async def test_recent_mutations_newest_first(store: MemoryStore) -> None:
    await store.create(C, D, {"id": "1"})
    await store.create(C, D, {"id": "2"})
    mutations = await store.recent_mutations(C, D)
    assert mutations[0].record_id == "2"
    assert mutations[1].record_id == "1"


async def test_recent_mutations_filtered_by_connector(store: MemoryStore) -> None:
    await store.create("acme", D, {"id": "1"})
    await store.create("other", D, {"id": "2"})
    mutations = await store.recent_mutations("acme")
    assert all(m.connector == "acme" for m in mutations)


async def test_recent_mutations_limit_respected(store: MemoryStore) -> None:
    for i in range(10):
        await store.create(C, D, {"id": str(i)})
    mutations = await store.recent_mutations(C, D, limit=3)
    assert len(mutations) == 3


# ---------------------------------------------------------------------------
# namespace isolation
# ---------------------------------------------------------------------------


async def test_different_datatypes_are_isolated(store: MemoryStore) -> None:
    await store.create(C, "contacts", {"id": "1"})
    await store.create(C, "accounts", {"id": "1"})
    contacts = await store.list_all(C, "contacts")
    accounts = await store.list_all(C, "accounts")
    assert len(contacts) == 1
    assert len(accounts) == 1
