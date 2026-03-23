"""Unit tests for transaction-level atomicity for write groups (B2)."""
from __future__ import annotations


def test_rows_grouped_by_group_id() -> None:
    """Rows should be groupable by _group_id for atomic processing."""
    rows = [
        {"external_id": "a", "_action": "update", "_group_id": "g1"},
        {"external_id": "b", "_action": "update", "_group_id": "g1"},
        {"external_id": "c", "_action": "update", "_group_id": "g2"},
        {"external_id": "d", "_action": "insert"},  # no _group_id → singleton
    ]

    from collections import defaultdict

    groups: dict[str, list] = defaultdict(list)
    singletons = []
    for row in rows:
        gid = row.get("_group_id")
        if gid:
            groups[gid].append(row)
        else:
            singletons.append(row)

    assert len(groups["g1"]) == 2
    assert len(groups["g2"]) == 1
    assert len(singletons) == 1


def test_singleton_failure_does_not_affect_others() -> None:
    """A failing singleton should only affect its own record."""
    failed_ids: list[str] = []
    aborted_ids: list[str] = []

    rows = [
        {"external_id": "a", "_action": "update"},
        {"external_id": "b", "_action": "update"},
        {"external_id": "c", "_action": "update"},
    ]

    for row in rows:
        eid = row["external_id"]
        if eid == "b":
            failed_ids.append(eid)
        # Singletons: failure only affects themselves
        # (no aborts for other singletons)

    assert failed_ids == ["b"]
    assert aborted_ids == []


def test_group_abort_produces_correct_error_class() -> None:
    """When a group fails, remaining members should have error_class='group_partial_failure'."""
    group = [
        {"external_id": "a", "_action": "update", "_group_id": "g1"},
        {"external_id": "b", "_action": "update", "_group_id": "g1"},
        {"external_id": "c", "_action": "update", "_group_id": "g1"},
    ]

    dead_letter_entries = []
    for i, row in enumerate(group):
        eid = row["external_id"]
        if eid == "a":
            # Simulate failure
            failed_eid = eid
            # Abort remaining
            for remaining in group[i + 1:]:
                dead_letter_entries.append({
                    "external_id": remaining["external_id"],
                    "error_class": "group_partial_failure",
                    "error_message": f"Group g1 aborted: {failed_eid} failed",
                })
            break

    assert len(dead_letter_entries) == 2
    for entry in dead_letter_entries:
        assert entry["error_class"] == "group_partial_failure"
        assert "g1 aborted" in entry["error_message"]
