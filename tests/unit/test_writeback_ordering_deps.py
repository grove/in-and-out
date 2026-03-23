"""Unit tests for write batch dependency ordering (T2 #26)."""
from __future__ import annotations

import pytest


def _make_dep(parent_datatype: str, join_field: str):
    """Create a WriteDependency."""
    from inandout.config.writeback import WriteDependency
    return WriteDependency(parent_datatype=parent_datatype, join_field=join_field)


# ---------------------------------------------------------------------------
# detect_dependency_cycle
# ---------------------------------------------------------------------------

def test_no_cycle_when_no_dependencies():
    """No dependencies configured → no cycle."""
    from inandout.writeback.ordering import detect_dependency_cycle
    rows = [
        {"external_id": "1", "parent_id": ""},
        {"external_id": "2", "parent_id": "1"},
    ]
    assert detect_dependency_cycle(rows, []) is False


def test_no_cycle_in_simple_parent_child():
    """Simple parent → child structure has no cycle."""
    from inandout.writeback.ordering import detect_dependency_cycle
    dep = _make_dep("parent_dtype", "parent_id")
    rows = [
        {"external_id": "parent-1", "parent_id": ""},
        {"external_id": "child-1", "parent_id": "parent-1"},
    ]
    assert detect_dependency_cycle(rows, [dep]) is False


def test_cycle_detected_in_circular_reference():
    """A → B → A cycle should be detected."""
    from inandout.writeback.ordering import detect_dependency_cycle
    dep = _make_dep("dtype", "ref_id")
    rows = [
        {"external_id": "A", "ref_id": "B"},
        {"external_id": "B", "ref_id": "A"},
    ]
    assert detect_dependency_cycle(rows, [dep]) is True


def test_no_cycle_when_rows_empty():
    """Empty row list → no cycle."""
    from inandout.writeback.ordering import detect_dependency_cycle
    dep = _make_dep("dtype", "parent_id")
    assert detect_dependency_cycle([], [dep]) is False


# ---------------------------------------------------------------------------
# topological_sort_rows
# ---------------------------------------------------------------------------

def test_order_preserved_when_no_dependencies():
    """No dependencies → order is preserved as-is."""
    from inandout.writeback.ordering import topological_sort_rows
    rows = [
        {"external_id": "3"},
        {"external_id": "1"},
        {"external_id": "2"},
    ]
    result = topological_sort_rows(rows, [])
    assert [r["external_id"] for r in result] == ["3", "1", "2"]


def test_parent_comes_before_child_in_group():
    """With dependencies and _group_id, parent should come before child."""
    from inandout.writeback.ordering import topological_sort_rows
    dep = _make_dep("company", "company_id")
    rows = [
        {"external_id": "contact-1", "company_id": "company-1", "_group_id": "g1"},
        {"external_id": "company-1", "company_id": "", "_group_id": "g1"},
    ]
    result = topological_sort_rows(rows, [dep])
    # company-1 (no deps) should come before contact-1 (depends on company-1)
    ids = [r["external_id"] for r in result if not r.get("_cycle_error")]
    assert ids.index("company-1") < ids.index("contact-1")


def test_cycle_marks_all_group_rows_with_cycle_error():
    """When cycle detected, all rows in group get _cycle_error=True."""
    from inandout.writeback.ordering import topological_sort_rows
    dep = _make_dep("dtype", "ref_id")
    rows = [
        {"external_id": "A", "ref_id": "B", "_group_id": "g1"},
        {"external_id": "B", "ref_id": "A", "_group_id": "g1"},
    ]
    result = topological_sort_rows(rows, [dep])
    assert all(r.get("_cycle_error") is True for r in result)


def test_mixed_groups_sorted_independently():
    """Multiple groups are each sorted independently."""
    from inandout.writeback.ordering import topological_sort_rows
    dep = _make_dep("parent", "parent_id")
    rows = [
        {"external_id": "child-g1", "parent_id": "parent-g1", "_group_id": "g1"},
        {"external_id": "parent-g1", "parent_id": "", "_group_id": "g1"},
        {"external_id": "child-g2", "parent_id": "parent-g2", "_group_id": "g2"},
        {"external_id": "parent-g2", "parent_id": "", "_group_id": "g2"},
    ]
    result = topological_sort_rows(rows, [dep])
    ids = [r["external_id"] for r in result if not r.get("_cycle_error")]
    # Within g1: parent before child
    assert ids.index("parent-g1") < ids.index("child-g1")
    # Within g2: parent before child
    assert ids.index("parent-g2") < ids.index("child-g2")


def test_ungrouped_rows_appended_at_end():
    """Rows without _group_id are appended after grouped rows."""
    from inandout.writeback.ordering import topological_sort_rows
    dep = _make_dep("parent", "parent_id")
    rows = [
        {"external_id": "ungrouped"},
        {"external_id": "child", "parent_id": "parent", "_group_id": "g1"},
        {"external_id": "parent", "parent_id": "", "_group_id": "g1"},
    ]
    result = topological_sort_rows(rows, [dep])
    ids = [r["external_id"] for r in result]
    # grouped rows should come before ungrouped
    group_indices = [i for i, r in enumerate(result) if r.get("_group_id") == "g1"]
    ungrouped_indices = [i for i, r in enumerate(result) if r.get("_group_id") is None]
    assert max(group_indices) < min(ungrouped_indices)


# ---------------------------------------------------------------------------
# WriteDependency config model
# ---------------------------------------------------------------------------

def test_write_dependency_model():
    """WriteDependency model has parent_datatype and join_field."""
    from inandout.config.writeback import WriteDependency
    dep = WriteDependency(parent_datatype="company", join_field="company_id")
    assert dep.parent_datatype == "company"
    assert dep.join_field == "company_id"


def test_writeback_config_has_write_dependencies():
    """WritebackConfig has write_dependencies field with default empty list."""
    from inandout.config.writeback import WritebackConfig, OperationsConfig, OperationConfig
    from inandout.config.writeback import UpdateOperationConfig, ConditionalWrite, ConflictResolution, ProtectionLevel

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.conditional_write_required,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/items/${external_id}"),
            update=UpdateOperationConfig(
                method="PATCH",
                path="/items/${external_id}",
                conditional_write=ConditionalWrite(enabled=True),
            ),
        ),
    )
    assert cfg.write_dependencies == []
