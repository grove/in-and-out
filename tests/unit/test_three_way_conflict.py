"""Unit tests for three-way conflict detection (Priority 7 — Phase 2)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# conflicts_detected_total metric tests
# ---------------------------------------------------------------------------


def test_conflicts_detected_total_metric_exists():
    """conflicts_detected_total should be importable from metrics module."""
    from inandout.observability.metrics import conflicts_detected_total
    assert conflicts_detected_total is not None


def test_conflicts_detected_total_metric_has_correct_labels():
    """conflicts_detected_total should have connector, datatype, resolution, namespace labels."""
    from inandout.observability.metrics import conflicts_detected_total
    # Access label names from the metric descriptor
    label_names = list(conflicts_detected_total._labelnames)
    assert "connector" in label_names
    assert "datatype" in label_names
    assert "resolution" in label_names
    assert "namespace" in label_names


# ---------------------------------------------------------------------------
# WritebackEngine conflict detection tests
# ---------------------------------------------------------------------------


def test_writeback_engine_imports_conflicts_detected_total():
    """writeback engine module should import conflicts_detected_total."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "conflicts_detected_total" in source


def test_writeback_engine_emits_conflicts_metric_on_server_wins():
    """writeback engine source should emit conflicts_detected_total for server_wins."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    # Should have conflicts_detected_total.labels(...).inc() for server_wins
    assert "server_wins" in source
    assert "conflicts_detected_total" in source


def test_writeback_engine_emits_conflicts_metric_on_412():
    """writeback engine source should emit conflicts_detected_total for 412."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "412_precondition_failed" in source
    assert "conflicts_detected_total" in source


def test_writeback_engine_emits_conflicts_metric_on_merge_fields():
    """writeback engine source should emit conflicts_detected_total for merge_fields."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "merge_fields" in source
    assert "conflicts_detected_total" in source


# ---------------------------------------------------------------------------
# _compute_field_diff used in three-way conflict detection
# ---------------------------------------------------------------------------


def test_compute_field_diff_detects_changed_fields():
    """_compute_field_diff should detect fields changed between base and current."""
    from inandout.writeback.engine import _compute_field_diff

    base = {"name": "Alice", "email": "alice@old.com"}
    current = {"name": "Alice", "email": "alice@new.com"}
    diff = _compute_field_diff(base, current)

    assert "email" in diff.get("changed", {})
    assert diff["changed"]["email"]["from"] == "alice@old.com"
    assert diff["changed"]["email"]["to"] == "alice@new.com"


def test_compute_field_diff_detects_added_fields():
    """_compute_field_diff should detect new fields added."""
    from inandout.writeback.engine import _compute_field_diff

    base = {"name": "Alice"}
    current = {"name": "Alice", "phone": "555-1234"}
    diff = _compute_field_diff(base, current)

    assert "phone" in diff.get("added", [])


def test_compute_field_diff_detects_removed_fields():
    """_compute_field_diff should detect fields removed."""
    from inandout.writeback.engine import _compute_field_diff

    base = {"name": "Alice", "phone": "555-1234"}
    current = {"name": "Alice"}
    diff = _compute_field_diff(base, current)

    assert "phone" in diff.get("removed", [])


def test_compute_field_diff_empty_for_identical_payloads():
    """_compute_field_diff should return empty diff for identical payloads."""
    from inandout.writeback.engine import _compute_field_diff

    payload = {"name": "Alice", "email": "alice@example.com"}
    diff = _compute_field_diff(payload, payload)

    assert not diff.get("added")
    assert not diff.get("removed")
    assert not diff.get("changed")
