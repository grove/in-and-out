"""Unit tests for WritebackResult dataclass in writeback/engine.py."""
from __future__ import annotations

import uuid
from dataclasses import is_dataclass

import pytest

from inandout.writeback.engine import WritebackResult


def test_is_dataclass():
    assert is_dataclass(WritebackResult)


def test_required_fields_stored():
    r = WritebackResult(connector="crm", datatype="contacts", delta_table="dt")
    assert r.connector == "crm"
    assert r.datatype == "contacts"
    assert r.delta_table == "dt"


def test_processed_default_zero():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r.processed == 0


def test_skipped_default_zero():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r.skipped == 0


def test_failed_default_zero():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r.failed == 0


def test_conflicts_default_zero():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r.conflicts == 0


def test_error_message_default_none():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r.error_message is None


def test_dry_run_log_default_empty():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r.dry_run_log == []


def test_dry_run_log_instances_independent():
    r1 = WritebackResult(connector="a", datatype="b", delta_table="c")
    r2 = WritebackResult(connector="d", datatype="e", delta_table="f")
    r1.dry_run_log.append({"op": "insert"})
    assert r2.dry_run_log == []


def test_run_id_is_valid_uuid():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    parsed = uuid.UUID(r.run_id)
    assert str(parsed) == r.run_id


def test_run_id_unique_per_instance():
    r1 = WritebackResult(connector="x", datatype="y", delta_table="z")
    r2 = WritebackResult(connector="x", datatype="y", delta_table="z")
    assert r1.run_id != r2.run_id


def test_missing_required_field_raises():
    with pytest.raises(TypeError):
        WritebackResult()


def test_counter_assignment():
    r = WritebackResult(connector="x", datatype="y", delta_table="z")
    r.processed = 10
    r.skipped = 2
    r.failed = 1
    r.conflicts = 1
    assert r.processed == 10
    assert r.skipped == 2
    assert r.failed == 1
    assert r.conflicts == 1
