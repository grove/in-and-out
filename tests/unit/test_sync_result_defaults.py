"""Unit tests for SyncResult default field values.

Guards against accidental mutation of defaults (e.g. sharing mutable state).
"""
from __future__ import annotations

import uuid

import pytest

from inandout.ingestion.engine import SyncResult


def _make() -> SyncResult:
    return SyncResult(uuid.uuid4(), "hubspot", "contacts", "full")


def test_records_fetched_default_zero():
    assert _make().records_fetched == 0


def test_records_inserted_default_zero():
    assert _make().records_inserted == 0


def test_records_updated_default_zero():
    assert _make().records_updated == 0


def test_records_errored_default_zero():
    assert _make().records_errored == 0


def test_records_deleted_default_zero():
    assert _make().records_deleted == 0


def test_error_message_default_none():
    assert _make().error_message is None


def test_status_default_running():
    assert _make().status == "running"


def test_connector_and_datatype_set_from_args():
    r = _make()
    assert r.connector == "hubspot"
    assert r.datatype == "contacts"


def test_mode_set_from_args():
    r = SyncResult(uuid.uuid4(), "x", "y", "incremental")
    assert r.mode == "incremental"


def test_run_id_set_from_args():
    rid = uuid.uuid4()
    r = SyncResult(rid, "x", "y", "full")
    assert r.run_id == rid


def test_instances_do_not_share_mutable_state():
    """Two SyncResult instances must be completely independent."""
    r1 = _make()
    r2 = _make()
    r1.records_inserted = 99
    r1.error_message = "boom"
    r1.status = "failed"
    assert r2.records_inserted == 0
    assert r2.error_message is None
    assert r2.status == "running"
