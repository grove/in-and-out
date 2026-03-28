"""Unit tests for PurgeResult dataclass in ingestion/privacy.py."""
from __future__ import annotations

from dataclasses import is_dataclass

import pytest

from inandout.ingestion.privacy import PurgeResult


def test_is_dataclass():
    assert is_dataclass(PurgeResult)


def test_fields_stored():
    r = PurgeResult(connector="crm", datatype="contacts", external_id="ext-001")
    assert r.connector == "crm"
    assert r.datatype == "contacts"
    assert r.external_id == "ext-001"


def test_tables_purged_default_is_empty_dict():
    r = PurgeResult(connector="crm", datatype="contacts", external_id="ext-001")
    assert r.tables_purged == {}


def test_tables_purged_instances_are_independent():
    r1 = PurgeResult(connector="a", datatype="b", external_id="1")
    r2 = PurgeResult(connector="c", datatype="d", external_id="2")
    r1.tables_purged["source"] = 1
    assert r2.tables_purged == {}


def test_tables_purged_assignable():
    r = PurgeResult(connector="a", datatype="b", external_id="c")
    r.tables_purged["source"] = 3
    r.tables_purged["history"] = 5
    assert r.tables_purged["source"] == 3
    assert r.tables_purged["history"] == 5


def test_tables_purged_accepts_dict_arg():
    r = PurgeResult(
        connector="a",
        datatype="b",
        external_id="c",
        tables_purged={"source": 1, "history": 2},
    )
    assert r.tables_purged["source"] == 1
    assert r.tables_purged["history"] == 2


def test_all_required_fields():
    with pytest.raises(TypeError):
        PurgeResult()  # missing required fields


def test_str_representation_contains_connector():
    r = PurgeResult(connector="myconn", datatype="d", external_id="e")
    assert "myconn" in repr(r)
