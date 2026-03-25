"""Unit tests for glob-style field exclusion (T1 #21)."""
from __future__ import annotations

import pytest

from inandout.ingestion.field_exclusion import apply_field_exclusions


def test_field_exclusion_empty_patterns():
    """Empty exclusion patterns should return record unchanged."""
    record = {"id": "1", "name": "Alice", "internal_flag": True}
    result = apply_field_exclusions(record, [])
    assert result == record


def test_field_exclusion_underscore_prefix():
    """Pattern _* should exclude all fields starting with underscore."""
    record = {
        "id": "1",
        "name": "Alice",
        "_internal_flag": True,
        "_temp_data": "xyz",
        "status": "active",
    }
    result = apply_field_exclusions(record, ["_*"])
    assert result == {"id": "1", "name": "Alice", "status": "active"}


def test_field_exclusion_suffix_pattern():
    """Pattern *_temp should exclude all fields ending with _temp."""
    record = {
        "id": "1",
        "data_temp": "abc",
        "cache_temp": "xyz",
        "permanent_data": "keep",
    }
    result = apply_field_exclusions(record, ["*_temp"])
    assert result == {"id": "1", "permanent_data": "keep"}


def test_field_exclusion_wildcard_pattern():
    """Pattern *.internal_* should exclude nested-style internal fields."""
    record = {
        "user.id": "1",
        "user.internal_flag": True,
        "user.name": "Alice",
        "order.internal_id": "999",
        "order.amount": 100,
    }
    result = apply_field_exclusions(record, ["*.internal_*"])
    assert "user.internal_flag" not in result
    assert "order.internal_id" not in result
    assert result["user.id"] == "1"
    assert result["user.name"] == "Alice"
    assert result["order.amount"] == 100


def test_field_exclusion_multiple_patterns():
    """Multiple exclusion patterns should all be applied."""
    record = {
        "id": "1",
        "_temp": "a",
        "internal_flag": True,
        "name": "Alice",
        "data_cache": "b",
    }
    result = apply_field_exclusions(record, ["_*", "internal_*", "*_cache"])
    assert result == {"id": "1", "name": "Alice"}


def test_field_exclusion_nested_dicts():
    """Exclusions should be applied recursively to nested dicts."""
    record = {
        "id": "1",
        "user": {
            "name": "Alice",
            "_internal_id": "999",
            "_temp": "xyz",
        },
        "_metadata": {"version": "1.0"},
    }
    result = apply_field_exclusions(record, ["_*"])
    assert result == {
        "id": "1",
        "user": {"name": "Alice"},
    }


def test_field_exclusion_config_in_list_config():
    """ListConfig should have properties_exclude field."""
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/records",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
        properties_exclude=["_*", "*.internal_*"],
    )
    assert cfg.properties_exclude == ["_*", "*.internal_*"]


def test_field_exclusion_defaults_to_empty():
    """properties_exclude should default to empty list."""
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/records",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
    )
    assert cfg.properties_exclude == []
