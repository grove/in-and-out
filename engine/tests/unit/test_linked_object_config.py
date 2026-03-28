"""Unit tests for LinkedObject config."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import LinkedObject


def test_minimal_valid():
    obj = LinkedObject(
        field="line_item_ids",
        datatype="line_items",
        detail_path="/line-items/${id}",
    )
    assert obj.field == "line_item_ids"
    assert obj.datatype == "line_items"
    assert obj.detail_path == "/line-items/${id}"


def test_concurrency_default_three():
    obj = LinkedObject(field="ids", datatype="items", detail_path="/items/${id}")
    assert obj.concurrency == 3


def test_custom_concurrency():
    obj = LinkedObject(
        field="ids",
        datatype="items",
        detail_path="/items/${id}",
        concurrency=5,
    )
    assert obj.concurrency == 5


def test_missing_field_raises():
    with pytest.raises(ValidationError):
        LinkedObject(datatype="items", detail_path="/items/${id}")


def test_missing_datatype_raises():
    with pytest.raises(ValidationError):
        LinkedObject(field="ids", detail_path="/items/${id}")


def test_missing_detail_path_raises():
    with pytest.raises(ValidationError):
        LinkedObject(field="ids", datatype="items")


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        LinkedObject(
            field="ids",
            datatype="items",
            detail_path="/items/${id}",
            unknown="bad",
        )


def test_round_trip_json():
    obj = LinkedObject(
        field="contact_ids",
        datatype="contacts",
        detail_path="/contacts/${id}",
        concurrency=2,
    )
    loaded = LinkedObject.model_validate_json(obj.model_dump_json())
    assert loaded.field == "contact_ids"
    assert loaded.concurrency == 2


def test_detail_path_with_interpolation():
    obj = LinkedObject(
        field="ids",
        datatype="items",
        detail_path="/api/v2/items/${id}?include=details",
    )
    assert "${id}" in obj.detail_path
