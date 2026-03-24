"""Unit tests for QualityRule Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.quality import QualityRule


def test_empty_rule_valid():
    rule = QualityRule()
    assert rule.required == []
    assert rule.unique_within_batch == []
    assert rule.regex == {}
    assert rule.min_length == {}
    assert rule.max_length == {}
    assert rule.allowed_values == {}


def test_required_list():
    rule = QualityRule(required=["id", "email"])
    assert "id" in rule.required
    assert "email" in rule.required


def test_unique_within_batch():
    rule = QualityRule(unique_within_batch=["external_id"])
    assert "external_id" in rule.unique_within_batch


def test_regex_dict():
    rule = QualityRule(regex={"email": r".*@.*"})
    assert rule.regex["email"] == r".*@.*"


def test_min_length_dict():
    rule = QualityRule(min_length={"name": 2})
    assert rule.min_length["name"] == 2


def test_max_length_dict():
    rule = QualityRule(max_length={"bio": 500})
    assert rule.max_length["bio"] == 500


def test_allowed_values_dict():
    rule = QualityRule(allowed_values={"status": ["active", "inactive"]})
    assert "active" in rule.allowed_values["status"]


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        QualityRule(unknown_field="bad")


def test_required_must_be_list():
    with pytest.raises(ValidationError):
        QualityRule(required="not_a_list")


def test_regex_must_be_dict():
    with pytest.raises(ValidationError):
        QualityRule(regex=["not", "a", "dict"])


def test_multiple_fields_combined():
    rule = QualityRule(
        required=["id"],
        unique_within_batch=["id"],
        regex={"email": r".*@.*"},
        min_length={"name": 1},
        max_length={"bio": 1000},
        allowed_values={"role": ["admin", "user"]},
    )
    assert rule.required == ["id"]
    assert rule.unique_within_batch == ["id"]
    assert "email" in rule.regex
    assert rule.min_length["name"] == 1
    assert rule.max_length["bio"] == 1000
    assert rule.allowed_values["role"] == ["admin", "user"]


def test_round_trip_json():
    rule = QualityRule(required=["id"], regex={"x": r"\d+"})
    loaded = QualityRule.model_validate_json(rule.model_dump_json())
    assert loaded.required == ["id"]
    assert loaded.regex["x"] == r"\d+"


def test_allowed_values_mixed_types():
    rule = QualityRule(allowed_values={"count": [1, 2, 3]})
    assert rule.allowed_values["count"] == [1, 2, 3]
