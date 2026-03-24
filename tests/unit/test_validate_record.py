"""Unit tests for validate_record quality rules.

Covers:
- required: missing/empty field → violation; present field → no violation.
- unique_within_batch: duplicate value → violation; first occurrence → no violation.
- allowed_values: value not in list → violation; value in list → no violation.
- regex: non-matching value → violation; matching value → no violation.
- min_length / max_length: length violations detected.
- Empty rules → no violations.
- Multiple violations returned for one record.
"""
from __future__ import annotations

import pytest

from inandout.config.quality import QualityRule
from inandout.ingestion.quality import QualityViolation, validate_record


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rules(**kwargs) -> QualityRule:
    return QualityRule(**kwargs)


# ---------------------------------------------------------------------------
# required
# ---------------------------------------------------------------------------

def test_required_passes_when_field_present():
    record = {"name": "Alice", "email": "alice@example.com"}
    rules = _rules(required=["name", "email"])
    violations = validate_record(record, rules, {})
    assert violations == []


def test_required_violation_when_field_missing():
    record = {"name": "Alice"}
    rules = _rules(required=["name", "email"])
    violations = validate_record(record, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "required"
    assert violations[0].field == "email"


def test_required_violation_when_field_empty_string():
    record = {"name": ""}
    rules = _rules(required=["name"])
    violations = validate_record(record, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "required"


def test_required_violation_when_field_none():
    record = {"name": None}
    rules = _rules(required=["name"])
    violations = validate_record(record, rules, {})
    assert len(violations) == 1


# ---------------------------------------------------------------------------
# unique_within_batch
# ---------------------------------------------------------------------------

def test_unique_within_batch_passes_first_occurrence():
    rules = _rules(unique_within_batch=["id"])
    seen: dict = {}
    violations = validate_record({"id": "abc"}, rules, seen)
    assert violations == []
    assert "abc" in seen["id"]


def test_unique_within_batch_violation_on_duplicate():
    rules = _rules(unique_within_batch=["id"])
    seen: dict = {"id": {"abc"}}
    violations = validate_record({"id": "abc"}, rules, seen)
    assert len(violations) == 1
    assert violations[0].rule == "unique_within_batch"
    assert violations[0].field == "id"


def test_unique_within_batch_different_values_both_pass():
    rules = _rules(unique_within_batch=["id"])
    seen: dict = {}
    v1 = validate_record({"id": "x"}, rules, seen)
    v2 = validate_record({"id": "y"}, rules, seen)
    assert v1 == []
    assert v2 == []


# ---------------------------------------------------------------------------
# allowed_values
# ---------------------------------------------------------------------------

def test_allowed_values_passes_valid_value():
    rules = _rules(allowed_values={"status": ["active", "inactive"]})
    violations = validate_record({"status": "active"}, rules, {})
    assert violations == []


def test_allowed_values_violation_on_unknown_value():
    rules = _rules(allowed_values={"status": ["active", "inactive"]})
    violations = validate_record({"status": "pending"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "allowed_values"
    assert violations[0].field == "status"


def test_allowed_values_skips_none_field():
    """If the field is missing, allowed_values must not fire."""
    rules = _rules(allowed_values={"status": ["active"]})
    violations = validate_record({}, rules, {})
    assert violations == []


# ---------------------------------------------------------------------------
# regex
# ---------------------------------------------------------------------------

def test_regex_passes_matching_value():
    rules = _rules(regex={"email": r".+@.+\..+"})
    violations = validate_record({"email": "a@b.com"}, rules, {})
    assert violations == []


def test_regex_violation_on_non_matching_value():
    rules = _rules(regex={"email": r".+@.+\..+"})
    violations = validate_record({"email": "not-an-email"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "regex"


# ---------------------------------------------------------------------------
# min_length / max_length
# ---------------------------------------------------------------------------

def test_min_length_passes():
    rules = _rules(min_length={"name": 2})
    violations = validate_record({"name": "Al"}, rules, {})
    assert violations == []


def test_min_length_violation():
    rules = _rules(min_length={"name": 5})
    violations = validate_record({"name": "Al"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "min_length"


def test_max_length_passes():
    rules = _rules(max_length={"name": 100})
    violations = validate_record({"name": "Alice"}, rules, {})
    assert violations == []


def test_max_length_violation():
    rules = _rules(max_length={"name": 3})
    violations = validate_record({"name": "Alice"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "max_length"


# ---------------------------------------------------------------------------
# Empty rules → no violations
# ---------------------------------------------------------------------------

def test_empty_rules_no_violations():
    rules = _rules()
    violations = validate_record({"id": "123", "name": "Bob"}, rules, {})
    assert violations == []


# ---------------------------------------------------------------------------
# Multiple violations
# ---------------------------------------------------------------------------

def test_multiple_violations_returned():
    rules = _rules(
        required=["id", "email"],
        allowed_values={"status": ["active"]},
    )
    record = {"status": "unknown"}  # id missing, email missing, status invalid
    violations = validate_record(record, rules, {})
    assert len(violations) == 3
    rule_names = {v.rule for v in violations}
    assert "required" in rule_names
    assert "allowed_values" in rule_names
