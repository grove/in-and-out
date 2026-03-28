"""Unit tests for Step 51 — Data quality rules."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# validate_record tests
# ---------------------------------------------------------------------------

def test_required_field_missing():
    """Required field missing → violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(required=["email"])
    violations = validate_record({"name": "Alice"}, rules, {})
    assert len(violations) == 1
    assert violations[0].field == "email"
    assert violations[0].rule == "required"


def test_required_field_empty_string():
    """Required field set to empty string → violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(required=["email"])
    violations = validate_record({"email": ""}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "required"


def test_required_field_present():
    """Required field present → no violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(required=["email"])
    violations = validate_record({"email": "alice@example.com"}, rules, {})
    assert violations == []


def test_regex_mismatch():
    """Regex mismatch → violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(regex={"email": r"[^@]+@[^@]+\.[^@]+"})
    violations = validate_record({"email": "not-an-email"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "regex"


def test_regex_match():
    """Regex match → no violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(regex={"email": r"[^@]+@[^@]+\.[^@]+"})
    violations = validate_record({"email": "alice@example.com"}, rules, {})
    assert violations == []


def test_unique_within_batch_second_occurrence():
    """Same value twice in batch → second triggers violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(unique_within_batch=["email"])
    seen: dict = {}

    v1 = validate_record({"email": "alice@example.com"}, rules, seen)
    assert v1 == []

    v2 = validate_record({"email": "alice@example.com"}, rules, seen)
    assert len(v2) == 1
    assert v2[0].rule == "unique_within_batch"


def test_unique_within_batch_different_values():
    """Different values in batch → no violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(unique_within_batch=["email"])
    seen: dict = {}
    v1 = validate_record({"email": "alice@example.com"}, rules, seen)
    v2 = validate_record({"email": "bob@example.com"}, rules, seen)
    assert v1 == []
    assert v2 == []


def test_allowed_values_unknown():
    """Value not in allowed list → violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(allowed_values={"status": ["active", "inactive"]})
    violations = validate_record({"status": "pending"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "allowed_values"


def test_allowed_values_valid():
    """Value in allowed list → no violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(allowed_values={"status": ["active", "inactive"]})
    violations = validate_record({"status": "active"}, rules, {})
    assert violations == []


def test_all_rules_pass_empty_violations():
    """Record passing all rules → empty violations list."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(
        required=["id", "email"],
        regex={"email": r"[^@]+@[^@]+\.[^@]+"},
        min_length={"email": 5},
        max_length={"email": 100},
        allowed_values={"status": ["active"]},
    )
    record = {"id": "123", "email": "alice@example.com", "status": "active"}
    violations = validate_record(record, rules, {})
    assert violations == []


def test_min_length_violation():
    """String shorter than min_length → violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(min_length={"name": 3})
    violations = validate_record({"name": "Al"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "min_length"


def test_min_length_pass():
    """String meeting min_length → no violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(min_length={"name": 3})
    violations = validate_record({"name": "Alice"}, rules, {})
    assert violations == []


def test_max_length_violation():
    """String longer than max_length → violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(max_length={"code": 3})
    violations = validate_record({"code": "ABCDEF"}, rules, {})
    assert len(violations) == 1
    assert violations[0].rule == "max_length"


def test_max_length_pass():
    """String within max_length → no violation."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(max_length={"code": 10})
    violations = validate_record({"code": "ABC"}, rules, {})
    assert violations == []


def test_multiple_violations_returned():
    """Multiple rules fail → multiple violations returned."""
    from inandout.config.quality import QualityRule
    from inandout.ingestion.quality import validate_record

    rules = QualityRule(
        required=["id"],
        regex={"email": r"[^@]+@[^@]+\.[^@]+"},
    )
    record = {"email": "not-an-email"}  # id missing, email bad
    violations = validate_record(record, rules, {})
    assert len(violations) == 2
    rules_hit = {v.rule for v in violations}
    assert "required" in rules_hit
    assert "regex" in rules_hit
