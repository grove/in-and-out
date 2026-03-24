"""Unit tests for redact_pii in ingestion/privacy.py."""
from __future__ import annotations

import pytest

from inandout.ingestion.privacy import redact_pii


def test_no_pii_fields_returns_record_unchanged():
    record = {"id": "1", "name": "Alice"}
    result = redact_pii(record, [])
    assert result == record


def test_redacts_single_field():
    record = {"id": "1", "email": "alice@example.com"}
    result = redact_pii(record, ["email"])
    assert result["email"] == "[REDACTED]"
    assert result["id"] == "1"


def test_redacts_multiple_fields():
    record = {"id": "1", "email": "alice@example.com", "phone": "555-1234", "name": "Alice"}
    result = redact_pii(record, ["email", "phone"])
    assert result["email"] == "[REDACTED]"
    assert result["phone"] == "[REDACTED]"
    assert result["name"] == "Alice"


def test_pii_field_not_in_record_is_ignored():
    record = {"id": "1"}
    result = redact_pii(record, ["email"])
    assert "email" not in result
    assert result == record


def test_returns_new_dict_not_mutating_original():
    original = {"id": "1", "ssn": "123-45-6789"}
    original_copy = dict(original)
    result = redact_pii(original, ["ssn"])
    assert original == original_copy  # original unchanged
    assert result is not original


def test_non_pii_fields_preserved():
    record = {"a": 1, "b": 2, "c": 3}
    result = redact_pii(record, ["a"])
    assert result["b"] == 2
    assert result["c"] == 3


def test_all_fields_redacted():
    record = {"x": "val1", "y": "val2"}
    result = redact_pii(record, ["x", "y"])
    assert result["x"] == "[REDACTED]"
    assert result["y"] == "[REDACTED]"


def test_empty_record_with_pii_fields_returns_empty():
    result = redact_pii({}, ["email"])
    assert result == {}


def test_empty_record_no_pii_fields():
    result = redact_pii({}, [])
    assert result == {}


def test_redacted_value_is_string():
    record = {"secret": 42}
    result = redact_pii(record, ["secret"])
    assert isinstance(result["secret"], str)
    assert result["secret"] == "[REDACTED]"
