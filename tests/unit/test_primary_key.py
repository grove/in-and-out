"""Unit tests for primary key extraction (A2 — T1 #27)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inandout.ingestion.primary_key import extract_primary_key, validate_primary_key_expression


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(primary_key: str = "id", primary_key_expression: str | None = None) -> MagicMock:
    cfg = MagicMock()
    cfg.primary_key = primary_key
    cfg.primary_key_expression = primary_key_expression
    return cfg


# ---------------------------------------------------------------------------
# Tests: extract_primary_key
# ---------------------------------------------------------------------------


def test_single_field_primary_key():
    """Single field ``primary_key='id'`` extracts ``record['id']``."""
    record = {"id": "abc-123", "name": "Alice"}
    cfg = _make_cfg(primary_key="id")
    assert extract_primary_key(record, cfg) == "abc-123"


def test_single_field_missing_returns_empty_string():
    """Missing field with single primary_key returns empty string (existing behaviour)."""
    record = {"name": "Alice"}
    cfg = _make_cfg(primary_key="id")
    assert extract_primary_key(record, cfg) == ""


def test_expression_composite_key():
    """Expression ``{account_id}:{contact_id}`` produces composite key."""
    record = {"account_id": "acc-1", "contact_id": "con-2", "name": "Alice"}
    cfg = _make_cfg(primary_key="id", primary_key_expression="{account_id}:{contact_id}")
    assert extract_primary_key(record, cfg) == "acc-1:con-2"


def test_expression_missing_field_raises_keyerror_with_helpful_message():
    """Expression referencing missing field → KeyError with field name in message."""
    record = {"account_id": "acc-1"}
    cfg = _make_cfg(primary_key="id", primary_key_expression="{account_id}:{contact_id}")
    with pytest.raises(KeyError, match="contact_id"):
        extract_primary_key(record, cfg)


def test_expression_takes_precedence_over_primary_key():
    """``primary_key_expression`` takes precedence over ``primary_key``."""
    record = {"id": "should-not-use", "account_id": "acc-x", "contact_id": "con-y"}
    cfg = _make_cfg(primary_key="id", primary_key_expression="{account_id}:{contact_id}")
    result = extract_primary_key(record, cfg)
    assert result == "acc-x:con-y"
    assert result != "should-not-use"


def test_expression_single_field():
    """Expression wrapping a single field."""
    record = {"user_id": "u-99"}
    cfg = _make_cfg(primary_key="id", primary_key_expression="{user_id}")
    assert extract_primary_key(record, cfg) == "u-99"


# ---------------------------------------------------------------------------
# Tests: validate_primary_key_expression
# ---------------------------------------------------------------------------


def test_validate_expression_valid():
    """validate_primary_key_expression returns True when expression can be evaluated."""
    assert validate_primary_key_expression("{account_id}:{contact_id}", {"account_id": "a", "contact_id": "b"}) is True


def test_validate_expression_missing_field():
    """validate_primary_key_expression returns False when a field is missing."""
    assert validate_primary_key_expression("{account_id}:{contact_id}", {"account_id": "a"}) is False


def test_validate_expression_empty_record():
    """validate_primary_key_expression returns False against empty record when fields referenced."""
    assert validate_primary_key_expression("{id}", {}) is False


def test_validate_expression_no_placeholders():
    """Expression with no placeholders is always valid."""
    assert validate_primary_key_expression("static-key", {}) is True
