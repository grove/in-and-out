"""Unit tests for _compute_row_hash in writeback/engine.py."""
from __future__ import annotations

import hashlib

import orjson
import pytest

from inandout.writeback.engine import _compute_row_hash


def _expected_hash(payload: dict) -> str:
    return hashlib.sha256(
        orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


def test_returns_hex_string():
    result = _compute_row_hash({"id": "1", "name": "Alice"})
    assert isinstance(result, str)
    assert len(result) == 64  # SHA-256 hex digest


def test_strips_underscore_prefixed_fields():
    row_with_internal = {"id": "1", "_source": "db", "_hash": "abc", "name": "Bob"}
    row_clean = {"id": "1", "name": "Bob"}
    assert _compute_row_hash(row_with_internal) == _compute_row_hash(row_clean)


def test_underscore_only_fields_yield_empty_payload_hash():
    row = {"_internal": "x", "_meta": 99}
    expected = _expected_hash({})
    assert _compute_row_hash(row) == expected


def test_key_order_independent():
    row_a = {"z": 3, "a": 1, "m": 2}
    row_b = {"a": 1, "m": 2, "z": 3}
    assert _compute_row_hash(row_a) == _compute_row_hash(row_b)


def test_different_values_produce_different_hashes():
    row_a = {"id": "1"}
    row_b = {"id": "2"}
    assert _compute_row_hash(row_a) != _compute_row_hash(row_b)


def test_deterministic_across_calls():
    row = {"id": "abc", "email": "x@y.com"}
    assert _compute_row_hash(row) == _compute_row_hash(row)


def test_empty_row_returns_stable_hash():
    h1 = _compute_row_hash({})
    h2 = _compute_row_hash({})
    assert h1 == h2


def test_nested_values_hashed():
    row = {"meta": {"source": "api", "version": 1}}
    expected = _expected_hash({"meta": {"source": "api", "version": 1}})
    assert _compute_row_hash(row) == expected


def test_mixed_underscore_and_normal_fields():
    row = {"_ts": 123, "id": "x", "_raw": "y", "value": 42}
    expected = _expected_hash({"id": "x", "value": 42})
    assert _compute_row_hash(row) == expected


def test_none_value_included():
    row = {"id": None}
    h = _compute_row_hash(row)
    assert isinstance(h, str)
    assert len(h) == 64
