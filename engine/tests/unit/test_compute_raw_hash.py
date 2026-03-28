"""Unit tests for _compute_raw_hash in engine.py.

Covers:
- Determinism: same dict → same hash.
- Key-order independence: dicts with same keys in different insertion order → same hash.
- Sensitivity: a single changed value → different hash.
- Return type is str (hex digest).
- Empty dict produces a stable non-empty hash.
- Nested dicts are hashed consistently.
"""
from __future__ import annotations

import pytest

from inandout.ingestion.engine import _compute_raw_hash


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_same_dict_produces_same_hash():
    d = {"id": "123", "name": "Alice", "score": 99}
    assert _compute_raw_hash(d) == _compute_raw_hash(d)


def test_returns_string():
    assert isinstance(_compute_raw_hash({"a": 1}), str)


def test_returns_non_empty_hex():
    h = _compute_raw_hash({"a": 1})
    assert len(h) == 64  # SHA-256 produces 64 hex chars
    assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# Key-order independence (OPT_SORT_KEYS)
# ---------------------------------------------------------------------------

def test_key_order_does_not_affect_hash():
    d1 = {"b": 2, "a": 1}
    d2 = {"a": 1, "b": 2}
    assert _compute_raw_hash(d1) == _compute_raw_hash(d2)


def test_nested_key_order_does_not_affect_hash():
    d1 = {"outer": {"z": 26, "a": 1}}
    d2 = {"outer": {"a": 1, "z": 26}}
    assert _compute_raw_hash(d1) == _compute_raw_hash(d2)


# ---------------------------------------------------------------------------
# Sensitivity
# ---------------------------------------------------------------------------

def test_changed_value_produces_different_hash():
    d1 = {"id": "1", "name": "Alice"}
    d2 = {"id": "1", "name": "Bob"}
    assert _compute_raw_hash(d1) != _compute_raw_hash(d2)


def test_changed_key_produces_different_hash():
    d1 = {"id": "1", "name": "Alice"}
    d2 = {"id": "1", "email": "Alice"}
    assert _compute_raw_hash(d1) != _compute_raw_hash(d2)


def test_extra_field_produces_different_hash():
    d1 = {"id": "1"}
    d2 = {"id": "1", "extra": True}
    assert _compute_raw_hash(d1) != _compute_raw_hash(d2)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_dict_produces_stable_hash():
    h1 = _compute_raw_hash({})
    h2 = _compute_raw_hash({})
    assert h1 == h2
    assert len(h1) == 64


def test_none_value_is_hashed():
    d1 = {"a": None}
    d2 = {"a": "None"}
    assert _compute_raw_hash(d1) != _compute_raw_hash(d2)


def test_integer_vs_string_value_differs():
    d1 = {"id": 1}
    d2 = {"id": "1"}
    assert _compute_raw_hash(d1) != _compute_raw_hash(d2)
