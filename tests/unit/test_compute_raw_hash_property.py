"""Hypothesis property test for _compute_raw_hash sensitivity.

For any two dicts that differ by at least one key or value, their hashes
must differ (collision-free over the strategy space).
"""
from __future__ import annotations

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from inandout.ingestion.engine import _compute_raw_hash

# Strategy for JSON-serialisable leaf values
_leaves = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=32),
)

# Flat dict strategy
_flat_dict = st.dictionaries(st.text(min_size=1, max_size=16), _leaves, max_size=8)


@given(d1=_flat_dict, d2=_flat_dict)
@settings(max_examples=500)
def test_different_dicts_produce_different_hashes(d1: dict, d2: dict):
    """Two dicts that are not equal must produce different hashes."""
    assume(d1 != d2)
    assert _compute_raw_hash(d1) != _compute_raw_hash(d2), (
        f"Hash collision: {d1!r} and {d2!r} both hash to {_compute_raw_hash(d1)!r}"
    )


@given(d=_flat_dict)
@settings(max_examples=200)
def test_hash_is_deterministic_for_any_dict(d: dict):
    """Any dict must hash to the same value on repeated calls."""
    assert _compute_raw_hash(d) == _compute_raw_hash(d)
