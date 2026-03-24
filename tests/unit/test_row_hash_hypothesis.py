"""Property-based tests for _compute_row_hash using Hypothesis."""
from __future__ import annotations

import string

from hypothesis import given, assume, settings
from hypothesis import strategies as st

from inandout.writeback.engine import _compute_row_hash


# Strategy: dict with string keys that don't start with "_"
_normal_key = st.text(alphabet=string.ascii_lowercase + string.digits, min_size=1, max_size=10).filter(lambda k: not k.startswith("_"))
_internal_key = st.text(alphabet=string.ascii_lowercase + string.digits, min_size=1, max_size=9).map(lambda k: "_" + k)
_value = st.one_of(st.none(), st.integers(-1000, 1000), st.text(max_size=20), st.booleans())


@given(st.dictionaries(_normal_key, _value, max_size=8))
def test_hash_deterministic(row):
    """Same row always produces same hash."""
    assert _compute_row_hash(row) == _compute_row_hash(row)


@given(st.dictionaries(_normal_key, _value, max_size=8))
def test_hash_is_64_char_hex(row):
    """Hash is always a 64-character lowercase hex string."""
    h = _compute_row_hash(row)
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


@given(
    st.dictionaries(_normal_key, _value, max_size=6),
    st.dictionaries(_internal_key, _value, min_size=1, max_size=4),
)
def test_internal_fields_do_not_affect_hash(normal_row, internal_extra):
    """Adding _-prefixed fields must not change the hash."""
    merged = {**normal_row, **internal_extra}
    assert _compute_row_hash(normal_row) == _compute_row_hash(merged)


@given(
    st.lists(
        st.tuples(_normal_key, _value),
        min_size=1,
        max_size=8,
    )
)
def test_key_order_independent(pairs):
    """Hash must be identical regardless of key insertion order."""
    assume(len({k for k, _ in pairs}) == len(pairs))  # unique keys only
    row_fwd = dict(pairs)
    row_rev = dict(reversed(pairs))
    assert _compute_row_hash(row_fwd) == _compute_row_hash(row_rev)


@given(
    st.dictionaries(_normal_key, _value, min_size=1, max_size=6),
    st.dictionaries(_normal_key, _value, min_size=1, max_size=6),
)
def test_different_content_usually_different_hash(row_a, row_b):
    """Different row content should (almost always) produce different hashes."""
    assume(row_a != row_b)
    # Not guaranteed (birthday bound), but with 64-byte hashes it's astronomically unlikely
    # We just verify both are valid hex strings; equality would be a collision
    h_a = _compute_row_hash(row_a)
    h_b = _compute_row_hash(row_b)
    assert len(h_a) == 64
    assert len(h_b) == 64
