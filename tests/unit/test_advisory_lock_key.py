"""Unit tests for _advisory_lock_key determinism and collision resistance.

Covers:
- Determinism: same inputs → same key.
- Distinctness: different connector/datatype pairs produce different keys.
- int64 range: result fits in a signed 64-bit integer.
- Hypothesis property test: random pairs all produce distinct keys.
"""
from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from inandout.ingestion.engine import _advisory_lock_key

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_same_inputs_produce_same_key():
    assert _advisory_lock_key("hubspot", "contacts") == _advisory_lock_key("hubspot", "contacts")


def test_key_is_int():
    assert isinstance(_advisory_lock_key("hubspot", "contacts"), int)


# ---------------------------------------------------------------------------
# int64 range
# ---------------------------------------------------------------------------

def test_key_within_signed_int64_range():
    key = _advisory_lock_key("hubspot", "contacts")
    assert _INT64_MIN <= key <= _INT64_MAX


@given(
    connector=st.text(min_size=1, max_size=64),
    datatype=st.text(min_size=1, max_size=64),
)
@settings(max_examples=200)
def test_key_always_within_int64_range(connector: str, datatype: str):
    key = _advisory_lock_key(connector, datatype)
    assert _INT64_MIN <= key <= _INT64_MAX


# ---------------------------------------------------------------------------
# Distinctness — known pairs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("a,b", [
    (("hubspot", "contacts"), ("hubspot", "deals")),
    (("hubspot", "contacts"), ("salesforce", "contacts")),
    (("a", "b"), ("b", "a")),
    (("connector", "type"), ("connectortype", "")),  # boundary: concatenation ambiguity
])
def test_distinct_pairs_produce_distinct_keys(a, b):
    key_a = _advisory_lock_key(*a)
    key_b = _advisory_lock_key(*b)
    assert key_a != key_b, (
        f"Collision between {a!r} → {key_a} and {b!r} → {key_b}"
    )


# ---------------------------------------------------------------------------
# Hypothesis: large random set has no collisions
# ---------------------------------------------------------------------------

@given(
    pairs=st.lists(
        st.tuples(
            st.text(min_size=1, max_size=32),
            st.text(min_size=1, max_size=32),
        ),
        min_size=5,
        max_size=20,
        unique=True,
    )
)
@settings(max_examples=100)
def test_no_collisions_among_random_pairs(pairs):
    keys = [_advisory_lock_key(c, d) for c, d in pairs]
    assert len(keys) == len(set(keys)), (
        f"Hash collision detected among pairs: {pairs}"
    )
