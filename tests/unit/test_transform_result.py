"""Unit tests for TransformResult dataclass fields and default values."""
from __future__ import annotations

import pytest

from inandout.deadletter.transform import TransformResult


def test_default_all_zero():
    r = TransformResult()
    assert r.processed == 0
    assert r.upserted == 0
    assert r.dropped == 0
    assert r.failed == 0


def test_fields_assignable():
    r = TransformResult()
    r.processed = 10
    r.upserted = 8
    r.dropped = 1
    r.failed = 1
    assert r.processed == 10
    assert r.upserted == 8
    assert r.dropped == 1
    assert r.failed == 1


def test_init_with_values():
    r = TransformResult(processed=5, upserted=4, dropped=1, failed=0)
    assert r.processed == 5
    assert r.upserted == 4
    assert r.dropped == 1
    assert r.failed == 0


def test_increment_pattern():
    r = TransformResult()
    for _ in range(3):
        r.processed += 1
    r.upserted += 2
    r.dropped += 1
    assert r.processed == 3
    assert r.upserted == 2
    assert r.dropped == 1
    assert r.failed == 0


def test_counts_are_consistent():
    r = TransformResult(processed=10, upserted=7, dropped=2, failed=1)
    assert r.upserted + r.dropped + r.failed == r.processed


def test_is_dataclass():
    from dataclasses import is_dataclass
    assert is_dataclass(TransformResult)
