"""Unit tests for SchemaVersionMismatch exception."""
from __future__ import annotations

import pytest

from inandout.postgres.version_check import SchemaVersionMismatch


def test_attributes_stored():
    exc = SchemaVersionMismatch(current="018_20260323", expected=24)
    assert exc.current == "018_20260323"
    assert exc.expected == 24


def test_none_current():
    exc = SchemaVersionMismatch(current=None, expected=24)
    assert exc.current is None
    assert exc.expected == 24


def test_message_contains_current():
    exc = SchemaVersionMismatch(current="018_20260323", expected=24)
    assert "018_20260323" in str(exc)


def test_message_contains_expected():
    exc = SchemaVersionMismatch(current="005_20260323", expected=24)
    assert "24" in str(exc)


def test_message_hints_upgrade():
    exc = SchemaVersionMismatch(current=None, expected=24)
    msg = str(exc)
    assert "upgrade" in msg.lower() or "db" in msg.lower()


def test_is_exception_subclass():
    exc = SchemaVersionMismatch(current="001", expected=24)
    assert isinstance(exc, Exception)


def test_raise_and_catch():
    with pytest.raises(SchemaVersionMismatch) as info:
        raise SchemaVersionMismatch(current="003_old", expected=24)
    assert info.value.current == "003_old"
    assert info.value.expected == 24
