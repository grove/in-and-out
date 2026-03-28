"""Unit tests for OperationConfig, UpdateOperationConfig, ConditionalWrite, OperationsConfig."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.writeback import (
    ConditionalWrite,
    OperationConfig,
    OperationsConfig,
    UpdateOperationConfig,
)


# --- OperationConfig ---

def test_operation_config_valid():
    op = OperationConfig(method="GET", path="/contacts/${external_id}")
    assert op.method == "GET"
    assert op.path == "/contacts/${external_id}"


def test_operation_config_missing_method_raises():
    with pytest.raises(ValidationError):
        OperationConfig(path="/contacts")


def test_operation_config_missing_path_raises():
    with pytest.raises(ValidationError):
        OperationConfig(method="GET")


def test_operation_config_extra_allowed():
    op = OperationConfig(method="GET", path="/path", custom="extra")
    assert op.custom == "extra"  # type: ignore[attr-defined]


# --- ConditionalWrite ---

def test_conditional_write_enabled_true():
    cw = ConditionalWrite(enabled=True, header="If-Match", value="${etag}")
    assert cw.enabled is True
    assert cw.header == "If-Match"


def test_conditional_write_enabled_false():
    cw = ConditionalWrite(enabled=False)
    assert cw.enabled is False


def test_conditional_write_header_default_none():
    cw = ConditionalWrite(enabled=True)
    assert cw.header is None


def test_conditional_write_value_default_none():
    cw = ConditionalWrite(enabled=True)
    assert cw.value is None


# --- UpdateOperationConfig ---

def test_update_operation_config_valid():
    op = UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}")
    assert op.method == "PATCH"
    assert op.conditional_write is None


def test_update_operation_with_conditional_write():
    cw = ConditionalWrite(enabled=True, header="If-Match")
    op = UpdateOperationConfig(method="PATCH", path="/contacts/${id}", conditional_write=cw)
    assert op.conditional_write.enabled is True


# --- OperationsConfig ---

def test_operations_config_lookup_required():
    with pytest.raises(ValidationError):
        OperationsConfig()


def test_operations_config_minimal():
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
    )
    assert ops.lookup.method == "GET"


def test_operations_config_insert_default_none():
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
    )
    assert ops.insert is None


def test_operations_config_update_set():
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${id}"),
        update=UpdateOperationConfig(method="PATCH", path="/contacts/${id}"),
    )
    assert ops.update.method == "PATCH"


def test_operations_config_delete_set():
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${id}"),
        delete=OperationConfig(method="DELETE", path="/contacts/${id}"),
    )
    assert ops.delete.method == "DELETE"


def test_operations_config_upsert_set():
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${id}"),
        upsert=OperationConfig(method="PUT", path="/contacts/${id}"),
    )
    assert ops.upsert.method == "PUT"


def test_operations_config_extra_allowed():
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/p"),
        custom_op="allowed",
    )
    assert ops.custom_op == "allowed"  # type: ignore[attr-defined]
