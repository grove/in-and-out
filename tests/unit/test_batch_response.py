"""Unit tests for partial-success batch response handling (B4)."""
from __future__ import annotations

import pytest

from inandout.config.writeback import BatchResponseConfig
from inandout.writeback.batch_response import parse_batch_response, extract_batch_errors


def _make_cfg(**kwargs) -> BatchResponseConfig:
    return BatchResponseConfig(**kwargs)


def test_207_mixed_success_failure() -> None:
    """parse_batch_response should correctly classify per-record success/failure."""
    cfg = _make_cfg(
        success_path="results",
        record_id_path="id",
        status_path="status",
        success_statuses=["ok", "success"],
    )
    body = {
        "results": [
            {"id": "rec-1", "status": "ok"},
            {"id": "rec-2", "status": "error"},
            {"id": "rec-3", "status": "success"},
        ]
    }
    result = parse_batch_response(body, cfg)
    assert result == {"rec-1": True, "rec-2": False, "rec-3": True}


def test_all_success() -> None:
    cfg = _make_cfg(
        success_path="items",
        record_id_path="id",
        status_path="status",
        success_statuses=["200"],
    )
    body = {
        "items": [
            {"id": "a", "status": "200"},
            {"id": "b", "status": "200"},
        ]
    }
    result = parse_batch_response(body, cfg)
    assert result == {"a": True, "b": True}


def test_success_path_nested() -> None:
    """dot-notation success_path should traverse nested objects."""
    cfg = _make_cfg(
        success_path="data.records",
        record_id_path="id",
        status_path="result",
        success_statuses=["ok"],
    )
    body = {
        "data": {
            "records": [
                {"id": "x1", "result": "ok"},
                {"id": "x2", "result": "fail"},
            ]
        }
    }
    result = parse_batch_response(body, cfg)
    assert result["x1"] is True
    assert result["x2"] is False


def test_missing_record_id_path_skips_item() -> None:
    """Items without the record_id_path field should be skipped with a warning."""
    cfg = _make_cfg(
        record_id_path="external_id",
        status_path="status",
        success_statuses=["ok"],
    )
    body = [
        {"status": "ok"},   # missing external_id
        {"external_id": "r2", "status": "ok"},
    ]
    result = parse_batch_response(body, cfg)
    # Only "r2" should appear
    assert "r2" in result
    assert len(result) == 1


def test_extract_batch_errors() -> None:
    """extract_batch_errors should return {id: error_message} for failed records."""
    cfg = _make_cfg(
        success_path="results",
        record_id_path="id",
        status_path="status",
        success_statuses=["ok"],
        error_path="error_message",
    )
    body = {
        "results": [
            {"id": "r1", "status": "ok"},
            {"id": "r2", "status": "error", "error_message": "validation failed"},
        ]
    }
    errors = extract_batch_errors(body, cfg)
    assert "r1" not in errors
    assert errors["r2"] == "validation failed"
