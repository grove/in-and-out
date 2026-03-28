"""Unit tests for PaginationConfig and CursorConfig (CFG-001)."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.pagination import (
    CursorConfig,
    PaginationConfig,
    PaginationStrategy,
)


# --- PaginationStrategy enum ---

def test_strategy_values():
    assert PaginationStrategy.cursor == "cursor"
    assert PaginationStrategy.offset == "offset"
    assert PaginationStrategy.link_header == "link_header"
    assert PaginationStrategy.page_number == "page_number"


# --- CursorConfig (CFG-001) ---

def test_cursor_config_valid():
    cfg = CursorConfig(response_path="next_cursor", request_param="cursor")
    assert cfg.response_path == "next_cursor"
    assert cfg.request_param == "cursor"


def test_cursor_config_missing_request_param_raises():
    with pytest.raises(ValidationError, match="CFG-001"):
        CursorConfig(response_path="next_cursor")


def test_cursor_config_extra_field_forbidden():
    with pytest.raises(ValidationError):
        CursorConfig(response_path="next", request_param="p", extra_field="bad")


# --- PaginationConfig: link_header strategy ---

def test_link_header_strategy_valid():
    cfg = PaginationConfig(strategy="link_header")
    assert cfg.strategy == PaginationStrategy.link_header


def test_offset_strategy_valid():
    cfg = PaginationConfig(strategy="offset")
    assert cfg.strategy == PaginationStrategy.offset


def test_page_number_strategy_valid():
    cfg = PaginationConfig(strategy="page_number")
    assert cfg.strategy == PaginationStrategy.page_number


# --- PaginationConfig: cursor strategy requires cursor ---

def test_cursor_strategy_without_cursor_raises():
    with pytest.raises(ValidationError, match="CFG-001"):
        PaginationConfig(strategy="cursor")


def test_cursor_strategy_with_cursor_valid():
    cursor = CursorConfig(response_path="meta.next", request_param="after")
    cfg = PaginationConfig(strategy="cursor", cursor=cursor)
    assert cfg.strategy == PaginationStrategy.cursor
    assert cfg.cursor.response_path == "meta.next"


# --- PaginationConfig: extra fields forbidden ---

def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        PaginationConfig(strategy="offset", unknown="bad")


# --- PaginationConfig: optional fields ---

def test_cursor_default_none():
    cfg = PaginationConfig(strategy="link_header")
    assert cfg.cursor is None


def test_termination_default_none():
    cfg = PaginationConfig(strategy="offset")
    assert cfg.termination is None


def test_termination_set():
    cfg = PaginationConfig(strategy="offset", termination=["empty_list"])
    assert cfg.termination == ["empty_list"]


def test_offset_dict_set():
    cfg = PaginationConfig(strategy="offset", offset={"param": "skip", "step": 50})
    assert cfg.offset["param"] == "skip"


# --- CursorConfig: page_size and page_size_param ---

def test_cursor_config_with_page_size_and_param():
    cfg = CursorConfig(response_path="next", request_param="after", page_size=100, page_size_param="limit")
    assert cfg.page_size == 100
    assert cfg.page_size_param == "limit"


def test_cursor_config_page_size_without_param_raises():
    """page_size without page_size_param must fail with a clear error."""
    with pytest.raises(ValidationError, match="page_size_param is required"):
        CursorConfig(response_path="next", request_param="after", page_size=100)


def test_cursor_config_no_page_size_no_param_ok():
    """Omitting both page_size and page_size_param is valid (no limit injected)."""
    cfg = CursorConfig(response_path="next", request_param="after")
    assert cfg.page_size is None
    assert cfg.page_size_param is None


def test_round_trip_json_cursor():
    cursor = CursorConfig(response_path="meta.cursor", request_param="page_token")
    cfg = PaginationConfig(strategy="cursor", cursor=cursor)
    loaded = PaginationConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.cursor.response_path == "meta.cursor"


def test_missing_strategy_raises():
    with pytest.raises(ValidationError):
        PaginationConfig()
