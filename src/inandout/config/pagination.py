"""Pagination configuration models.

Covers all strategies declared in schemas/defs/pagination.schema.json:
  cursor, offset, link_header, page_number

Rule CFG-001: cursor strategy requires cursor.response_path AND cursor.request_param.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator


class PaginationStrategy(StrEnum):
    cursor = "cursor"
    offset = "offset"
    link_header = "link_header"
    page_number = "page_number"


class CursorConfig(BaseModel):
    """Required when strategy == 'cursor'. Enforces CFG-001."""

    model_config = ConfigDict(extra="forbid")

    response_path: str
    request_param: str | None = None

    @model_validator(mode="after")
    def request_param_required(self) -> "CursorConfig":
        """CFG-001: cursor must have both response_path and request_param."""
        if self.request_param is None:
            raise ValueError(
                "CFG-001: cursor strategy requires cursor.response_path and cursor.request_param"
            )
        return self


class PaginationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: PaginationStrategy
    cursor: CursorConfig | None = None
    offset: dict[str, Any] | None = None
    link_header: dict[str, Any] | None = None
    page_number: dict[str, Any] | None = None
    termination: list[str | dict[str, Any]] | None = None

    @model_validator(mode="after")
    def cursor_required_when_strategy_is_cursor(self) -> "PaginationConfig":
        """CFG-001: cursor strategy requires cursor.response_path and cursor.request_param."""
        if self.strategy == PaginationStrategy.cursor and self.cursor is None:
            raise ValueError(
                "CFG-001: cursor strategy requires cursor.response_path and cursor.request_param"
            )
        return self
