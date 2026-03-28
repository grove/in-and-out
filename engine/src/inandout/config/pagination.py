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
    keyset = "keyset"  # T2 #12: keyset / seek pagination (WHERE id > :last_id ORDER BY id)


class CursorConfig(BaseModel):
    """Required when strategy == 'cursor'. Enforces CFG-001."""

    model_config = ConfigDict(extra="forbid")

    response_path: str
    request_param: str | None = None
    page_size: int | None = None         # optional page size sent on every request (e.g. HubSpot limit=100)
    page_size_param: str | None = None   # query param name for page size — required when page_size is set

    @model_validator(mode="after")
    def request_param_required(self) -> "CursorConfig":
        """CFG-001: cursor must have both response_path and request_param."""
        if self.request_param is None:
            raise ValueError(
                "CFG-001: cursor strategy requires cursor.response_path and cursor.request_param"
            )
        if self.page_size is not None and self.page_size_param is None:
            raise ValueError(
                "CFG-001: cursor.page_size_param is required when cursor.page_size is set"
            )
        return self


class KeysetConfig(BaseModel):
    """Required when strategy == 'keyset'.  Enforces seek-based pagination.

    Records are fetched in pages ordered by *keyset_field* with the last seen
    value passed as a query parameter on each subsequent request.  The loop
    stops when a page smaller than *page_size* is returned.

    Example API shape:  GET /records?after=<last_id>&limit=100
    """

    model_config = ConfigDict(extra="forbid")

    keyset_field: str                # field in each record used as the seek key (e.g. "id")
    request_param: str               # query param name for the seek value (e.g. "after")
    page_size: int = 100
    page_size_param: str = "limit"   # query param name for page size


class PaginationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: PaginationStrategy
    cursor: CursorConfig | None = None
    offset: dict[str, Any] | None = None
    link_header: dict[str, Any] | None = None
    page_number: dict[str, Any] | None = None
    keyset: KeysetConfig | None = None  # T2 #12
    termination: list[str | dict[str, Any]] | None = None

    @model_validator(mode="after")
    def cursor_required_when_strategy_is_cursor(self) -> "PaginationConfig":
        """CFG-001: cursor strategy requires cursor.response_path and cursor.request_param."""
        if self.strategy == PaginationStrategy.cursor and self.cursor is None:
            raise ValueError(
                "CFG-001: cursor strategy requires cursor.response_path and cursor.request_param"
            )
        return self
