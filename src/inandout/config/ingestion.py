"""Ingestion datatype configuration models.

Covers schemas/defs/ingestion.schema.json.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from inandout.config.pagination import PaginationConfig

if TYPE_CHECKING:
    from inandout.ingestion.cdc import CdcSourceConfig


class HistoryMode(StrEnum):
    overwrite = "overwrite"
    append = "append"


class PrimaryKeyExpression(BaseModel):
    model_config = ConfigDict(extra="allow")

    expression: str


# primary_key: single field name | composite list | expression object
PrimaryKey = str | list[str] | PrimaryKeyExpression


class ScheduleConfig(BaseModel):
    """Exactly one of interval or cron must be provided."""

    model_config = ConfigDict(extra="forbid")

    interval: str | None = None
    cron: str | None = None
    max_lag_seconds: int | None = None

    @model_validator(mode="after")
    def interval_or_cron_required(self) -> "ScheduleConfig":
        if self.interval is None and self.cron is None:
            raise ValueError("schedule requires either 'interval' or 'cron'")
        return self


class IncrementalCursorType(StrEnum):
    timestamp = "timestamp"
    cursor = "cursor"
    offset = "offset"
    sequence = "sequence"


class RequestFilterMode(StrEnum):
    query_param = "query_param"
    body_param = "body_param"
    sort_filter = "sort_filter"


class RequestFilterConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    mode: RequestFilterMode


class IncrementalConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    cursor_field: str | None = None
    cursor_type: IncrementalCursorType | None = None
    request_filter: RequestFilterConfig | None = None


class ListConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: str
    path: str
    record_selector: str | None = None
    pagination: PaginationConfig
    incremental: IncrementalConfig | None = None


class WebhookPayloadType(StrEnum):
    notification = "notification"
    full_state = "full_state"
    partial = "partial"


class WebhookSubscription(BaseModel):
    model_config = ConfigDict(extra="allow")


class WebhookEventsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    subscriptions: list[dict[str, Any]] = Field(min_length=1)
    record_id_path: str
    payload_type: WebhookPayloadType
    ordering: dict[str, Any]
    debounce: dict[str, Any] | None = None


class IngestionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    primary_key: PrimaryKey
    history_mode: HistoryMode
    schedule: ScheduleConfig
    list: ListConfig = Field(alias="list")
    webhook_events: WebhookEventsConfig | None = None
    prune_orphan_columns: bool = False
    source_mode: Literal["polling", "cdc"] = "polling"
    cdc: Any | None = None  # CdcSourceConfig | None — imported lazily to avoid circular imports
