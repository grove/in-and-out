"""Ingestion datatype configuration models.

Covers schemas/defs/ingestion.schema.json.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from inandout.config.pagination import PaginationConfig


class OutOfOrderStrategy(StrEnum):
    accept_latest_timestamp = "accept_latest_timestamp"
    accept_highest_sequence = "accept_highest_sequence"
    buffer_and_reorder = "buffer_and_reorder"  # T1 #35: buffer events and wait for missing
    ignore = "ignore"


class OutOfOrderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: OutOfOrderStrategy = OutOfOrderStrategy.accept_latest_timestamp
    timestamp_field: str = "updated_at"   # field in payload to compare
    sequence_field: str | None = None     # field for sequence number comparison
    buffer_timeout_secs: float = 30.0     # max time to wait for missing events (buffer_and_reorder only)
    buffer_size: int = 100                # max events to buffer per external_id (buffer_and_reorder only)

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
    until_param: str | None = None  # param name for window-end timestamp injection


class IncrementalConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    cursor_field: str | None = None
    cursor_type: IncrementalCursorType | None = None
    request_filter: RequestFilterConfig | None = None
    cursor_window: str | None = None  # e.g. "1d" — max time window per poll cycle


class BulkExportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    submit_path: str                   # POST to submit export job
    submit_method: str = "POST"
    status_path: str                   # GET {status_path}/{job_id} to poll status
    status_field: str = "status"       # field in status response
    complete_values: list[str] = ["completed", "done", "success"]
    failed_values: list[str] = ["failed", "error"]
    download_path: str                 # GET {download_path}/{job_id} to download
    job_id_field: str = "id"           # field in submit response containing job ID
    poll_interval: str = "30s"
    max_wait: str = "4h"
    result_format: Literal["jsonl", "csv", "json_array"] = "jsonl"
    record_selector: str | None = None  # for json_array: dot-notation path to records


class ListConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: str = "GET"
    path: str
    record_selector: str | None = None
    pagination: PaginationConfig
    incremental: IncrementalConfig | None = None
    graphql_query: str | None = None  # GraphQL query string
    graphql_variables: dict = {}  # static variables merged with runtime vars
    graphql_data_path: str | None = None  # dot-notation path e.g. "data.contacts.nodes"
    detail_path: str | None = None  # e.g. "/contacts/${external_id}" — GET this to verify deletion
    # A2: ID-first / parameterized-sources strategy
    fetch_strategy: Literal["list", "id_list"] = "list"
    id_field: str = "id"              # field in list response items that holds the ID
    detail_concurrency: int = 5       # max concurrent detail GETs
    # A4: declarative field/property selection
    properties: list[str] = []                                           # fields to request (empty = request all)
    properties_param: str = "properties"                                  # query/body param name
    properties_format: Literal["comma", "array", "json_array"] = "comma"  # encoding format
    # A2: pagination drift protection
    drift_protection: bool = True
    drift_max_shrink_pct: float = 50.0   # trip circuit breaker if result set shrinks >50%
    drift_min_records: int = 0           # minimum expected records; 0 = use previous run's count
    snapshot_param: str | None = None    # query param for server-side snapshot
    reconciliation_pass: bool = False    # after full page fetch, re-query changed records
    # A5: bulk export support
    bulk_export: BulkExportConfig | None = None


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
    out_of_order: OutOfOrderConfig = Field(default_factory=OutOfOrderConfig)


class IngestionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    primary_key: PrimaryKey
    primary_key_expression: str | None = None  # A2: "{account_id}:{contact_id}" style; takes precedence over primary_key
    history_mode: HistoryMode
    schedule: ScheduleConfig
    list: ListConfig = Field(alias="list")
    webhook_events: WebhookEventsConfig | None = None
    prune_orphan_columns: bool = False
    max_concurrent_fetches: int = 1  # parallelism for fan-out fetch (1 = no parallelism)
    bulk_upsert_batch_size: int = 1  # 1 = single-record path; >1 = bulk batch path
    verify_deletion: bool = True  # confirm each tombstone via detail_path GET before marking deleted
    checkpoint_every_n_pages: int = 0  # 0 = disabled; >0 = save checkpoint every N pages
    # T1 #44: exponential back-off for source unavailability
    unavailability_cooldown_secs: float = 300.0        # base back-off window (seconds)
    unavailability_backoff_multiplier: float = 2.0     # cooldown doubles on each consecutive skip
    unavailability_backoff_ceiling_secs: float = 3600.0  # max back-off cap (1 hour)
