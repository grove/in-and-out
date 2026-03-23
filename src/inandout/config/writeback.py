"""Writeback datatype configuration models.

Covers schemas/defs/writeback.schema.json.

Rule CFG-010: protection_level=1 requires operations.update.conditional_write.enabled = true.
Rule CFG-011: cyclic datatype dependency graph (checked at ConnectorConfig level).
"""

from __future__ import annotations

from enum import IntEnum, StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ProtectionLevel(IntEnum):
    none = 0
    conditional_write_required = 1
    optimistic = 2
    post_write_verify = 3


class ConflictResolution(StrEnum):
    dead_letter = "dead_letter"
    last_writer_wins = "last_writer_wins"
    skip_and_warn = "skip_and_warn"
    re_ingest_and_recompute = "re_ingest_and_recompute"
    server_wins = "server_wins"
    merge_fields = "merge_fields"
    custom_merge = "custom_merge"


class OperationConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    method: str
    path: str


class ConditionalWrite(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool
    header: str | None = None
    value: str | None = None


class UpdateOperationConfig(OperationConfig):
    conditional_write: ConditionalWrite | None = None


class DependencyConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    depends_on: str


class WriteDependency(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parent_datatype: str   # must be written before this datatype
    join_field: str        # field in this datatype's row that references parent's external_id


class OperationsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    lookup: OperationConfig
    insert: OperationConfig | None = None
    update: UpdateOperationConfig | None = None
    delete: OperationConfig | None = None
    archive: OperationConfig | None = None
    upsert: OperationConfig | None = None


class JoinSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connector: str
    datatype: str
    join_key: str  # column in both this source and the primary delta table
    fields: list[str]  # columns to pull from this source into the payload


class BatchResponseConfig(BaseModel):
    """Config for parsing partial-success batch API responses (T2 #29)."""

    model_config = ConfigDict(extra="forbid")

    success_path: str | None = None        # dot-notation path to array of results in response body
    record_id_path: str = "id"             # path to external_id within each result object
    status_path: str = "status"            # path to per-record status
    success_statuses: list[str] = ["ok", "success", "200"]
    error_path: str | None = None          # path to per-record error message


class WritebackConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    dry_run: bool = False  # B1: if True, skip HTTP writes and populate dry_run_log
    protection_level: ProtectionLevel
    conflict_resolution: ConflictResolution
    supported_actions: list[str] = Field(min_length=1)
    dependencies: list[DependencyConfig] = []
    write_dependencies: list[WriteDependency] = []  # ordering within write batch
    operations: OperationsConfig
    max_concurrent_writes: int = Field(default=10, ge=1)
    batch_size: int = Field(default=50, ge=1)
    etag_header: str = "ETag"
    if_match_header: str = "If-Match"
    diff_fields: bool = False
    streaming: bool = False
    join_sources: list[JoinSource] = []
    idempotency_key_header: str | None = None  # e.g. "Idempotency-Key"
    enable_crash_recovery: bool = True  # skip already-sent rows from audit log on restart
    use_desired_state_table: bool = False  # read delta rows from inout_dst_* instead of _delta_*
    batch_response: BatchResponseConfig | None = None  # B4: partial-success batch response parsing
    # T2 #6: CRDT-based conflict resolution strategy ('lww_register' | 'g_counter' | None)
    crdt_type: str | None = None
    crdt_ts_field: str = "_updated_at"  # field carrying the timestamp for lww_register
    # T2 #31: delete safety guard — abort the batch when delete-action count exceeds this limit
    max_deletes_per_batch: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_crdt_ts_field_requires_lww(self) -> "WritebackConfig":
        """CFG-014: crdt_ts_field is only meaningful with crdt_type='lww_register'."""
        if self.crdt_ts_field != "_updated_at" and self.crdt_type != "lww_register":
            raise ValueError(
                "CFG-014: crdt_ts_field is only used with crdt_type='lww_register'. "
                f"Got crdt_type={self.crdt_type!r}. Remove crdt_ts_field or set "
                "crdt_type='lww_register'."
            )
        return self

    @model_validator(mode="after")
    def validate_protection_level_pairing(self) -> "WritebackConfig":
        """CFG-010: protection_level=1 requires operations.update.conditional_write.enabled=true."""
        if self.protection_level == ProtectionLevel.conditional_write_required:
            update = self.operations.update
            if (
                update is None
                or update.conditional_write is None
                or update.conditional_write.enabled is not True
            ):
                raise ValueError(
                    "CFG-010: protection_level=1 requires "
                    "operations.update.conditional_write.enabled = true"
                )
        return self
