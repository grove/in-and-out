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
    conditional_write_required = 1
    optimistic = 2
    fire_and_forget = 3


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


class WritebackConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    protection_level: ProtectionLevel
    conflict_resolution: ConflictResolution
    supported_actions: list[str] = Field(min_length=1)
    dependencies: list[DependencyConfig] = []
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
