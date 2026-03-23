"""Schema registry type definitions."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ColumnSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    pg_type: str
    nullable: bool = True


class ConnectorSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connector: str
    datatype: str
    version: str
    columns: list[ColumnSchema]
