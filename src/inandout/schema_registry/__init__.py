"""Schema registry package."""
from __future__ import annotations

from inandout.schema_registry.local import LocalSchemaRegistry, infer_schema_from_record
from inandout.schema_registry.types import ColumnSchema, ConnectorSchema

__all__ = [
    "ColumnSchema",
    "ConnectorSchema",
    "LocalSchemaRegistry",
    "infer_schema_from_record",
]
