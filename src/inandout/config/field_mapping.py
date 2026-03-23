"""Field mapping and transformation DSL configuration model."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class FieldMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str  # dot-notation JSONPath: "properties.email" or "id"
    target: str  # destination column name
    cast: Literal["str", "int", "float", "bool", "datetime", "date"] | None = None
    default: Any = None  # value if source path is missing/null
