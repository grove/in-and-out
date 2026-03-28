"""Data quality rule configuration models."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class QualityRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required: list[str] = []  # fields that must be non-null/non-empty
    unique_within_batch: list[str] = []  # fields unique within a sync batch
    regex: dict[str, str] = {}  # field → regex pattern
    min_length: dict[str, int] = {}  # field → minimum string length
    max_length: dict[str, int] = {}  # field → maximum string length
    allowed_values: dict[str, list[Any]] = {}  # field → allowed value list
