"""Field mapping and transformation DSL execution."""
from __future__ import annotations

import datetime
from typing import Any

from inandout.config.field_mapping import FieldMapping


def _get_nested(record: dict[str, Any], path: str) -> Any:
    """Traverse dot-notation path in a nested dict. Returns None if any key is missing."""
    parts = path.split(".")
    cur: Any = record
    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


_CAST_FUNCS: dict[str, Any] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "datetime": datetime.datetime.fromisoformat,
    "date": datetime.date.fromisoformat,
}


def _cast_value(value: Any, cast: str) -> Any:
    """Apply the cast function to *value*."""
    func = _CAST_FUNCS.get(cast)
    if func is None:
        return value
    return func(value)


def apply_field_mappings(
    record: dict[str, Any],
    mappings: list[FieldMapping],
    strict: bool = False,
) -> dict[str, Any]:
    """Apply field mappings to a record.

    If *mappings* is empty, return *record* as-is.

    If *strict* is True, only mapped fields are kept.
    Otherwise, unmapped fields are passed through.
    """
    if not mappings:
        return record

    result: dict[str, Any] = {}

    # Track which source top-level keys are mapped (for pass-through in non-strict mode)
    mapped_sources: set[str] = set()

    for mapping in mappings:
        mapped_sources.add(mapping.source.split(".")[0])
        value = _get_nested(record, mapping.source)

        if value is None:
            value = mapping.default
        elif mapping.cast is not None:
            try:
                value = _cast_value(value, mapping.cast)
            except (ValueError, TypeError):
                value = mapping.default

        result[mapping.target] = value

    if not strict:
        # Pass through fields not mentioned in mappings
        for key, val in record.items():
            if key not in mapped_sources and key not in result:
                result[key] = val

    return result
