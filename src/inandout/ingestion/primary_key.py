"""Primary key extraction helpers (A2 — T1 #27).

Supports:
  - Single field name: ``primary_key="id"``
  - Python str.format expression: ``primary_key_expression="{account_id}:{contact_id}"``

``primary_key_expression`` takes precedence over ``primary_key`` when both are set.
"""
from __future__ import annotations

from typing import Any


def extract_primary_key(record: dict[str, Any], ingestion_cfg: Any) -> str:
    """Return the primary key string for *record* given *ingestion_cfg*.

    Raises:
        KeyError: if ``primary_key_expression`` references a missing field.
    """
    expression: str | None = getattr(ingestion_cfg, "primary_key_expression", None)
    if expression:
        try:
            return expression.format(**record)
        except KeyError as exc:
            missing_field = str(exc).strip("'\"")
            raise KeyError(
                f"primary_key_expression {expression!r} references missing field "
                f"{missing_field!r}. Available fields: {sorted(record.keys())}"
            ) from exc

    primary_key = ingestion_cfg.primary_key
    if isinstance(primary_key, str):
        return str(record.get(primary_key, ""))
    # List / expression types are handled by _extract_external_id in the engine;
    # fall back to empty string for unsupported types here.
    return str(record.get(str(primary_key), ""))


def validate_primary_key_expression(expression: str, sample_record: dict[str, Any]) -> bool:
    """Return True if *expression* can be evaluated against *sample_record* without error."""
    try:
        expression.format(**sample_record)
        return True
    except (KeyError, ValueError, IndexError):
        return False
