"""Data quality validation for ingested records."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from inandout.config.quality import QualityRule


@dataclass
class QualityViolation:
    field: str
    rule: str
    value: Any
    message: str


def validate_record(
    record: dict[str, Any],
    rules: QualityRule,
    seen: dict[str, set[Any]],
) -> list[QualityViolation]:
    """Validate a record against quality rules.

    Args:
        record: The record dict to validate.
        rules: The QualityRule config to apply.
        seen: Mutable dict of field → set of seen values (updated in-place on pass).

    Returns:
        List of QualityViolation objects. Empty list means all checks passed.
    """
    violations: list[QualityViolation] = []

    # --- required ---
    for field in rules.required:
        val = record.get(field)
        if val is None or val == "":
            violations.append(
                QualityViolation(
                    field=field,
                    rule="required",
                    value=val,
                    message=f"Field '{field}' is required but missing or empty",
                )
            )

    # --- unique_within_batch ---
    for field in rules.unique_within_batch:
        val = record.get(field)
        bucket = seen.setdefault(field, set())
        if val in bucket:
            violations.append(
                QualityViolation(
                    field=field,
                    rule="unique_within_batch",
                    value=val,
                    message=f"Field '{field}' value {val!r} already seen in this batch",
                )
            )
        else:
            bucket.add(val)

    # --- regex ---
    for field, pattern in rules.regex.items():
        val = record.get(field)
        if val is not None:
            if not re.fullmatch(pattern, str(val)):
                violations.append(
                    QualityViolation(
                        field=field,
                        rule="regex",
                        value=val,
                        message=f"Field '{field}' value {val!r} does not match pattern {pattern!r}",
                    )
                )

    # --- min_length ---
    for field, min_len in rules.min_length.items():
        val = record.get(field)
        if val is not None and len(str(val)) < min_len:
            violations.append(
                QualityViolation(
                    field=field,
                    rule="min_length",
                    value=val,
                    message=f"Field '{field}' length {len(str(val))} < min {min_len}",
                )
            )

    # --- max_length ---
    for field, max_len in rules.max_length.items():
        val = record.get(field)
        if val is not None and len(str(val)) > max_len:
            violations.append(
                QualityViolation(
                    field=field,
                    rule="max_length",
                    value=val,
                    message=f"Field '{field}' length {len(str(val))} > max {max_len}",
                )
            )

    # --- allowed_values ---
    for field, allowed in rules.allowed_values.items():
        val = record.get(field)
        if val is not None and val not in allowed:
            violations.append(
                QualityViolation(
                    field=field,
                    rule="allowed_values",
                    value=val,
                    message=f"Field '{field}' value {val!r} not in allowed list {allowed}",
                )
            )

    return violations
