"""PII redaction helper (B6).

Provides utilities to redact PII fields from log payloads and a purge
function that wipes a specific external_id from all relevant tables.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def redact_pii(record: dict[str, Any], pii_fields: list[str]) -> dict[str, Any]:
    """Return a copy of *record* with PII fields replaced by ``"[REDACTED]"``."""
    if not pii_fields:
        return record
    result = dict(record)
    for f in pii_fields:
        if f in result:
            result[f] = "[REDACTED]"
    return result


@dataclass
class PurgeResult:
    connector: str
    datatype: str
    external_id: str
    tables_purged: dict[str, int] = field(default_factory=dict)
