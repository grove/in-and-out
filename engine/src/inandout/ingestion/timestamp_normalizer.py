"""Timestamp normalisation utilities (T1 #45).

Converts timestamps in various formats to UTC ISO 8601 strings.
"""
from __future__ import annotations

import re
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# Boundary between unix-seconds and unix-milliseconds: 1e10 seconds ≈ year 2286
# Anything >= 1e10 is treated as milliseconds.
_UNIX_MILLIS_THRESHOLD = 1e10


def normalize_timestamp(value: Any, fmt: str) -> str | None:
    """Normalize *value* to UTC ISO 8601 string.

    Parameters
    ----------
    value:
        Raw timestamp value from the API response.
    fmt:
        One of 'iso8601', 'unix_seconds', 'unix_millis', 'rfc2822', 'auto'.

    Returns
    -------
    str | None
        UTC ISO 8601 string (e.g. ``"2026-01-15T12:00:00Z"``) or None on failure.
    """
    if value is None:
        return None

    if fmt == "auto":
        return _auto_detect(value)
    if fmt == "unix_seconds":
        return _from_unix(value, millis=False)
    if fmt == "unix_millis":
        return _from_unix(value, millis=True)
    if fmt == "iso8601":
        return _from_iso8601(value)
    if fmt == "rfc2822":
        return _from_rfc2822(value)
    return None


def _auto_detect(value: Any) -> str | None:
    """Try all formats in order and return the first successful parse."""
    # Numeric → unix epoch
    if isinstance(value, (int, float)):
        if value >= _UNIX_MILLIS_THRESHOLD:
            return _from_unix(value, millis=True)
        return _from_unix(value, millis=False)

    if not isinstance(value, str):
        return None

    # Try ISO 8601
    result = _from_iso8601(value)
    if result is not None:
        return result

    # Try RFC 2822
    result = _from_rfc2822(value)
    if result is not None:
        return result

    # Try numeric string
    try:
        numeric = float(value)
        if numeric >= _UNIX_MILLIS_THRESHOLD:
            return _from_unix(numeric, millis=True)
        return _from_unix(numeric, millis=False)
    except ValueError:
        pass

    return None


def _from_unix(value: Any, *, millis: bool) -> str | None:
    try:
        import datetime
        ts = float(value)
        if millis:
            ts = ts / 1000.0
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _from_iso8601(value: Any) -> str | None:
    """Parse ISO 8601 string (with or without timezone offset) → UTC."""
    if not isinstance(value, str):
        return None
    import datetime

    # Python 3.11+ fromisoformat handles timezone offsets; for 3.9/3.10 we handle manually.
    # Strip trailing 'Z' and replace with '+00:00' for fromisoformat compatibility
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.datetime.fromisoformat(s)
    except ValueError:
        return None

    if dt.tzinfo is None:
        # Assume UTC if no timezone info
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _from_rfc2822(value: Any) -> str | None:
    """Parse RFC 2822 date string (e.g. 'Mon, 15 Jan 2026 12:00:00 +0000') → UTC."""
    if not isinstance(value, str):
        return None
    try:
        from email.utils import parsedate_to_datetime
        import datetime
        dt = parsedate_to_datetime(value.strip())
        dt_utc = dt.astimezone(datetime.timezone.utc)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def apply_timestamp_normalization(
    record: dict[str, Any],
    configs: list[Any],  # list[TimestampFieldConfig]
) -> dict[str, Any]:
    """Apply all timestamp normalisations to *record*. Returns modified copy.

    On failure for any field: logs a warning and leaves original value unchanged.
    """
    if not configs:
        return record

    result = dict(record)
    for cfg in configs:
        field = cfg.field
        if field not in result:
            continue
        raw_value = result[field]
        normalized = normalize_timestamp(raw_value, cfg.format)
        if normalized is None:
            logger.warning(
                "timestamp_normalization_failed",
                field=field,
                raw_value=raw_value,
                fmt=cfg.format,
            )
            # Leave original value unchanged
        else:
            target = cfg.target_field if cfg.target_field else field
            result[target] = normalized

    return result
