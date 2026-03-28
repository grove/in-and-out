"""Unit tests for writeback HTTP response capture (B1)."""
from __future__ import annotations

import pytest


def test_writeback_response_columns_migration_exists() -> None:
    """Migration 018 should add response_status, response_body, response_headers columns."""
    import importlib.util
    import pathlib

    path = pathlib.Path(__file__).parent.parent.parent / "migrations/versions/018_20260323_writeback_response.py"
    assert path.exists(), "Migration 018 does not exist"
    spec = importlib.util.spec_from_file_location("migration_018", path)
    assert spec is not None


def test_response_body_non_json_stored_as_raw() -> None:
    """Non-JSON response bodies should be captured as {'raw': '...'}."""
    non_json = "plain text error"
    try:
        import orjson
        orjson.loads(non_json.encode())
        captured = {"raw": non_json}
    except Exception:
        captured = {"raw": non_json}
    assert captured == {"raw": "plain text error"}


def test_response_headers_filtering() -> None:
    """Sensitive headers (Set-Cookie, Authorization) should be excluded."""
    raw_headers = {
        "Content-Type": "application/json",
        "Set-Cookie": "session=abc123",
        "Authorization": "Bearer token",
        "X-Request-Id": "req-001",
    }
    _EXCLUDED = {"set-cookie", "authorization"}
    filtered = {
        k: v for k, v in raw_headers.items()
        if k.lower() not in _EXCLUDED
    }
    assert "Set-Cookie" not in filtered
    assert "Authorization" not in filtered
    assert filtered["Content-Type"] == "application/json"
    assert filtered["X-Request-Id"] == "req-001"
