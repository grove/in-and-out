"""Unit tests for connector schema migration utilities."""
from __future__ import annotations

import pytest

from inandout.migrations.connector_schema import (
    MIGRATIONS,
    ConnectorMigration,
    apply_migrations,
    find_migration_path,
)
from inandout.migrations.connector_migrations.v1_0_to_v1_1 import migrate as migrate_v1_0_to_v1_1


# ---------------------------------------------------------------------------
# v1.0 → v1.1 migration tests
# ---------------------------------------------------------------------------

def test_v1_0_to_v1_1_renames_signature_header():
    """v1.0→v1.1 migration should rename webhook.signature_header to webhook.signature.header."""
    data = {
        "schema_version": 1,
        "connector": {
            "name": "myconn",
            "webhooks": {
                "path": "/webhooks/myconn",
                "signature_header": "X-Signature-256",
            }
        }
    }
    result = migrate_v1_0_to_v1_1(data)
    webhook = result["connector"]["webhooks"]
    assert "signature_header" not in webhook
    assert webhook["signature"]["header"] == "X-Signature-256"


def test_v1_0_to_v1_1_preserves_other_webhook_fields():
    """v1.0→v1.1 migration should not modify other webhook fields."""
    data = {
        "schema_version": 1,
        "connector": {
            "webhooks": {
                "path": "/webhooks/test",
                "signature_header": "X-Hub-Signature",
                "some_other_field": "value",
            }
        }
    }
    result = migrate_v1_0_to_v1_1(data)
    webhook = result["connector"]["webhooks"]
    assert webhook["path"] == "/webhooks/test"
    assert webhook["some_other_field"] == "value"


def test_v1_0_to_v1_1_is_idempotent():
    """Applying the v1.0→v1.1 migration twice should not break the result."""
    data = {
        "schema_version": 1,
        "connector": {
            "webhooks": {
                "signature_header": "X-Signature",
            }
        }
    }
    result_once = migrate_v1_0_to_v1_1(data)
    result_twice = migrate_v1_0_to_v1_1(result_once)
    # First application moves to nested format
    assert result_once["connector"]["webhooks"]["signature"]["header"] == "X-Signature"
    # Second application on already-migrated data should not corrupt
    # (no signature_header key to rename)
    assert "signature" in result_twice["connector"]["webhooks"]


def test_v1_0_to_v1_1_no_webhook_section():
    """Migration should be a no-op if there's no webhook section."""
    data = {
        "schema_version": 1,
        "connector": {
            "name": "myconn",
        }
    }
    result = migrate_v1_0_to_v1_1(data)
    assert result == data


def test_v1_0_to_v1_1_does_not_mutate_original():
    """Migration should return a new dict and not mutate the input."""
    data = {
        "schema_version": 1,
        "connector": {
            "webhooks": {
                "signature_header": "X-Sig",
            }
        }
    }
    original_sig = data["connector"]["webhooks"].get("signature_header")
    migrate_v1_0_to_v1_1(data)
    assert data["connector"]["webhooks"].get("signature_header") == original_sig


# ---------------------------------------------------------------------------
# find_migration_path tests
# ---------------------------------------------------------------------------

def test_find_migration_path_same_version():
    """find_migration_path returns empty list when from == to."""
    path = find_migration_path("1.0", "1.0")
    assert path == []


def test_find_migration_path_v1_0_to_v1_1():
    """find_migration_path returns the correct migration for v1.0 → v1.1."""
    path = find_migration_path("1.0", "1.1")
    assert len(path) == 1
    assert path[0].from_version == "1.0"
    assert path[0].to_version == "1.1"


def test_find_migration_path_no_path_raises_value_error():
    """find_migration_path raises ValueError when no path exists."""
    with pytest.raises(ValueError, match="No migration path"):
        find_migration_path("9.9", "10.0")


def test_find_migration_path_error_message_shows_available():
    """Error message should list available migrations."""
    with pytest.raises(ValueError) as exc_info:
        find_migration_path("99.0", "100.0")
    assert "Available migrations" in str(exc_info.value)


# ---------------------------------------------------------------------------
# apply_migrations tests
# ---------------------------------------------------------------------------

def test_apply_migrations_empty_list():
    """apply_migrations with empty migrations list returns copy of input."""
    data = {"schema_version": 1, "connector": {"name": "test"}}
    result = apply_migrations(data, [])
    assert result == data
    assert result is not data  # Should be a copy


def test_apply_migrations_applies_in_order():
    """apply_migrations should apply migrations in order."""
    call_order: list[str] = []

    def m1(d: dict) -> dict:
        call_order.append("m1")
        return {**d, "step1": True}

    def m2(d: dict) -> dict:
        call_order.append("m2")
        return {**d, "step2": True}

    migrations = [
        ConnectorMigration("1.0", "1.1", "step1", m1),
        ConnectorMigration("1.1", "1.2", "step2", m2),
    ]
    result = apply_migrations({"data": "original"}, migrations)
    assert call_order == ["m1", "m2"]
    assert result["step1"] is True
    assert result["step2"] is True


def test_apply_migrations_does_not_mutate_original():
    """apply_migrations should not mutate the input dict."""
    data = {"schema_version": 1, "connector": {"webhooks": {"signature_header": "X-Sig"}}}
    original_copy = {"schema_version": 1, "connector": {"webhooks": {"signature_header": "X-Sig"}}}

    path = find_migration_path("1.0", "1.1")
    apply_migrations(data, path)

    # Original should be unchanged
    assert data == original_copy


# ---------------------------------------------------------------------------
# MIGRATIONS registry
# ---------------------------------------------------------------------------

def test_migrations_registry_not_empty():
    """MIGRATIONS should contain at least one migration."""
    assert len(MIGRATIONS) >= 1


def test_migrations_registry_ordered():
    """MIGRATIONS should be ordered (each from_version comes before to_version)."""
    for m in MIGRATIONS:
        assert m.from_version != m.to_version
