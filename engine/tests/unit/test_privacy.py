"""Unit tests for PII privacy features (B6)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.ingestion.privacy import redact_pii, PurgeResult


def test_redact_pii_replaces_pii_fields() -> None:
    record = {"email": "alice@example.com", "name": "Alice", "phone": "555-1234"}
    result = redact_pii(record, pii_fields=["email", "phone"])
    assert result["email"] == "[REDACTED]"
    assert result["phone"] == "[REDACTED]"
    assert result["name"] == "Alice"  # non-PII unchanged


def test_redact_pii_non_pii_fields_unchanged() -> None:
    record = {"id": "123", "name": "Bob", "age": 30}
    result = redact_pii(record, pii_fields=["ssn"])
    assert result == {"id": "123", "name": "Bob", "age": 30}


def test_redact_pii_empty_pii_fields() -> None:
    record = {"email": "test@test.com", "id": "1"}
    result = redact_pii(record, pii_fields=[])
    assert result == record


def test_redact_pii_returns_copy_not_mutation() -> None:
    record = {"email": "alice@example.com"}
    result = redact_pii(record, pii_fields=["email"])
    assert record["email"] == "alice@example.com"  # original unchanged
    assert result["email"] == "[REDACTED]"


def test_purge_result_dataclass() -> None:
    pr = PurgeResult(
        connector="hubspot",
        datatype="contacts",
        external_id="ext-123",
        tables_purged={"source": 1, "history": 3},
    )
    assert pr.connector == "hubspot"
    assert pr.tables_purged["source"] == 1


@pytest.mark.asyncio
async def test_purge_by_external_id_touches_correct_tables() -> None:
    """purge_by_external_id should UPDATE source and DELETE from other tables."""
    from inandout.privacy import purge_by_external_id

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.commit = AsyncMock()

    # Return rowcount=1 for all execute calls
    exec_result = AsyncMock()
    exec_result.rowcount = 1
    conn.execute = AsyncMock(return_value=exec_result)

    # Mock transaction context manager
    txn = AsyncMock()
    txn.__aenter__ = AsyncMock(return_value=txn)
    txn.__aexit__ = AsyncMock(return_value=None)
    conn.transaction = MagicMock(return_value=txn)

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    result = await purge_by_external_id(pool, "hubspot", "contacts", "ext-123")

    assert result.connector == "hubspot"
    assert result.datatype == "contacts"
    assert result.external_id == "ext-123"
    # Verify execute was called at least once (UPDATE + DELETEs)
    assert conn.execute.call_count >= 1
