"""Unit tests for Step 53 — Multi-connector fan-in (JOIN writeback)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_join_source(
    connector: str = "crm",
    datatype: str = "companies",
    join_key: str = "company_id",
    fields: list[str] | None = None,
):
    from inandout.config.writeback import JoinSource
    return JoinSource(
        connector=connector,
        datatype=datatype,
        join_key=join_key,
        fields=fields or ["company_name", "industry"],
    )


def make_mock_pool(row_data: dict | None = None):
    """Return a mock pool that yields the given row."""
    mock_cursor = AsyncMock()
    if row_data is not None:
        values = list(row_data.values())
        mock_cursor.fetchone = AsyncMock(return_value=values)
        mock_cursor.description = [(k,) for k in row_data.keys()]
    else:
        mock_cursor.fetchone = AsyncMock(return_value=None)
        mock_cursor.description = []

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)
    return mock_pool


# ---------------------------------------------------------------------------
# test_single_join_source_enriches_payload
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_single_join_source_enriches_payload():
    """Single join source adds extra fields to the row."""
    from inandout.writeback.fan_in import enrich_with_join_sources

    join_src = make_join_source()
    pool = make_mock_pool({"company_name": "Acme Corp", "industry": "Tech"})

    row = {"id": "001", "company_id": "c-123", "amount": 5000}
    enriched = await enrich_with_join_sources(pool, row, [join_src])

    assert enriched["company_name"] == "Acme Corp"
    assert enriched["industry"] == "Tech"
    assert enriched["id"] == "001"  # original fields preserved
    assert enriched["amount"] == 5000


# ---------------------------------------------------------------------------
# test_missing_join_key_leaves_row_unchanged
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_missing_join_key_leaves_row_unchanged(caplog):
    """Missing join key → warning logged, row unchanged."""
    from inandout.writeback.fan_in import enrich_with_join_sources

    join_src = make_join_source(join_key="company_id")
    pool = make_mock_pool({"company_name": "Acme"})

    # Row does NOT have company_id
    row = {"id": "001", "amount": 5000}
    enriched = await enrich_with_join_sources(pool, row, [join_src])

    # Row unchanged
    assert enriched == row


# ---------------------------------------------------------------------------
# test_multiple_join_sources_all_fields_merged
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_multiple_join_sources_all_fields_merged():
    """Multiple join sources → all fields merged, no overwrites of primary fields."""
    from inandout.writeback.fan_in import enrich_with_join_sources
    from inandout.config.writeback import JoinSource

    src_a = JoinSource(connector="crm", datatype="companies", join_key="company_id", fields=["company_name"])
    src_b = JoinSource(connector="crm", datatype="owners", join_key="owner_id", fields=["owner_name"])

    # Two separate mock pools won't work with a single mock.
    # Instead, set up sequential responses.
    mock_cursor_a = AsyncMock()
    mock_cursor_a.fetchone = AsyncMock(return_value=["Acme Corp"])
    mock_cursor_a.description = [("company_name",)]

    mock_cursor_b = AsyncMock()
    mock_cursor_b.fetchone = AsyncMock(return_value=["Bob"])
    mock_cursor_b.description = [("owner_name",)]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(side_effect=[mock_cursor_a, mock_cursor_b])

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    row = {"id": "001", "company_id": "c-1", "owner_id": "o-1"}
    enriched = await enrich_with_join_sources(mock_pool, row, [src_a, src_b])

    assert enriched["company_name"] == "Acme Corp"
    assert enriched["owner_name"] == "Bob"
    assert enriched["id"] == "001"  # primary field not overwritten


@pytest.mark.anyio
async def test_join_source_no_overwrite_primary_fields():
    """Join source fields do NOT overwrite primary row fields."""
    from inandout.writeback.fan_in import enrich_with_join_sources

    join_src = make_join_source(fields=["amount"])  # 'amount' exists in primary row too
    pool = make_mock_pool({"amount": 9999})

    row = {"id": "001", "company_id": "c-123", "amount": 5000}
    enriched = await enrich_with_join_sources(pool, row, [join_src])

    # Primary field 'amount' should NOT be overwritten by join source
    assert enriched["amount"] == 5000


# ---------------------------------------------------------------------------
# test_join_source_row_not_found
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_join_source_row_not_found():
    """Join source row not found → warning, row unchanged."""
    from inandout.writeback.fan_in import enrich_with_join_sources

    join_src = make_join_source()
    pool = make_mock_pool(row_data=None)  # fetchone returns None

    row = {"id": "001", "company_id": "c-999"}
    enriched = await enrich_with_join_sources(pool, row, [join_src])

    # Row unchanged
    assert enriched == row


@pytest.mark.anyio
async def test_join_source_query_exception_leaves_row_unchanged():
    """DB exception during join query → warning logged, row unchanged."""
    from inandout.writeback.fan_in import enrich_with_join_sources

    join_src = make_join_source()

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(side_effect=Exception("relation does not exist"))

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    row = {"id": "001", "company_id": "c-123"}
    enriched = await enrich_with_join_sources(mock_pool, row, [join_src])

    # Row unchanged despite DB error
    assert enriched == row
