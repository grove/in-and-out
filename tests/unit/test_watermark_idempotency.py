"""Unit tests for set_watermark idempotency / overwrite behaviour.

Verifies that calling set_watermark a second time with a newer value
produces an upsert whose params contain the new value, not the first value.
(The ON CONFLICT DO UPDATE semantics guarantee the latest call wins.)
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from inandout.postgres.watermark import set_watermark


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_conn() -> tuple[AsyncMock, list[str], list[list]]:
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        params_list.append(list(params) if params else [])
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=None)
        return cur

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    return conn, sql_list, params_list


# ---------------------------------------------------------------------------
# Second call uses the new watermark value
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_second_set_watermark_uses_new_value():
    """Each call to set_watermark sends its own watermark_value in the params."""
    conn, sql_list, params_list = _make_conn()
    run1 = uuid.uuid4()
    run2 = uuid.uuid4()

    await set_watermark(conn, "hubspot", "contacts", "cursor", "page-1", run1)
    await set_watermark(conn, "hubspot", "contacts", "cursor", "page-99", run2)

    assert len(params_list) == 2
    assert params_list[0][3] == "page-1"
    assert params_list[1][3] == "page-99"


@pytest.mark.anyio
async def test_set_watermark_upsert_sql_present_on_both_calls():
    """Both calls must issue INSERT ... ON CONFLICT DO UPDATE."""
    conn, sql_list, _ = _make_conn()
    run = uuid.uuid4()

    await set_watermark(conn, "hubspot", "contacts", "cursor", "v1", run)
    await set_watermark(conn, "hubspot", "contacts", "cursor", "v2", run)

    upserts = [s for s in sql_list if "ON CONFLICT" in s]
    assert len(upserts) == 2


@pytest.mark.anyio
async def test_set_watermark_connector_and_datatype_in_both_calls():
    """connector and datatype params must be correct on every call."""
    conn, _, params_list = _make_conn()
    run = uuid.uuid4()

    await set_watermark(conn, "salesforce", "deals", "timestamp", "t1", run)
    await set_watermark(conn, "salesforce", "deals", "timestamp", "t2", run)

    for p in params_list:
        assert p[0] == "salesforce"
        assert p[1] == "deals"
