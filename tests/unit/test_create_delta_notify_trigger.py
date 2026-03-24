"""Unit tests for create_delta_notify_trigger in writeback/notify.py."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from inandout.writeback.notify import create_delta_notify_trigger


def _make_conn() -> AsyncMock:
    conn = AsyncMock()
    conn.execute = AsyncMock()
    return conn


async def test_executes_twice():
    """create_delta_notify_trigger must call conn.execute exactly twice (func + trigger)."""
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "_delta_crm_contacts")
    assert conn.execute.await_count == 2


async def test_first_call_is_function_ddl():
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "_delta_crm_contacts")
    func_sql = conn.execute.await_args_list[0].args[0]
    assert "CREATE OR REPLACE FUNCTION" in func_sql
    assert "RETURNS trigger" in func_sql


async def test_second_call_is_trigger_ddl():
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "_delta_crm_contacts")
    trigger_sql = conn.execute.await_args_list[1].args[0]
    assert "CREATE TRIGGER" in trigger_sql


async def test_connector_extracted_correctly():
    """For _delta_crm_contacts, connector='crm', datatype='contacts'."""
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "_delta_crm_contacts")
    func_sql = conn.execute.await_args_list[0].args[0]
    assert "crm" in func_sql
    assert "contacts" in func_sql


async def test_safe_name_in_function():
    """Table name is sanitised in function identifier (dots/dashes → underscores)."""
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "_delta_crm_contacts")
    func_sql = conn.execute.await_args_list[0].args[0]
    assert "inandout_notify__delta_crm_contacts" in func_sql


async def test_delta_table_in_trigger_ddl():
    """The original table name appears in the trigger DDL."""
    conn = _make_conn()
    table = "_delta_sfdc_leads"
    await create_delta_notify_trigger(conn, table)
    trigger_sql = conn.execute.await_args_list[1].args[0]
    assert table in trigger_sql


async def test_short_table_name_uses_delta_table_as_connector():
    """When table name can't be split into 3 parts, connector=table, datatype='unknown'."""
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "simple")
    func_sql = conn.execute.await_args_list[0].args[0]
    # datatype should be 'unknown'
    assert "unknown" in func_sql


async def test_schema_qualified_table():
    """Schema-qualified tables get their dots replaced in safe_name."""
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "myschema.delta_crm_contacts")
    func_sql = conn.execute.await_args_list[0].args[0]
    assert "myschema_delta_crm_contacts" in func_sql


async def test_trigger_name_in_trigger_ddl():
    conn = _make_conn()
    await create_delta_notify_trigger(conn, "_delta_crm_contacts")
    trigger_sql = conn.execute.await_args_list[1].args[0]
    assert "inandout_notify__delta_crm_contacts_trigger" in trigger_sql
