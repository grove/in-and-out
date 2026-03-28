"""Unit tests for _write_operator_audit CLI helper."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch


def _call_audit(dsn: str, command: str, payload: dict, issued_by: str) -> None:
    from inandout.cli.main import _write_operator_audit
    _write_operator_audit(dsn, command, payload, issued_by)


class _FakeConn:
    """Minimal async context-manager connection stub."""

    def __init__(self) -> None:
        self.execute = AsyncMock()
        self.commit = AsyncMock()
        self._execute_calls: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def _make_fake_conn() -> _FakeConn:
    return _FakeConn()


# ---------------------------------------------------------------------------
# Test: ingest-run-started audit payload
# ---------------------------------------------------------------------------

def test_operator_audit_ingest_run_captures_command():
    """_write_operator_audit inserts the command name into inout_ops_control."""
    fake_conn = _make_fake_conn()

    with patch("psycopg.AsyncConnection.connect", new=AsyncMock(return_value=fake_conn)):
        _call_audit(
            dsn="postgresql://localhost/test",
            command="ingest-run-started",
            payload={"action": "ingest-run-started", "config": "config/ingestion.yaml"},
            issued_by="cli",
        )

    assert fake_conn.execute.called
    call_args = fake_conn.execute.call_args
    sql, params = call_args[0][0], call_args[0][1]
    assert "inout_ops_control" in sql
    assert "ingest-run-started" in params
    assert "cli" in params


def test_operator_audit_db_upgrade_payload():
    """_write_operator_audit records the revision for db upgrade."""
    fake_conn = _make_fake_conn()

    with patch("psycopg.AsyncConnection.connect", new=AsyncMock(return_value=fake_conn)):
        _call_audit(
            dsn="postgresql://localhost/test",
            command="db-upgrade",
            payload={"action": "db-upgrade", "revision": "head"},
            issued_by="testuser",
        )

    assert fake_conn.execute.called
    call_args = fake_conn.execute.call_args
    sql, params = call_args[0][0], call_args[0][1]
    assert "inout_ops_control" in sql
    assert "db-upgrade" in params
    assert "testuser" in params
    # Payload JSON must contain the revision
    payload_json = params[2]
    assert "head" in payload_json


def test_operator_audit_db_downgrade_payload():
    """_write_operator_audit records the revision for db downgrade."""
    fake_conn = _make_fake_conn()

    with patch("psycopg.AsyncConnection.connect", new=AsyncMock(return_value=fake_conn)):
        _call_audit(
            dsn="postgresql://localhost/test",
            command="db-downgrade",
            payload={"action": "db-downgrade", "revision": "-1"},
            issued_by="alice",
        )

    assert fake_conn.execute.called
    call_args = fake_conn.execute.call_args
    sql, params = call_args[0][0], call_args[0][1]
    assert "db-downgrade" in params
    assert "alice" in params
    payload_json = params[2]
    assert "-1" in payload_json


def test_operator_audit_swallows_db_error():
    """A DB error in _write_operator_audit must not propagate."""
    with patch("psycopg.AsyncConnection.connect", side_effect=Exception("connection refused")):
        # Must not raise
        _call_audit(
            dsn="postgresql://localhost/test",
            command="db-upgrade",
            payload={"action": "db-upgrade", "revision": "head"},
            issued_by="ci",
        )


def test_operator_audit_target_tool_is_cli():
    """The target_tool column must always be 'cli' for operator audit rows."""
    fake_conn = _make_fake_conn()

    with patch("psycopg.AsyncConnection.connect", new=AsyncMock(return_value=fake_conn)):
        _call_audit(
            dsn="postgresql://localhost/test",
            command="db-upgrade",
            payload={"action": "db-upgrade", "revision": "head"},
            issued_by="ci",
        )

    sql = fake_conn.execute.call_args[0][0]
    assert "'cli'" in sql or "cli" in sql


def test_operator_audit_status_is_completed():
    """The status must be 'completed' to signify a successfully issued command."""
    fake_conn = _make_fake_conn()

    with patch("psycopg.AsyncConnection.connect", new=AsyncMock(return_value=fake_conn)):
        _call_audit(
            dsn="postgresql://localhost/test",
            command="ingest-run-started",
            payload={"action": "ingest-run-started"},
            issued_by="ci",
        )

    sql = fake_conn.execute.call_args[0][0]
    assert "completed" in sql
