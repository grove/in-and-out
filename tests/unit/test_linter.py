"""Unit tests for the connector YAML linter (Step 70)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Helper factories to build mock ConnectorFileConfig objects
# ---------------------------------------------------------------------------

def _make_pagination(strategy: str = "none", cursor_response_path: str | None = "paging.next.after"):
    """Return a mock pagination object."""

    class _Cursor:
        def __init__(self) -> None:
            self.response_path = cursor_response_path
            self.request_param = "after"

        def model_dump(self) -> dict:
            return {
                "response_path": self.response_path,
                "request_param": self.request_param,
            }

    class _Pagination:
        def __init__(self) -> None:
            self.strategy = strategy
            self.cursor = _Cursor() if strategy == "cursor" else None

        def model_dump(self) -> dict:
            d: dict = {"strategy": self.strategy}
            if self.cursor:
                d["cursor"] = self.cursor.model_dump()
            return d

    return _Pagination()


def _make_schedule(interval: str = "5m", cron: str | None = None, max_lag_seconds: int | None = None):
    class _Schedule:
        def __init__(self) -> None:
            self.interval = interval
            self.cron = cron
            self.max_lag_seconds = max_lag_seconds
    return _Schedule()


def _make_list_config(
    path: str = "/records",
    record_selector: str | None = None,
    pagination: Any = None,
):
    class _List:
        def __init__(self) -> None:
            self.path = path
            self.record_selector = record_selector
            self.pagination = pagination or _make_pagination()
    return _List()


def _make_ingestion(
    primary_key: str = "id",
    schedule: Any = None,
    list_config: Any = None,
    prune_orphan_columns: bool = False,
):
    class _Ingestion:
        def __init__(self) -> None:
            self.primary_key = primary_key
            self.schedule = schedule or _make_schedule()
            self.list = list_config or _make_list_config()
            self.prune_orphan_columns = prune_orphan_columns
    return _Ingestion()


def _make_operations(has_lookup: bool = True):
    class _Lookup:
        method = "GET"
        path = "/records/${external_id}"

    class _Ops:
        def __init__(self) -> None:
            self.lookup = _Lookup() if has_lookup else None
            self.insert = None
            self.update = None
            self.delete = None
            self.archive = None

    return _Ops()


def _make_writeback(protection_level: int = 3, has_lookup: bool = True):
    from inandout.config.writeback import ProtectionLevel

    class _Writeback:
        def __init__(self) -> None:
            self.protection_level = ProtectionLevel(protection_level)
            self.operations = _make_operations(has_lookup=has_lookup)
    return _Writeback()


def _make_dtype(
    ingestion: Any = None,
    writeback: Any = None,
    field_mappings: list = None,
):
    class _Dtype:
        def __init__(self) -> None:
            self.ingestion = ingestion
            self.writeback = writeback
            self.field_mappings = field_mappings or []
    return _Dtype()


def _make_auth(credential_ref: str | None = None):
    class _Auth:
        def __init__(self) -> None:
            self.credential_ref = credential_ref

        def model_dump(self) -> dict:
            d: dict = {}
            if self.credential_ref:
                d["credential_ref"] = self.credential_ref
            return d

    return _Auth()


def _make_connector(
    name: str = "hubspot",
    datatypes: dict | None = None,
    depends_on: list[str] | None = None,
    auth: Any = None,
):
    class _Connector:
        def __init__(self) -> None:
            self.name = name
            self.datatypes = datatypes or {"contacts": _make_dtype(ingestion=_make_ingestion())}
            self.depends_on = depends_on or []
            self.auth = auth or _make_auth()
    return _Connector()


def _make_file_cfg(connector: Any = None):
    class _FileCfg:
        def __init__(self) -> None:
            self.connector = connector or _make_connector()

        def model_dump(self) -> dict:
            return {}

    return _FileCfg()


# ---------------------------------------------------------------------------
# LINT001: cursor pagination without response_path
# ---------------------------------------------------------------------------

def test_lint001_cursor_without_response_path():
    from inandout.linter import lint_connector

    pagination = _make_pagination(strategy="cursor", cursor_response_path=None)
    # Manually break: response_path is None
    pagination.cursor.response_path = None
    list_cfg = _make_list_config(pagination=pagination)
    ingestion = _make_ingestion(list_config=list_cfg)
    dtype = _make_dtype(ingestion=ingestion)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    error_diags = [d for d in diags if d.rule_id == "LINT001" and d.severity == "error"]
    assert len(error_diags) >= 1


def test_lint001_cursor_with_response_path_no_error():
    from inandout.linter import lint_connector

    pagination = _make_pagination(strategy="cursor", cursor_response_path="paging.next.after")
    list_cfg = _make_list_config(pagination=pagination)
    ingestion = _make_ingestion(list_config=list_cfg)
    dtype = _make_dtype(ingestion=ingestion)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    error_diags = [d for d in diags if d.rule_id == "LINT001"]
    assert len(error_diags) == 0


# ---------------------------------------------------------------------------
# LINT002: credential_ref not in environment
# ---------------------------------------------------------------------------

def test_lint002_missing_env_var_produces_warning():
    from inandout.linter import lint_connector

    env_var = "VERY_UNLIKELY_ENV_VAR_XYZ_12345"
    auth = _make_auth(credential_ref=env_var)
    connector = _make_connector(auth=auth)
    cfg = _make_file_cfg(connector)

    # Ensure the var is not set
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop(env_var, None)
        diags = lint_connector(cfg)

    warn_diags = [d for d in diags if d.rule_id == "LINT002" and d.severity == "warning"]
    assert len(warn_diags) >= 1


def test_lint002_present_env_var_no_warning():
    from inandout.linter import lint_connector

    env_var = "HUBSPOT_OAUTH_KEY_TEST_12345"
    auth = _make_auth(credential_ref=env_var)
    connector = _make_connector(auth=auth)
    cfg = _make_file_cfg(connector)

    with patch.dict(os.environ, {env_var: "secret_value"}):
        diags = lint_connector(cfg)

    warn_diags = [d for d in diags if d.rule_id == "LINT002"]
    assert len(warn_diags) == 0


# ---------------------------------------------------------------------------
# LINT005: optimistic without lookup
# ---------------------------------------------------------------------------

def test_lint005_optimistic_without_lookup_error():
    from inandout.linter import lint_connector

    writeback = _make_writeback(protection_level=2, has_lookup=False)
    dtype = _make_dtype(writeback=writeback)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    error_diags = [d for d in diags if d.rule_id == "LINT005" and d.severity == "error"]
    assert len(error_diags) >= 1


def test_lint005_optimistic_with_lookup_no_error():
    from inandout.linter import lint_connector

    writeback = _make_writeback(protection_level=2, has_lookup=True)
    dtype = _make_dtype(writeback=writeback)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    error_diags = [d for d in diags if d.rule_id == "LINT005"]
    assert len(error_diags) == 0


# ---------------------------------------------------------------------------
# LINT007: max_lag_seconds without alerting
# ---------------------------------------------------------------------------

def test_lint007_max_lag_without_alerting_info():
    from inandout.linter import lint_connector

    schedule = _make_schedule(max_lag_seconds=300)
    ingestion = _make_ingestion(schedule=schedule)
    dtype = _make_dtype(ingestion=ingestion)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    info_diags = [d for d in diags if d.rule_id == "LINT007" and d.severity == "info"]
    assert len(info_diags) >= 1


def test_lint007_no_max_lag_no_diagnostic():
    from inandout.linter import lint_connector

    schedule = _make_schedule(max_lag_seconds=None)
    ingestion = _make_ingestion(schedule=schedule)
    dtype = _make_dtype(ingestion=ingestion)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    info_diags = [d for d in diags if d.rule_id == "LINT007"]
    assert len(info_diags) == 0


# ---------------------------------------------------------------------------
# Clean connector → no errors
# ---------------------------------------------------------------------------

def test_clean_connector_no_diagnostics():
    from inandout.linter import lint_connector

    # Simple connector with no issues
    ingestion = _make_ingestion()
    dtype = _make_dtype(ingestion=ingestion)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    # No credential_ref, no cursor pagination, no optimistic, no max_lag
    diags = lint_connector(cfg)

    # Only info-level or below; no errors
    errors = [d for d in diags if d.severity == "error"]
    assert len(errors) == 0


# ---------------------------------------------------------------------------
# LintDiagnostic dataclass
# ---------------------------------------------------------------------------

def test_lint_diagnostic_dataclass():
    from inandout.linter import LintDiagnostic

    d = LintDiagnostic(
        severity="error",
        rule_id="LINT001",
        message="test message",
        path="connector.datatypes.x",
    )
    assert d.severity == "error"
    assert d.rule_id == "LINT001"
    assert d.message == "test message"
