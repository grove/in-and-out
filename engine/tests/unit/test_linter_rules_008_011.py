"""Unit tests for linter rules LINT008–LINT011."""
from __future__ import annotations

from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Helpers (mirrors the pattern from test_linter.py)
# ---------------------------------------------------------------------------


def _make_operation(method: str = "GET", path: str = "/records/${external_id}"):
    class _Op:
        def __init__(self) -> None:
            self.method = method
            self.path = path
    return _Op()


def _make_operations(has_lookup: bool = True, has_update: bool = False, has_insert: bool = False):
    class _Ops:
        def __init__(self) -> None:
            self.lookup = _make_operation() if has_lookup else None
            self.insert = _make_operation(method="POST", path="/records") if has_insert else None
            self.update = _make_operation(method="PATCH", path="/records/${external_id}") if has_update else None
            self.delete = None
            self.archive = None
            self.upsert = None
    return _Ops()


def _make_writeback(protection_level: int = 2, supported_actions: list[str] | None = None, has_lookup: bool = True, has_update: bool = False, has_insert: bool = False):
    from inandout.config.writeback import ProtectionLevel

    class _Writeback:
        def __init__(self) -> None:
            self.protection_level = ProtectionLevel(protection_level)
            self.operations = _make_operations(has_lookup=has_lookup, has_update=has_update, has_insert=has_insert)
            self.supported_actions = supported_actions or ["insert", "update"]
    return _Writeback()


def _make_ingestion():
    class _Schedule:
        interval = "5m"
        cron = None
        max_lag_seconds = None

    class _List:
        path = "/records"
        record_selector = None

        class _Pagination:
            def model_dump(self) -> dict:
                return {"strategy": "none"}

        pagination = _Pagination()

    class _Ingestion:
        primary_key = "id"
        schedule = _Schedule()
        list = _List()
    return _Ingestion()


def _make_dtype(ingestion: Any = None, writeback: Any = None, pii_fields: list | None = None):
    class _Dtype:
        pass

    d = _Dtype()
    d.ingestion = ingestion
    d.writeback = writeback
    d.pii_fields = pii_fields or []
    d.field_mappings = []
    return d


def _make_connector(name: str = "crm", datatypes: dict | None = None):
    class _Connector:
        pass

    c = _Connector()
    c.name = name
    c.datatypes = datatypes or {}
    c.depends_on = []

    class _Auth:
        credential_ref = None

        def model_dump(self) -> dict:
            return {}

    c.auth = _Auth()
    return c


def _make_file_cfg(connector: Any):
    class _FileCfg:
        pass

    f = _FileCfg()
    f.connector = connector

    def model_dump(self) -> dict:
        return {}

    f.model_dump = lambda: {}
    return f


# ---------------------------------------------------------------------------
# LINT008: protection_level=none warns
# ---------------------------------------------------------------------------


def test_lint008_protection_level_none_warns():
    from inandout.linter.rules import _lint008

    wb = _make_writeback(protection_level=0)  # ProtectionLevel.none = 0
    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=wb)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint008(cfg)
    assert len(diags) == 1
    assert diags[0].rule_id == "LINT008"
    assert diags[0].severity == "warning"
    assert "protection_level=none" in diags[0].message


def test_lint008_optimistic_level_no_diag():
    from inandout.linter.rules import _lint008

    wb = _make_writeback(protection_level=2)  # ProtectionLevel.optimistic = 2
    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=wb)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint008(cfg)
    assert diags == []


def test_lint008_no_writeback_no_diag():
    from inandout.linter.rules import _lint008

    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=None)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint008(cfg)
    assert diags == []


def test_lint008_multiple_datatypes_only_flags_affected():
    from inandout.linter.rules import _lint008

    wb_none = _make_writeback(protection_level=0)
    wb_opt = _make_writeback(protection_level=2)
    cfg = _make_file_cfg(_make_connector(datatypes={
        "contacts": _make_dtype(ingestion=_make_ingestion(), writeback=wb_none),
        "accounts": _make_dtype(ingestion=_make_ingestion(), writeback=wb_opt),
    }))

    diags = _lint008(cfg)
    assert len(diags) == 1
    assert "contacts" in diags[0].message


# ---------------------------------------------------------------------------
# LINT011: PII fields + writeback config → info
# ---------------------------------------------------------------------------


def test_lint011_pii_with_writeback_emits_info():
    from inandout.linter.rules import _lint011

    wb = _make_writeback()
    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=wb, pii_fields=["email", "phone"])
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint011(cfg)
    assert len(diags) == 1
    assert diags[0].rule_id == "LINT011"
    assert diags[0].severity == "info"
    assert "email" in diags[0].message
    assert "2 PII field" in diags[0].message


def test_lint011_pii_without_writeback_no_diag():
    from inandout.linter.rules import _lint011

    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=None, pii_fields=["email"])
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint011(cfg)
    assert diags == []


def test_lint011_no_pii_fields_no_diag():
    from inandout.linter.rules import _lint011

    wb = _make_writeback()
    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=wb, pii_fields=[])
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint011(cfg)
    assert diags == []


def test_lint011_many_pii_fields_truncated_in_message():
    from inandout.linter.rules import _lint011

    pii = ["email", "phone", "address", "ssn", "dob"]  # > 3 fields
    wb = _make_writeback()
    dtype = _make_dtype(ingestion=_make_ingestion(), writeback=wb, pii_fields=pii)
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = _lint011(cfg)
    assert len(diags) == 1
    assert "\u2026" in diags[0].message  # ellipsis present for truncation


# ---------------------------------------------------------------------------
# Integration: lint_connector runs all new rules
# ---------------------------------------------------------------------------


def test_lint_connector_runs_lint008_to_lint011():
    from inandout.linter import lint_connector

    wb_none = _make_writeback(protection_level=0, supported_actions=["update"])
    dtype = _make_dtype(
        ingestion=_make_ingestion(),
        writeback=wb_none,
        pii_fields=["email"],
    )
    cfg = _make_file_cfg(_make_connector(datatypes={"contacts": dtype}))

    diags = lint_connector(cfg)
    rule_ids = {d.rule_id for d in diags}
    assert "LINT008" in rule_ids  # protection_level=none
    assert "LINT011" in rule_ids  # pii + writeback
