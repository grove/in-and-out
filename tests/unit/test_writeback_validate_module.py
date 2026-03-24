"""Unit tests for writeback/validate.py (T2 #37 — connector validation module)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.writeback.validate import (
    DatatypeValidationResult,
    WritebackValidateResult,
    probe_etag_support,
    validate_writeback_connector,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_connector(
    base_url: str = "https://api.example.com",
    etag_header: str = "ETag",
    protection_level_name: str = "optimistic",
    lookup_path: str = "/contacts/${external_id}",
) -> MagicMock:
    from inandout.config.writeback import ProtectionLevel

    pl = ProtectionLevel[protection_level_name]

    ops = MagicMock()
    ops.lookup = MagicMock()
    ops.lookup.path = lookup_path
    ops.insert = MagicMock()
    ops.insert.path = "/contacts"
    ops.update = MagicMock()
    ops.update.path = "/contacts/${external_id}"
    ops.delete = MagicMock()
    ops.delete.path = "/contacts/${external_id}"

    wb_cfg = MagicMock()
    wb_cfg.protection_level = pl
    wb_cfg.etag_header = etag_header
    wb_cfg.if_match_header = "If-Match"
    wb_cfg.operations = ops

    dtype_cfg = MagicMock()
    dtype_cfg.writeback = wb_cfg

    connection = MagicMock()
    connection.base_url = base_url

    connector = MagicMock()
    connector.name = "testconn"
    connector.connection = connection
    connector.datatypes = {"contacts": dtype_cfg}
    return connector


def _make_transport(
    connectivity_ok: bool = True,
    status_code: int = 200,
    etag_value: str | None = None,
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if etag_value:
        resp.headers = {"ETag": etag_value}
    else:
        resp.headers = {}
    resp.is_success = connectivity_ok

    transport = MagicMock()
    if not connectivity_ok:
        transport._raw_request = AsyncMock(side_effect=Exception("connection refused"))
    else:
        transport._raw_request = AsyncMock(return_value=resp)
    transport.__aenter__ = AsyncMock(return_value=transport)
    transport.__aexit__ = AsyncMock(return_value=False)
    return transport


# ---------------------------------------------------------------------------
# probe_etag_support
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_probe_etag_found():
    transport = _make_transport(etag_value='"abc123"')
    etag_ok, if_match_ok = await probe_etag_support(transport, "/items/${id}")
    assert etag_ok is True
    assert if_match_ok is True


@pytest.mark.anyio
async def test_probe_etag_not_found():
    transport = _make_transport(etag_value=None)
    etag_ok, if_match_ok = await probe_etag_support(transport, "/items/${id}")
    assert etag_ok is False
    assert if_match_ok is False


@pytest.mark.anyio
async def test_probe_etag_exception_returns_false():
    transport = MagicMock()
    transport._raw_request = AsyncMock(side_effect=RuntimeError("boom"))
    etag_ok, if_match_ok = await probe_etag_support(transport, "/items/${id}")
    assert etag_ok is False
    assert if_match_ok is False


@pytest.mark.anyio
async def test_probe_etag_strips_template_vars():
    """Template vars like ${external_id} are replaced before probing."""
    transport = _make_transport(etag_value='"v1"')
    await probe_etag_support(transport, "/items/${external_id}/detail")
    call_path = transport._raw_request.call_args[0][1]
    assert "${external_id}" not in call_path
    assert "probe" in call_path


@pytest.mark.anyio
async def test_probe_custom_etag_header():
    """Respects custom etag_header name."""
    resp = MagicMock()
    resp.headers = {"X-Version": '"v2"'}
    transport = MagicMock()
    transport._raw_request = AsyncMock(return_value=resp)
    etag_ok, _ = await probe_etag_support(transport, "/", etag_header="X-Version")
    assert etag_ok is True


# ---------------------------------------------------------------------------
# validate_writeback_connector — no datatypes
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_no_writeback_datatypes():
    connector = MagicMock()
    connector.name = "empty"
    connector.datatypes = {"leads": MagicMock(writeback=None)}

    result = await validate_writeback_connector(connector)
    assert result.connectivity == "unknown"
    assert result.errors


# ---------------------------------------------------------------------------
# validate_writeback_connector — connectivity failure
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_connectivity_failure():
    connector = _make_connector()

    transport = MagicMock()
    transport._raw_request = AsyncMock(side_effect=Exception("refused"))
    transport.__aenter__ = AsyncMock(return_value=transport)
    transport.__aexit__ = AsyncMock(return_value=False)

    with patch("inandout.writeback.validate.HttpTransportAdapter", return_value=transport):
        result = await validate_writeback_connector(connector)

    assert result.connectivity == "failed"
    assert result.auth == "unknown"
    assert result.errors
    assert len(result.datatypes) == 1  # still one entry, marked with error
    assert not result.datatypes[0].etag_support


# ---------------------------------------------------------------------------
# validate_writeback_connector — success + ETag present
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_ok_with_etag():
    connector = _make_connector(protection_level_name="optimistic")

    transport = _make_transport(connectivity_ok=True, status_code=200, etag_value='"v1"')

    with patch("inandout.writeback.validate.HttpTransportAdapter", return_value=transport):
        result = await validate_writeback_connector(connector)

    assert result.connectivity == "ok"
    assert result.auth == "ok"
    assert len(result.datatypes) == 1
    dt = result.datatypes[0]
    assert dt.etag_support is True
    assert dt.effective_protection_level == "optimistic"
    assert not dt.errors
    assert result.ok is True


# ---------------------------------------------------------------------------
# validate_writeback_connector — ETag missing emits warning
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_optimistic_without_etag_warns():
    connector = _make_connector(protection_level_name="optimistic")

    transport = _make_transport(connectivity_ok=True, status_code=200, etag_value=None)

    with patch("inandout.writeback.validate.HttpTransportAdapter", return_value=transport):
        result = await validate_writeback_connector(connector)

    dt = result.datatypes[0]
    assert dt.etag_support is False
    assert dt.effective_protection_level == "none"
    assert dt.warnings  # Should emit a warning about effective protection


# ---------------------------------------------------------------------------
# validate_writeback_connector — 401 auth failure
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_401_marks_auth_failed():
    connector = _make_connector()

    transport = _make_transport(connectivity_ok=True, status_code=401, etag_value=None)

    with patch("inandout.writeback.validate.HttpTransportAdapter", return_value=transport):
        result = await validate_writeback_connector(connector)

    assert result.connectivity == "ok"
    assert result.auth == "failed"
    assert any("401" in e for e in result.errors)


# ---------------------------------------------------------------------------
# validate_writeback_connector — post_write_verify level
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_post_write_verify_effective_level():
    connector = _make_connector(protection_level_name="post_write_verify")
    transport = _make_transport(connectivity_ok=True, status_code=200, etag_value=None)

    with patch("inandout.writeback.validate.HttpTransportAdapter", return_value=transport):
        result = await validate_writeback_connector(connector)

    dt = result.datatypes[0]
    assert dt.effective_protection_level == "post_write_verify"


# ---------------------------------------------------------------------------
# validate_writeback_connector — specific datatype filter
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_validate_filters_to_requested_datatype():
    connector = _make_connector()
    # Add a second datatype
    connector.datatypes["accounts"] = MagicMock()
    connector.datatypes["accounts"].writeback = MagicMock()
    connector.datatypes["accounts"].writeback.protection_level = MagicMock()
    connector.datatypes["accounts"].writeback.protection_level.name = "none"

    transport = _make_transport(connectivity_ok=True, status_code=200, etag_value='"v1"')

    with patch("inandout.writeback.validate.HttpTransportAdapter", return_value=transport):
        result = await validate_writeback_connector(connector, datatype_names=["contacts"])

    assert len(result.datatypes) == 1
    assert result.datatypes[0].datatype == "contacts"


# ---------------------------------------------------------------------------
# WritebackValidateResult.ok property
# ---------------------------------------------------------------------------


def test_validate_result_ok_true():
    r = WritebackValidateResult(connector="x", connectivity="ok", auth="ok")
    assert r.ok is True


def test_validate_result_ok_false_on_error():
    r = WritebackValidateResult(connector="x", connectivity="ok", auth="ok", errors=["boom"])
    assert r.ok is False


def test_validate_result_ok_false_on_connectivity_fail():
    r = WritebackValidateResult(connector="x", connectivity="failed", auth="unknown")
    assert r.ok is False
