"""Unit tests for check_all_slas – iterates connector configs and delegates to check_sla."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from inandout.observability.sla import check_all_slas


def _make_cfg(
    connector_name: str,
    datatypes: dict,
):
    """Build a minimal connector_file_cfg object expected by check_all_slas."""
    connector = SimpleNamespace(
        name=connector_name,
        datatypes=datatypes,
    )
    return SimpleNamespace(connector=connector)


def _dtype_with_lag(lag: int | None):
    schedule = SimpleNamespace(max_lag_seconds=lag)
    ingestion = SimpleNamespace(schedule=schedule)
    return SimpleNamespace(ingestion=ingestion)


def _dtype_no_ingestion():
    return SimpleNamespace(ingestion=None)


@pytest.fixture
def mock_check_sla():
    with patch(
        "inandout.observability.sla.check_sla",
        new_callable=AsyncMock,
        return_value=False,
    ) as m:
        yield m


async def test_single_connector_single_dtype(mock_check_sla):
    cfg = _make_cfg("crm", {"contacts": _dtype_with_lag(300)})
    result = await check_all_slas(object(), [cfg])
    assert result == {("crm", "contacts"): False}
    mock_check_sla.assert_awaited_once()
    _, connector_arg, dtype_arg, lag_arg = mock_check_sla.await_args.args
    assert connector_arg == "crm"
    assert dtype_arg == "contacts"
    assert lag_arg == 300


async def test_skip_when_ingestion_is_none(mock_check_sla):
    cfg = _make_cfg("crm", {"contacts": _dtype_no_ingestion()})
    result = await check_all_slas(object(), [cfg])
    assert result == {}
    mock_check_sla.assert_not_awaited()


async def test_skip_when_max_lag_seconds_is_none(mock_check_sla):
    cfg = _make_cfg("crm", {"contacts": _dtype_with_lag(None)})
    result = await check_all_slas(object(), [cfg])
    assert result == {}
    mock_check_sla.assert_not_awaited()


async def test_multiple_datatypes_only_lag_configured_checked(mock_check_sla):
    mock_check_sla.side_effect = [False, True]
    cfg = _make_cfg(
        "bi",
        {
            "events": _dtype_with_lag(60),
            "noop": _dtype_no_ingestion(),
            "orders": _dtype_with_lag(120),
        },
    )
    result = await check_all_slas(object(), [cfg])
    assert result == {("bi", "events"): False, ("bi", "orders"): True}
    assert mock_check_sla.await_count == 2


async def test_multiple_connectors(mock_check_sla):
    mock_check_sla.side_effect = [False, False, True]
    cfgs = [
        _make_cfg("a", {"d1": _dtype_with_lag(10)}),
        _make_cfg("b", {"d2": _dtype_with_lag(20), "d3": _dtype_with_lag(30)}),
    ]
    result = await check_all_slas(object(), cfgs)
    assert result == {("a", "d1"): False, ("b", "d2"): False, ("b", "d3"): True}
    assert mock_check_sla.await_count == 3


async def test_empty_connector_list(mock_check_sla):
    result = await check_all_slas(object(), [])
    assert result == {}
    mock_check_sla.assert_not_awaited()


async def test_violated_true_propagated(mock_check_sla):
    mock_check_sla.return_value = True
    cfg = _make_cfg("feed", {"items": _dtype_with_lag(3600)})
    result = await check_all_slas(object(), [cfg])
    assert result[("feed", "items")] is True
