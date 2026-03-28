"""Unit tests for IpAllowlistMiddleware._is_allowed in ingestion/webhook_server.py."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inandout.ingestion.webhook_server import IpAllowlistMiddleware


def _make_mw(allowlist: list[str]) -> IpAllowlistMiddleware:
    return IpAllowlistMiddleware(MagicMock(), allowlist=allowlist)


# --- Empty allowlist ---

def test_empty_allowlist_allows_any_ip():
    mw = _make_mw([])
    assert mw._is_allowed("1.2.3.4") is True


def test_empty_allowlist_allows_ipv6():
    mw = _make_mw([])
    assert mw._is_allowed("::1") is True


# --- Exact IP match ---

def test_exact_ip_in_allowlist_allowed():
    mw = _make_mw(["192.168.1.10"])
    assert mw._is_allowed("192.168.1.10") is True


def test_exact_ip_not_in_allowlist_blocked():
    mw = _make_mw(["192.168.1.10"])
    assert mw._is_allowed("192.168.1.11") is False


# --- CIDR notation ---

def test_cidr_ip_in_range_allowed():
    mw = _make_mw(["10.0.0.0/24"])
    assert mw._is_allowed("10.0.0.100") is True


def test_cidr_ip_outside_range_blocked():
    mw = _make_mw(["10.0.0.0/24"])
    assert mw._is_allowed("10.0.1.1") is False


def test_cidr_32_is_single_ip():
    mw = _make_mw(["172.16.0.5/32"])
    assert mw._is_allowed("172.16.0.5") is True
    assert mw._is_allowed("172.16.0.6") is False


def test_multiple_entries_any_match_allowed():
    mw = _make_mw(["10.0.0.0/24", "192.168.1.0/24"])
    assert mw._is_allowed("10.0.0.50") is True
    assert mw._is_allowed("192.168.1.200") is True


def test_multiple_entries_no_match_blocked():
    mw = _make_mw(["10.0.0.0/24", "192.168.1.0/24"])
    assert mw._is_allowed("8.8.8.8") is False


# --- IPv6 ---

def test_ipv6_loopback_allowed_in_v6_list():
    mw = _make_mw(["::1"])
    assert mw._is_allowed("::1") is True


def test_ipv6_not_in_list_blocked():
    mw = _make_mw(["::1"])
    assert mw._is_allowed("::2") is False


# --- Invalid IP input ---

def test_invalid_ip_string_returns_false():
    mw = _make_mw(["10.0.0.0/24"])
    assert mw._is_allowed("not-an-ip") is False
