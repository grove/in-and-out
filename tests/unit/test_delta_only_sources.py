"""Unit tests for delta-only source protection (T1 #14)."""
from __future__ import annotations

import pytest

from inandout.config.ingestion import IngestionConfig


def test_delta_only_defaults_to_false():
    """delta_only should default to False."""
    cfg = IngestionConfig(
        primary_key="id",
        history_mode="overwrite",
        schedule={"interval": "5m"},
        list={
            "method": "GET",
            "path": "/records",
            "record_selector": "items",
            "pagination": {"strategy": "offset", "offset": {"page_size": 100}},
        },
    )
    assert cfg.delta_only is False


def test_delta_only_can_be_enabled():
    """delta_only flag should be configurable."""
    cfg = IngestionConfig(
        primary_key="id",
        history_mode="overwrite",
        schedule={"interval": "5m"},
        delta_only=True,
        list={
            "method": "GET",
            "path": "/changes",
            "record_selector": "deltas",
            "pagination": {"strategy": "cursor", "cursor": {"request_param": "cursor", "response_path": "next"}},
        },
    )
    assert cfg.delta_only is True
