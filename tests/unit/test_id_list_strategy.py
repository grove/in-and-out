"""Unit tests for ID-list fetch strategy (A2)."""
from __future__ import annotations

from inandout.config.ingestion import ListConfig


def _make_list_config(**kwargs) -> ListConfig:
    defaults = {
        "path": "/items",
        "pagination": {"strategy": "offset", "offset": {"page_size": 100}},
    }
    defaults.update(kwargs)
    return ListConfig(**defaults)


def test_default_fetch_strategy_is_list() -> None:
    cfg = _make_list_config()
    assert cfg.fetch_strategy == "list"


def test_id_list_strategy_fields() -> None:
    cfg = _make_list_config(
        fetch_strategy="id_list",
        id_field="uid",
        detail_concurrency=10,
        detail_path="/items/${id}",
    )
    assert cfg.fetch_strategy == "id_list"
    assert cfg.id_field == "uid"
    assert cfg.detail_concurrency == 10


def test_id_field_default() -> None:
    cfg = _make_list_config(fetch_strategy="id_list")
    assert cfg.id_field == "id"
    assert cfg.detail_concurrency == 5
