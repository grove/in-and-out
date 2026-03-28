"""Unit tests for declarative field/property selection (A4)."""
from __future__ import annotations

from inandout.config.ingestion import ListConfig


def _make_list_config(**kwargs) -> ListConfig:
    defaults = {
        "path": "/contacts",
        "pagination": {"strategy": "offset", "offset": {"page_size": 50}},
    }
    defaults.update(kwargs)
    return ListConfig(**defaults)


def test_properties_default_empty() -> None:
    cfg = _make_list_config()
    assert cfg.properties == []
    assert cfg.properties_param == "properties"
    assert cfg.properties_format == "comma"


def test_properties_comma_format() -> None:
    cfg = _make_list_config(
        properties=["id", "email", "name"],
        properties_format="comma",
    )
    assert cfg.properties == ["id", "email", "name"]
    assert cfg.properties_format == "comma"


def test_properties_array_format() -> None:
    cfg = _make_list_config(
        properties=["id", "email"],
        properties_format="array",
    )
    assert cfg.properties_format == "array"


def test_properties_json_array_format() -> None:
    cfg = _make_list_config(
        properties=["id", "email"],
        properties_format="json_array",
    )
    assert cfg.properties_format == "json_array"


def test_exclusion_pattern_preserved_in_list() -> None:
    """!-prefixed patterns should be stored as-is in the list."""
    cfg = _make_list_config(
        properties=["id", "email", "!*.internal"],
    )
    assert "!*.internal" in cfg.properties
