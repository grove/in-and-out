"""Unit tests for connector dependency graph (Step 66)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest


@dataclass
class FakeConnectorConfig:
    name: str
    depends_on: list[str] = field(default_factory=list)


@dataclass
class FakeConnectorFileConfig:
    connector: FakeConnectorConfig


def _make_cfgs(deps: dict[str, list[str]]) -> list[FakeConnectorFileConfig]:
    return [
        FakeConnectorFileConfig(FakeConnectorConfig(name=name, depends_on=dep_list))
        for name, dep_list in deps.items()
    ]


# ---------------------------------------------------------------------------
# Linear chain A → B → C
# ---------------------------------------------------------------------------

def test_topological_sort_linear():
    from inandout.config.dependency_graph import topological_sort

    cfgs = _make_cfgs({"c": ["b"], "b": ["a"], "a": []})
    sorted_cfgs = topological_sort(cfgs)
    names = [c.connector.name for c in sorted_cfgs]
    assert names.index("a") < names.index("b")
    assert names.index("b") < names.index("c")


# ---------------------------------------------------------------------------
# Diamond: a → b, a → c; d → b, d → c
# ---------------------------------------------------------------------------

def test_topological_sort_diamond():
    from inandout.config.dependency_graph import topological_sort

    cfgs = _make_cfgs({"b": ["a"], "c": ["a"], "d": ["b", "c"], "a": []})
    sorted_cfgs = topological_sort(cfgs)
    names = [c.connector.name for c in sorted_cfgs]
    assert names.index("a") < names.index("b")
    assert names.index("a") < names.index("c")
    assert names.index("b") < names.index("d")
    assert names.index("c") < names.index("d")


# ---------------------------------------------------------------------------
# Circular dependency → ValueError with cycle in message
# ---------------------------------------------------------------------------

def test_topological_sort_circular_raises():
    from inandout.config.dependency_graph import topological_sort

    cfgs = _make_cfgs({"a": ["b"], "b": ["c"], "c": ["a"]})
    with pytest.raises(ValueError, match="[Cc]ircular"):
        topological_sort(cfgs)


def test_topological_sort_circular_error_mentions_nodes():
    from inandout.config.dependency_graph import topological_sort

    cfgs = _make_cfgs({"x": ["y"], "y": ["x"], "z": []})
    with pytest.raises(ValueError) as exc_info:
        topological_sort(cfgs)
    msg = str(exc_info.value)
    assert "x" in msg or "y" in msg


# ---------------------------------------------------------------------------
# No dependencies → alphabetical order
# ---------------------------------------------------------------------------

def test_topological_sort_no_deps_alphabetical():
    from inandout.config.dependency_graph import topological_sort

    cfgs = _make_cfgs({"charlie": [], "alpha": [], "beta": []})
    sorted_cfgs = topological_sort(cfgs)
    names = [c.connector.name for c in sorted_cfgs]
    assert names == sorted(names)


# ---------------------------------------------------------------------------
# Empty list
# ---------------------------------------------------------------------------

def test_topological_sort_empty():
    from inandout.config.dependency_graph import topological_sort

    result = topological_sort([])
    assert result == []


# ---------------------------------------------------------------------------
# ConnectorConfig field
# ---------------------------------------------------------------------------

def test_connector_config_depends_on_default():
    """depends_on should default to an empty list."""
    from inandout.config.connector import ConnectorConfig

    # We can't easily build a full ConnectorConfig without all fields,
    # so just verify the field exists with its default
    import inspect
    fields = ConnectorConfig.model_fields
    assert "depends_on" in fields
    assert fields["depends_on"].default == []
