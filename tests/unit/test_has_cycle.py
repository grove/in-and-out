"""Unit tests for _has_cycle helper in config/connector.py."""
from __future__ import annotations

from inandout.config.connector import _has_cycle


def test_empty_graph_no_cycle():
    assert _has_cycle({}) is False


def test_single_node_no_self_loop():
    assert _has_cycle({"a": []}) is False


def test_single_node_self_loop():
    assert _has_cycle({"a": ["a"]}) is True


def test_two_nodes_no_cycle():
    assert _has_cycle({"a": ["b"], "b": []}) is False


def test_two_nodes_cycle():
    assert _has_cycle({"a": ["b"], "b": ["a"]}) is True


def test_three_nodes_linear_no_cycle():
    graph = {"a": ["b"], "b": ["c"], "c": []}
    assert _has_cycle(graph) is False


def test_three_nodes_cycle():
    graph = {"a": ["b"], "b": ["c"], "c": ["a"]}
    assert _has_cycle(graph) is True


def test_disconnected_nodes_no_cycle():
    graph = {"a": ["b"], "b": [], "c": ["d"], "d": []}
    assert _has_cycle(graph) is False


def test_disconnected_with_cycle():
    graph = {"a": ["b"], "b": [], "c": ["d"], "d": ["c"]}
    assert _has_cycle(graph) is True


def test_diamond_no_cycle():
    # a → b, a → c, b → d, c → d
    graph = {"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": []}
    assert _has_cycle(graph) is False


def test_complex_cycle():
    graph = {"a": ["b", "c"], "b": ["d"], "c": ["d"], "d": ["a"]}
    assert _has_cycle(graph) is True


def test_node_referencing_missing_node():
    # Reference to a node not in graph — should still not raise
    graph = {"a": ["b"]}
    result = _has_cycle(graph)
    assert isinstance(result, bool)
