"""Unit tests for _load_transform_function in deadletter/transform.py."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from inandout.deadletter.transform import _load_transform_function


def _write_script(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "transform_script.py"
    p.write_text(textwrap.dedent(content))
    return p


def test_loads_transform_function(tmp_path: Path):
    script = _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            return record
        """,
    )
    fn = _load_transform_function(script)
    assert fn is not None
    assert callable(fn)


def test_loaded_function_is_named_transform(tmp_path: Path):
    script = _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            return record
        """,
    )
    fn = _load_transform_function(script)
    assert fn.__name__ == "transform"


async def test_loaded_function_is_callable_and_async(tmp_path: Path):
    script = _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            return {"patched": True, **record}
        """,
    )
    fn = _load_transform_function(script)
    result = await fn({"id": "1"})
    assert result == {"patched": True, "id": "1"}


def test_raises_attribute_error_when_no_transform(tmp_path: Path):
    script = _write_script(
        tmp_path,
        """\
        async def process(record: dict):
            return record
        """,
    )
    with pytest.raises(AttributeError, match="transform"):
        _load_transform_function(script)


async def test_transform_returning_none(tmp_path: Path):
    """transform() that returns None should be loadable and return None."""
    script = _write_script(
        tmp_path,
        """\
        async def transform(record: dict):
            return None
        """,
    )
    fn = _load_transform_function(script)
    result = await fn({"x": 1})
    assert result is None


def test_script_with_imports_loads_correctly(tmp_path: Path):
    script = _write_script(
        tmp_path,
        """\
        import json

        async def transform(record: dict):
            return json.loads(json.dumps(record))
        """,
    )
    fn = _load_transform_function(script)
    assert callable(fn)
