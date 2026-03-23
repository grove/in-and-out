"""Smoke test: package is importable and version is set."""

import inandout


def test_version_is_set() -> None:
    assert inandout.__version__ == "0.1.0"
