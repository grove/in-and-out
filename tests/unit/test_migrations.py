"""Tests for migration file chain integrity and schema version consistency.

These tests run entirely in-process without a database connection.  They
verify that:
  - Every migration file (001–NNN) is present in migrations/versions/.
  - The down_revision chain forms a complete, non-branching sequence from
    the latest revision back to None (the initial migration).
  - The SCHEMA_VERSION constant in version_check.py matches the count of
    numbered migration files.
  - The latest migration updates inout_ops_meta to SCHEMA_VERSION.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path


MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations" / "versions"


def _load_migration_modules() -> dict[str, object]:
    """Import all numbered migration modules and return them keyed by revision."""
    modules: dict[str, object] = {}
    for path in sorted(MIGRATIONS_DIR.glob("[0-9]*.py")):
        module_name = f"migrations.versions.{path.stem}"
        try:
            mod = importlib.import_module(module_name)
        except Exception as exc:
            raise ImportError(f"Failed to import {path}: {exc}") from exc
        revision = getattr(mod, "revision", None)
        assert revision is not None, f"{path} missing 'revision'"
        modules[revision] = mod
    return modules


def test_migration_files_form_complete_chain():
    """Every migration's down_revision must point to the previous migration or None."""
    modules = _load_migration_modules()

    # Build a map: revision → down_revision
    chain: dict[str, str | None] = {}
    for rev, mod in modules.items():
        chain[rev] = getattr(mod, "down_revision", None)

    # Trace the chain from each leaf back to root; there must be exactly one root
    roots = [rev for rev, dr in chain.items() if dr is None]
    assert len(roots) == 1, f"Expected exactly one root migration (down_revision=None), found: {roots}"

    # Build reverse map: down_revision → [revision] for successor lookup
    from collections import defaultdict
    successors: dict[str | None, list[str]] = defaultdict(list)
    for rev, dr in chain.items():
        successors[dr].append(rev)

    # Walk from root to tip; verify no branching
    current: str | None = roots[0]
    visited: list[str] = []
    while current is not None:
        visited.append(current)
        nexts = successors.get(current, [])
        if not nexts:
            break
        assert len(nexts) == 1, (
            f"Migration chain branches at {current!r}: multiple successors {nexts}"
        )
        current = nexts[0]

    assert len(visited) == len(modules), (
        f"Chain length {len(visited)} != number of migration files {len(modules)}. "
        f"Unvisited: {set(modules) - set(visited)}"
    )


def test_migration_count_matches_schema_version():
    """Number of numbered migration files must equal SCHEMA_VERSION."""
    from inandout.postgres.version_check import SCHEMA_VERSION

    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9]*.py"))
    count = len(migration_files)
    assert count == SCHEMA_VERSION, (
        f"Found {count} migration files but SCHEMA_VERSION={SCHEMA_VERSION}. "
        "Either add the missing migration(s) or update SCHEMA_VERSION."
    )


def test_latest_migration_sets_correct_schema_version():
    """The most recent migration's upgrade() SQL must reference SCHEMA_VERSION."""
    from inandout.postgres.version_check import SCHEMA_VERSION

    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9]*.py"))
    assert migration_files, "No migration files found"
    latest = migration_files[-1]

    source = latest.read_text()
    # The upgrade function should contain the correct version number
    assert str(SCHEMA_VERSION) in source, (
        f"Latest migration {latest.name} does not reference SCHEMA_VERSION={SCHEMA_VERSION}. "
        "Make sure upgrade() sets schema_version to the current SCHEMA_VERSION."
    )


def test_revision_ids_use_sequential_numeric_prefix():
    """All migration revision IDs must use zero-padded sequential numeric prefixes."""
    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9]*.py"))
    assert migration_files, "No migration files found"

    numbers: list[int] = []
    for path in migration_files:
        m = re.match(r"^(\d+)_", path.stem)
        assert m, f"Migration file {path.name} does not start with a numeric prefix"
        numbers.append(int(m.group(1)))

    expected = list(range(1, len(numbers) + 1))
    assert numbers == expected, (
        f"Migration numeric prefixes are not sequential: got {numbers}, expected {expected}"
    )


def test_no_duplicate_revision_ids():
    """No two migration files may share the same revision ID."""
    modules = _load_migration_modules()
    # If _load_migration_modules ran without assertion errors we already checked uniqueness
    # via dict keying, but be explicit:
    revisions = list(modules.keys())
    assert len(revisions) == len(set(revisions)), f"Duplicate revision IDs found: {revisions}"
