"""Connector YAML schema migration registry and utilities."""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Callable


@dataclass
class ConnectorMigration:
    """Describes a single connector schema migration step."""

    from_version: str
    to_version: str
    description: str
    migrate: Callable[[dict], dict]


# ---------------------------------------------------------------------------
# Migration implementations
# ---------------------------------------------------------------------------


def _migrate_v1_0_to_v1_1(data: dict) -> dict:
    """Rename webhook.signature_header → webhook.signature.header."""
    from inandout.migrations.connector_migrations.v1_0_to_v1_1 import migrate
    return migrate(data)


# ---------------------------------------------------------------------------
# Ordered migration registry
# ---------------------------------------------------------------------------

MIGRATIONS: list[ConnectorMigration] = [
    ConnectorMigration(
        from_version="1.0",
        to_version="1.1",
        description="Rename webhook.signature_header to webhook.signature.header",
        migrate=_migrate_v1_0_to_v1_1,
    ),
]


# ---------------------------------------------------------------------------
# Path finding and application
# ---------------------------------------------------------------------------


def find_migration_path(from_ver: str, to_ver: str) -> list[ConnectorMigration]:
    """Find the ordered list of migrations to apply to go from from_ver to to_ver.

    Args:
        from_ver: Source version string (e.g. "1.0").
        to_ver: Target version string (e.g. "1.1").

    Returns:
        Ordered list of ConnectorMigration steps to apply.

    Raises:
        ValueError: If no migration path can be found.
    """
    if from_ver == to_ver:
        return []

    # Build a simple chain: try to traverse MIGRATIONS in order
    path: list[ConnectorMigration] = []
    current = from_ver

    remaining = list(MIGRATIONS)
    max_steps = len(remaining) + 1

    for _ in range(max_steps):
        if current == to_ver:
            return path

        # Find the next migration step from current version
        found = None
        for m in remaining:
            if m.from_version == current:
                found = m
                break

        if found is None:
            # No path found
            available = ", ".join(f"{m.from_version}→{m.to_version}" for m in MIGRATIONS)
            raise ValueError(
                f"No migration path from {from_ver!r} to {to_ver!r}. "
                f"Available migrations: {available}"
            )

        path.append(found)
        remaining.remove(found)
        current = found.to_version

    raise ValueError(
        f"Could not find migration path from {from_ver!r} to {to_ver!r} "
        f"within {max_steps} steps."
    )


def apply_migrations(yaml_dict: dict, migrations: list[ConnectorMigration]) -> dict:
    """Apply a list of migrations in order to a raw YAML dict.

    Args:
        yaml_dict: Raw parsed YAML as a Python dict.
        migrations: Ordered list of ConnectorMigration objects to apply.

    Returns:
        Modified dict after all migrations are applied.
    """
    result = copy.deepcopy(yaml_dict)
    for migration in migrations:
        result = migration.migrate(result)
    return result
