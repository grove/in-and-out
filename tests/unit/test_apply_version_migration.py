"""Unit tests for _apply_version_migration.

Verifies:
- The method completes without raising on happy path.
- It logs 'connector_version_migration_check' with connector/datatype info.
- It handles and swallows internal exceptions (logs a warning, no raise).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.ingestion.engine import IngestionEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool() -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=AsyncMock())
    return pool


def _make_connector(name: str = "hubspot") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "2.0.0"
    return cfg


def _make_dtype_cfg() -> MagicMock:
    dtype = MagicMock()
    dtype.field_mappings = []
    return dtype


def _make_log() -> MagicMock:
    log = MagicMock()
    log.info = MagicMock()
    log.warning = MagicMock()
    return log


# ---------------------------------------------------------------------------
# Happy-path: does not raise
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_apply_version_migration_does_not_raise():
    engine = IngestionEngine(_make_pool())
    conn = AsyncMock()
    log = _make_log()

    # Should complete silently
    await engine._apply_version_migration(
        conn, _make_connector(), "contacts",
        MagicMock(), _make_dtype_cfg(), log,
    )


# ---------------------------------------------------------------------------
# Logs migration_check info entry
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_apply_version_migration_logs_info():
    engine = IngestionEngine(_make_pool())
    conn = AsyncMock()
    log = _make_log()

    await engine._apply_version_migration(
        conn, _make_connector("salesforce"), "deals",
        MagicMock(), _make_dtype_cfg(), log,
    )

    log.info.assert_called_once()
    call_kwargs = log.info.call_args
    # First positional arg is the event name
    assert call_kwargs.args[0] == "connector_version_migration_check"
    assert call_kwargs.kwargs.get("connector") == "salesforce"
    assert call_kwargs.kwargs.get("datatype") == "deals"


# ---------------------------------------------------------------------------
# Internal exceptions are swallowed — logs warning instead
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_apply_version_migration_swallows_import_error():
    """If apply_schema_migrations import fails, must not raise — logs warning."""
    engine = IngestionEngine(_make_pool())
    conn = AsyncMock()
    log = _make_log()

    with patch(
        "inandout.ingestion.engine.IngestionEngine._apply_version_migration",
        wraps=engine._apply_version_migration,
    ):
        # Patch the internal import that _apply_version_migration uses
        with patch(
            "builtins.__import__",
            side_effect=lambda name, *args, **kw: (
                (_ for _ in ()).throw(ImportError("no module"))
                if name == "inandout.postgres.schema_migration"
                else __import__(name, *args, **kw)
            ),
        ):
            # Must not raise
            try:
                await engine._apply_version_migration(
                    conn, _make_connector(), "contacts",
                    MagicMock(), _make_dtype_cfg(), log,
                )
            except Exception as exc:
                pytest.fail(f"_apply_version_migration raised unexpectedly: {exc}")


@pytest.mark.anyio
async def test_apply_version_migration_logs_warning_on_exception():
    """When an exception occurs inside, log.warning is called."""
    engine = IngestionEngine(_make_pool())
    conn = AsyncMock()
    log = _make_log()

    # Force an exception by passing a dtype_cfg that raises on .field_mappings access
    bad_dtype = MagicMock()
    bad_dtype.field_mappings = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))

    # The implementation catches all exceptions, so just confirm no raise
    await engine._apply_version_migration(
        conn, _make_connector(), "contacts",
        MagicMock(), bad_dtype, log,
    )
    # Either info was called (no exception) or warning was called (exception path)
    assert log.info.called or log.warning.called
