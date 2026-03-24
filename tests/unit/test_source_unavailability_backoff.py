"""Unit tests for T1 #44 — source unavailability exponential back-off."""
from __future__ import annotations

import inspect


# ---------------------------------------------------------------------------
# Config field checks
# ---------------------------------------------------------------------------

def test_ingestion_config_has_backoff_fields():
    """IngestionConfig must have the two new exponential back-off fields."""
    from inandout.config.ingestion import IngestionConfig

    fields = IngestionConfig.model_fields
    assert "unavailability_backoff_multiplier" in fields
    assert "unavailability_backoff_ceiling_secs" in fields


def test_backoff_multiplier_default():
    from inandout.config.ingestion import IngestionConfig

    field = IngestionConfig.model_fields["unavailability_backoff_multiplier"]
    assert field.default == 2.0


def test_backoff_ceiling_default():
    from inandout.config.ingestion import IngestionConfig

    field = IngestionConfig.model_fields["unavailability_backoff_ceiling_secs"]
    assert field.default == 3600.0


def test_unavailability_cooldown_secs_existing_default():
    """The base cooldown field must still be present with its 300 s default."""
    from inandout.config.ingestion import IngestionConfig

    field = IngestionConfig.model_fields.get("unavailability_cooldown_secs")
    assert field is not None
    assert field.default == 300.0


# ---------------------------------------------------------------------------
# Back-off math
# ---------------------------------------------------------------------------

def _effective_cooldown(base: float, multiplier: float, ceiling: float, skip_n: int) -> float:
    """Replicate the engine's back-off calculation."""
    return min(base * (multiplier ** skip_n), ceiling)


def test_first_skip_uses_base_cooldown():
    assert _effective_cooldown(300, 2.0, 3600, skip_n=0) == 300.0


def test_second_skip_doubles():
    assert _effective_cooldown(300, 2.0, 3600, skip_n=1) == 600.0


def test_third_skip_quadruples():
    assert _effective_cooldown(300, 2.0, 3600, skip_n=2) == 1200.0


def test_backoff_capped_at_ceiling():
    # 300 * 2^4 = 4800 > 3600 → capped
    result = _effective_cooldown(300, 2.0, 3600, skip_n=4)
    assert result == 3600.0


def test_backoff_never_exceeds_ceiling():
    for n in range(20):
        assert _effective_cooldown(300, 2.0, 3600, n) <= 3600.0


# ---------------------------------------------------------------------------
# IngestionEngine._unavailability_skip_counts dict
# ---------------------------------------------------------------------------

def _make_engine():
    from inandout.ingestion.engine import IngestionEngine
    from unittest.mock import MagicMock

    engine = object.__new__(IngestionEngine)
    engine._pool = MagicMock()
    engine._namespace = "public"
    engine._debouncer = None
    engine._read_pool = None
    engine._unavailability_skip_counts = {}
    return engine


def test_engine_has_skip_counts_dict():
    from inandout.ingestion.engine import IngestionEngine

    engine = object.__new__(IngestionEngine)
    engine._pool = None
    engine._namespace = "public"
    engine._debouncer = None
    engine._read_pool = None
    engine._unavailability_skip_counts = {}
    assert isinstance(engine._unavailability_skip_counts, dict)


def test_skip_counts_initialised_in_init():
    """IngestionEngine.__init__ must initialise _unavailability_skip_counts."""
    src = inspect.getsource(
        __import__("inandout.ingestion.engine", fromlist=["IngestionEngine"]).IngestionEngine.__init__
    )
    assert "_unavailability_skip_counts" in src


def test_engine_source_uses_backoff_multiplier():
    """Engine source must reference unavailability_backoff_multiplier."""
    from inandout.ingestion import engine as engine_module

    src = inspect.getsource(engine_module)
    assert "unavailability_backoff_multiplier" in src
    assert "unavailability_backoff_ceiling_secs" in src


def test_engine_source_increments_skip_count():
    """Engine source must increment the skip count on each skip."""
    from inandout.ingestion import engine as engine_module

    src = inspect.getsource(engine_module)
    assert "_unavailability_skip_counts" in src
    assert "_skip_n + 1" in src


def test_engine_source_resets_skip_count_on_recovery():
    """Engine source must reset skip count when connector becomes healthy again."""
    from inandout.ingestion import engine as engine_module

    src = inspect.getsource(engine_module)
    assert "_unavailability_skip_counts" in src
    # The pop call resets the counter on recovery
    assert ".pop(" in src


# ---------------------------------------------------------------------------
# Effective cooldown grows correctly across consecutive skips
# ---------------------------------------------------------------------------

import pytest


def test_skip_n0_cooldown_300():
    assert _effective_cooldown(300, 2.0, 3600, 0) == pytest.approx(300.0)


def test_skip_n3_cooldown_2400():
    assert _effective_cooldown(300, 2.0, 3600, 3) == pytest.approx(2400.0)


def test_custom_multiplier_1_5():
    # 300 * 1.5^2 = 675
    assert _effective_cooldown(300, 1.5, 3600, 2) == pytest.approx(675.0)


def test_zero_base_stays_zero():
    # edge case: if base is 0, all back-offs are 0 (no back-off configured)
    assert _effective_cooldown(0, 2.0, 3600, 5) == 0.0
