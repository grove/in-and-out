"""Connector health scoring: composite metric from error rate, dead-letter, and circuit breaker."""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


async def compute_health_score(
    pool: Any,
    connector: str,
    datatype: str,
    window_secs: int = 3600,
) -> float:
    """Compute a health score in [0.0, 1.0] for a connector/datatype pair.

    Components (weights):
    - circuit_breaker (0.4): CLOSED=1.0, HALF_OPEN=0.5, OPEN=0.0
    - error_rate      (0.4): 1.0 - (failed_runs / total_runs); 1.0 if no runs
    - dead_letter     (0.2): max(0, 1 - dl_depth/100); 0.0 if DL table has ≥100 rows

    Returns a float in [0.0, 1.0].
    """
    # --- Circuit breaker score ---
    try:
        from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
        cb = get_circuit_breaker(connector, datatype)
        state = cb.state
        if state == CircuitState.open:
            cb_score = 0.0
        elif state == CircuitState.half_open:
            cb_score = 0.5
        else:
            cb_score = 1.0
    except Exception:
        cb_score = 1.0

    # --- Error rate score ---
    error_rate = 0.0
    try:
        async with pool.connection() as conn:
            row = await (await conn.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
                    COUNT(*) AS total_runs
                FROM inout_ops_sync_run
                WHERE connector = %s AND datatype = %s
                  AND started_at >= NOW() - INTERVAL '1 second' * %s
                """,
                [connector, datatype, window_secs],
            )).fetchone()
        if row and row[1] and row[1] > 0:
            error_rate = row[0] / row[1]
    except Exception:
        pass

    # --- Dead-letter depth score ---
    dl_depth = 0
    try:
        from inandout.postgres.schema import dead_letter_table_name
        dl_table = dead_letter_table_name("ingestion", connector, datatype)
        async with pool.connection() as conn:
            dl_row = await (await conn.execute(
                f"SELECT COUNT(*) FROM {dl_table}"
            )).fetchone()
        if dl_row:
            dl_depth = int(dl_row[0])
    except Exception:
        dl_depth = 0

    dl_score = max(0.0, 1.0 - (dl_depth / 100))

    # --- Composite score ---
    score = max(0.0, (cb_score * 0.4) + ((1.0 - error_rate) * 0.4) + (dl_score * 0.2))

    # Emit Prometheus gauge
    try:
        from inandout.observability.metrics import connector_health_score
        connector_health_score.labels(connector=connector, datatype=datatype).set(score)
    except Exception:
        pass

    log = logger.bind(connector=connector, datatype=datatype)
    log.debug(
        "health_score_computed",
        score=round(score, 4),
        cb_score=cb_score,
        error_rate=round(error_rate, 4),
        dl_depth=dl_depth,
        dl_score=round(dl_score, 4),
    )

    return score


def health_components(
    cb_score: float,
    error_rate: float,
    dl_depth: int,
) -> dict[str, float]:
    """Return the breakdown dict for the health API response."""
    dl_score = max(0.0, 1.0 - (dl_depth / 100))
    return {
        "circuit_breaker": cb_score,
        "error_rate": round(1.0 - error_rate, 4),
        "dead_letter": round(dl_score, 4),
    }
