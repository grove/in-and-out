"""T1 #47: Composite connector health scoring.

Computes a rolling success-rate score (0.0–1.0) from the last N completed sync
runs stored in ``inout_ops_sync_run``.  A score of 1.0 means all recent runs
succeeded; 0.0 means all recent runs failed.

The score is intentionally a float so Prometheus alert thresholds can be tuned
(e.g., alert when score < 0.8 rather than only at 0.0).
"""
from __future__ import annotations

HEALTH_SCORE_WINDOW = 10  # number of recent completed runs to consider


async def compute_health_score(
    pool: object,
    connector: str,
    datatype: str,
    window: int = HEALTH_SCORE_WINDOW,
) -> float:
    """Return a rolling health score in [0.0, 1.0].

    Queries the last *window* completed (non-running) sync runs for the given
    connector/datatype and returns the fraction that have status == 'completed'.
    Falls back to 1.0 (optimistic) when the table is unavailable or no runs
    exist yet.

    Parameters
    ----------
    pool:
        An ``AsyncConnectionPool`` (psycopg3).
    connector:
        Connector name.
    datatype:
        Datatype name.
    window:
        How many recent runs to look at.
    """
    try:
        async with pool.connection() as conn:  # type: ignore[attr-defined]
            rows = await (await conn.execute(
                """
                SELECT status
                FROM inout_ops_sync_run
                WHERE connector = %s
                  AND datatype = %s
                  AND status <> 'running'
                ORDER BY finished_at DESC NULLS LAST
                LIMIT %s
                """,
                [connector, datatype, window],
            )).fetchall()

        if not rows:
            return 1.0  # No history → assume healthy

        total = len(rows)
        successes = sum(1 for (status,) in rows if status == "completed")
        return round(successes / total, 4)
    except Exception:
        return 1.0  # On DB error, return optimistic value rather than flapping alerts
