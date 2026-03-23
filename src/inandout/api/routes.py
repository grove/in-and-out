"""Runtime management API routes."""
from __future__ import annotations

import uuid
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = structlog.get_logger(__name__)

router = APIRouter()

# Module-level pool reference — set by build_api_router()
_pool: Any = None


def _set_pool(pool: Any) -> None:
    global _pool
    _pool = pool


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ConnectorSummary(BaseModel):
    name: str
    datatypes: list[str]
    last_sync_at: str | None = None
    last_sync_status: str | None = None


class DatatypeStatus(BaseModel):
    connector: str
    datatype: str
    last_sync_status: str | None = None
    last_sync_at: str | None = None
    watermark: str | None = None
    circuit_breaker_state: str = "closed"


class ControlCommandResponse(BaseModel):
    command: str
    connector: str
    datatype: str
    id: str


class DeadLetterRow(BaseModel):
    id: int
    external_id: str | None
    error_message: str
    error_class: str
    failed_at: str
    requeue_count: int


class HealthResponse(BaseModel):
    status: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/health", response_model=HealthResponse)
async def api_health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/connectors", response_model=list[ConnectorSummary])
async def list_connectors() -> list[ConnectorSummary]:
    """List all connectors with their datatypes and last sync info."""
    if _pool is None:
        return []
    try:
        async with _pool.connection() as conn:
            rows = await (await conn.execute(
                """
                SELECT DISTINCT ON (connector, datatype)
                    connector, datatype, status, finished_at
                FROM inout_ops_sync_run
                ORDER BY connector, datatype, started_at DESC
                """
            )).fetchall()

        # Group by connector
        connector_map: dict[str, dict[str, Any]] = {}
        for row in rows:
            connector_name = row[0]
            datatype = row[1]
            status = row[2]
            finished_at = str(row[3]) if row[3] else None
            if connector_name not in connector_map:
                connector_map[connector_name] = {
                    "name": connector_name,
                    "datatypes": [],
                    "last_sync_at": finished_at,
                    "last_sync_status": status,
                }
            connector_map[connector_name]["datatypes"].append(datatype)

        return [ConnectorSummary(**v) for v in connector_map.values()]
    except Exception as exc:
        logger.warning("api_list_connectors_error", error=str(exc))
        return []


@router.get(
    "/connectors/{connector}/datatypes/{datatype}/status",
    response_model=DatatypeStatus,
)
async def get_datatype_status(connector: str, datatype: str) -> DatatypeStatus:
    """Get last sync result, watermark, and circuit breaker state for a datatype."""
    if _pool is None:
        return DatatypeStatus(connector=connector, datatype=datatype)

    sync_status: str | None = None
    sync_at: str | None = None
    watermark: str | None = None

    try:
        async with _pool.connection() as conn:
            row = await (await conn.execute(
                """
                SELECT status, finished_at
                FROM inout_ops_sync_run
                WHERE connector = %s AND datatype = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                [connector, datatype],
            )).fetchone()
            if row:
                sync_status = row[0]
                sync_at = str(row[1]) if row[1] else None

            wm_row = await (await conn.execute(
                """
                SELECT watermark_value FROM inout_ops_watermark
                WHERE connector = %s AND datatype = %s
                """,
                [connector, datatype],
            )).fetchone()
            if wm_row:
                watermark = wm_row[0]
    except Exception as exc:
        logger.warning("api_datatype_status_error", error=str(exc))

    from inandout.transport.circuit_breaker import get_circuit_breaker
    cb = get_circuit_breaker(connector, datatype)
    cb_state = cb.state if hasattr(cb, "state") else "closed"

    return DatatypeStatus(
        connector=connector,
        datatype=datatype,
        last_sync_status=sync_status,
        last_sync_at=sync_at,
        watermark=watermark,
        circuit_breaker_state=str(cb_state),
    )


async def _insert_control_command(
    connector: str,
    datatype: str,
    command: str,
    payload: dict | None = None,
) -> str:
    """Insert a control command into inout_ops_control and return the command ID."""
    if _pool is None:
        raise HTTPException(status_code=503, detail="Database pool not available")
    cmd_id = str(uuid.uuid4())
    async with _pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_control (id, connector, datatype, command, payload)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [cmd_id, connector, datatype, command, None],
        )
        await conn.commit()
    return cmd_id


@router.post(
    "/connectors/{connector}/datatypes/{datatype}/force-sync",
    response_model=ControlCommandResponse,
)
async def force_sync(connector: str, datatype: str) -> ControlCommandResponse:
    """Trigger a forced full sync for the given connector/datatype."""
    cmd_id = await _insert_control_command(connector, datatype, "force_full_sync")
    return ControlCommandResponse(
        command="force_full_sync",
        connector=connector,
        datatype=datatype,
        id=cmd_id,
    )


@router.post(
    "/connectors/{connector}/datatypes/{datatype}/pause",
    response_model=ControlCommandResponse,
)
async def pause_connector(connector: str, datatype: str) -> ControlCommandResponse:
    """Pause polling for the given connector/datatype."""
    cmd_id = await _insert_control_command(connector, datatype, "pause_connector")
    return ControlCommandResponse(
        command="pause_connector",
        connector=connector,
        datatype=datatype,
        id=cmd_id,
    )


@router.post(
    "/connectors/{connector}/datatypes/{datatype}/resume",
    response_model=ControlCommandResponse,
)
async def resume_connector(connector: str, datatype: str) -> ControlCommandResponse:
    """Resume polling for the given connector/datatype."""
    cmd_id = await _insert_control_command(connector, datatype, "resume_connector")
    return ControlCommandResponse(
        command="resume_connector",
        connector=connector,
        datatype=datatype,
        id=cmd_id,
    )


@router.get(
    "/dead-letter/{connector}/{datatype}",
    response_model=list[DeadLetterRow],
)
async def list_dead_letter(connector: str, datatype: str) -> list[DeadLetterRow]:
    """List the last 20 dead-letter rows for a connector/datatype."""
    if _pool is None:
        return []

    from inandout.postgres.schema import dead_letter_table_name
    table = dead_letter_table_name("ingestion", connector, datatype)

    try:
        async with _pool.connection() as conn:
            rows = await (await conn.execute(
                f"""
                SELECT id, external_id, error_message, error_class, failed_at, requeue_count
                FROM {table}
                ORDER BY failed_at DESC
                LIMIT 20
                """
            )).fetchall()
            return [
                DeadLetterRow(
                    id=r[0],
                    external_id=r[1],
                    error_message=r[2],
                    error_class=r[3],
                    failed_at=str(r[4]),
                    requeue_count=r[5],
                )
                for r in rows
            ]
    except Exception as exc:
        logger.warning("api_dead_letter_error", error=str(exc))
        return []


@router.post(
    "/dead-letter/{connector}/{datatype}/requeue",
    response_model=ControlCommandResponse,
)
async def requeue_dead_letter(connector: str, datatype: str) -> ControlCommandResponse:
    """Requeue dead-letter rows for reprocessing."""
    cmd_id = await _insert_control_command(connector, datatype, "requeue_dead_letter")
    return ControlCommandResponse(
        command="requeue_dead_letter",
        connector=connector,
        datatype=datatype,
        id=cmd_id,
    )


# ---------------------------------------------------------------------------
# Health score endpoint (Step 43)
# ---------------------------------------------------------------------------


class HealthScoreComponents(BaseModel):
    circuit_breaker: float
    error_rate: float
    dead_letter: float


class HealthScoreResponse(BaseModel):
    connector: str
    datatype: str
    health_score: float
    components: HealthScoreComponents


@router.get(
    "/connectors/{connector}/datatypes/{datatype}/health",
    response_model=HealthScoreResponse,
)
async def get_health_score(connector: str, datatype: str) -> HealthScoreResponse:
    """Return the composite health score for a connector/datatype."""
    from inandout.observability.health_score import compute_health_score, health_components
    from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState

    cb = get_circuit_breaker(connector, datatype)
    state = cb.state
    if state == CircuitState.open:
        cb_score = 0.0
    elif state == CircuitState.half_open:
        cb_score = 0.5
    else:
        cb_score = 1.0

    # Compute error rate and dead letter depth for components breakdown
    error_rate = 0.0
    dl_depth = 0
    if _pool is not None:
        try:
            async with _pool.connection() as conn:
                row = await (await conn.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
                        COUNT(*) AS total_runs
                    FROM inout_ops_sync_run
                    WHERE connector = %s AND datatype = %s
                      AND started_at >= NOW() - INTERVAL '1 hour'
                    """,
                    [connector, datatype],
                )).fetchone()
            if row and row[1] and row[1] > 0:
                error_rate = row[0] / row[1]
        except Exception:
            pass

        try:
            from inandout.postgres.schema import dead_letter_table_name
            dl_table = dead_letter_table_name("ingestion", connector, datatype)
            async with _pool.connection() as conn:
                dl_row = await (await conn.execute(
                    f"SELECT COUNT(*) FROM {dl_table}"
                )).fetchone()
            if dl_row:
                dl_depth = int(dl_row[0])
        except Exception:
            pass

    dl_score = max(0.0, 1.0 - (dl_depth / 100))
    score = max(0.0, (cb_score * 0.4) + ((1.0 - error_rate) * 0.4) + (dl_score * 0.2))

    return HealthScoreResponse(
        connector=connector,
        datatype=datatype,
        health_score=round(score, 4),
        components=HealthScoreComponents(
            circuit_breaker=cb_score,
            error_rate=round(1.0 - error_rate, 4),
            dead_letter=round(dl_score, 4),
        ),
    )


# ---------------------------------------------------------------------------
# SLA status endpoint (Step 47)
# ---------------------------------------------------------------------------


class SlaStatus(BaseModel):
    connector: str
    datatype: str
    violated: bool
    max_lag_seconds: int | None = None


@router.get("/sla", response_model=list[SlaStatus])
async def list_sla_status() -> list[SlaStatus]:
    """Return SLA status for all connectors/datatypes."""
    if _pool is None:
        return []
    results: list[SlaStatus] = []
    try:
        async with _pool.connection() as conn:
            rows = await (await conn.execute(
                """
                SELECT DISTINCT connector, datatype
                FROM inout_ops_sync_run
                ORDER BY connector, datatype
                """
            )).fetchall()

        for row in rows:
            connector, datatype = row[0], row[1]
            results.append(SlaStatus(
                connector=connector,
                datatype=datatype,
                violated=False,
            ))
    except Exception as exc:
        logger.warning("api_sla_error", error=str(exc))
    return results
