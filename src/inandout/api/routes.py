"""Runtime management API routes."""
from __future__ import annotations

import fnmatch
import uuid
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from inandout.config._duration import parse_duration

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


def _health_bracket(score: float) -> str:
    """Return 'healthy', 'degraded', or 'unhealthy' for a health score."""
    if score >= 0.8:
        return "healthy"
    if score >= 0.5:
        return "degraded"
    return "unhealthy"


@router.get("/connectors", response_model=list[ConnectorSummary])
async def list_connectors(
    status: str | None = Query(default=None, description="Filter by health bracket: healthy|degraded|unhealthy"),
    connector: str | None = Query(default=None, description="Glob filter on connector name, e.g. hub*"),
    since: str | None = Query(default=None, description="Only connectors with a sync in the last N (e.g. 1h)"),
) -> list[ConnectorSummary]:
    """List all connectors with their datatypes and last sync info."""
    if _pool is None:
        return []

    since_secs: float | None = None
    if since:
        try:
            since_secs = parse_duration(since)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid since duration: {since!r}")

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
            row_status = row[2]
            finished_at = row[3]
            finished_at_str = str(finished_at) if finished_at else None

            # Apply ?since filter
            if since_secs is not None and finished_at is not None:
                import datetime
                now = datetime.datetime.now(datetime.timezone.utc)
                fa = finished_at
                if hasattr(fa, "tzinfo") and fa.tzinfo is None:
                    fa = fa.replace(tzinfo=datetime.timezone.utc)
                if (now - fa).total_seconds() > since_secs:
                    continue

            if connector_name not in connector_map:
                connector_map[connector_name] = {
                    "name": connector_name,
                    "datatypes": [],
                    "last_sync_at": finished_at_str,
                    "last_sync_status": row_status,
                }
            connector_map[connector_name]["datatypes"].append(datatype)

        summaries = [ConnectorSummary(**v) for v in connector_map.values()]

        # Apply ?connector glob filter
        if connector:
            summaries = [s for s in summaries if fnmatch.fnmatch(s.name, connector)]

        # Apply ?status health score filter
        if status:
            from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
            filtered = []
            for s in summaries:
                # Compute a rough per-connector health score using CB state of first datatype
                if s.datatypes:
                    cb = get_circuit_breaker(s.name, s.datatypes[0])
                    if cb.state == CircuitState.open:
                        cb_score = 0.0
                    elif cb.state == CircuitState.half_open:
                        cb_score = 0.5
                    else:
                        cb_score = 1.0
                    score = cb_score
                else:
                    score = 1.0
                bracket = _health_bracket(score)
                if bracket == status:
                    filtered.append(s)
            summaries = filtered

        return summaries
    except HTTPException:
        raise
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
async def list_dead_letter(
    connector: str,
    datatype: str,
    limit: int = Query(default=20, ge=1, le=100, description="Max rows to return (1-100)"),
    since: str | None = Query(default=None, description="Only rows where failed_at >= NOW() - INTERVAL"),
) -> list[DeadLetterRow]:
    """List dead-letter rows for a connector/datatype."""
    if _pool is None:
        return []

    since_secs: float | None = None
    if since:
        try:
            since_secs = parse_duration(since)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid since duration: {since!r}")

    from inandout.postgres.schema import dead_letter_table_name
    table = dead_letter_table_name("ingestion", connector, datatype)

    try:
        async with _pool.connection() as conn:
            if since_secs is not None:
                rows = await (await conn.execute(
                    f"""
                    SELECT id, external_id, error_message, error_class, failed_at, requeue_count
                    FROM {table}
                    WHERE failed_at >= NOW() - INTERVAL '1 second' * %s
                    ORDER BY failed_at DESC
                    LIMIT %s
                    """,
                    [since_secs, limit],
                )).fetchall()
            else:
                rows = await (await conn.execute(
                    f"""
                    SELECT id, external_id, error_message, error_class, failed_at, requeue_count
                    FROM {table}
                    ORDER BY failed_at DESC
                    LIMIT %s
                    """,
                    [limit],
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
    except HTTPException:
        raise
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
# Sync runs endpoint (Step 48)
# ---------------------------------------------------------------------------


class SyncRunRow(BaseModel):
    id: str
    connector: str
    datatype: str
    mode: str
    status: str
    started_at: str
    finished_at: str | None = None
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_errored: int = 0
    error_message: str | None = None


@router.get("/sync-runs", response_model=list[SyncRunRow])
async def list_sync_runs(
    connector: str | None = Query(default=None),
    datatype: str | None = Query(default=None),
    status: str | None = Query(default=None, description="Filter by status: completed|failed|running"),
    since: str | None = Query(default=None, description="Only runs started within e.g. 1h"),
    limit: int = Query(default=20, ge=1, le=1000),
) -> list[SyncRunRow]:
    """List sync run rows from inout_ops_sync_run."""
    if _pool is None:
        return []

    since_secs: float | None = None
    if since:
        try:
            since_secs = parse_duration(since)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid since duration: {since!r}")

    conditions: list[str] = []
    params: list[Any] = []

    if connector:
        conditions.append("connector = %s")
        params.append(connector)
    if datatype:
        conditions.append("datatype = %s")
        params.append(datatype)
    if status:
        conditions.append("status = %s")
        params.append(status)
    if since_secs is not None:
        conditions.append("started_at >= NOW() - INTERVAL '1 second' * %s")
        params.append(since_secs)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    try:
        async with _pool.connection() as conn:
            rows = await (await conn.execute(
                f"""
                SELECT id, connector, datatype, mode, status, started_at, finished_at,
                       records_fetched, records_inserted, records_updated, records_errored,
                       error_message
                FROM inout_ops_sync_run
                {where}
                ORDER BY started_at DESC
                LIMIT %s
                """,
                params,
            )).fetchall()

        return [
            SyncRunRow(
                id=str(r[0]),
                connector=r[1],
                datatype=r[2],
                mode=r[3],
                status=r[4],
                started_at=str(r[5]),
                finished_at=str(r[6]) if r[6] else None,
                records_fetched=r[7] or 0,
                records_inserted=r[8] or 0,
                records_updated=r[9] or 0,
                records_errored=r[10] or 0,
                error_message=r[11],
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("api_sync_runs_error", error=str(exc))
        return []


# ---------------------------------------------------------------------------
# SLA status endpoint (Step 47)
# ---------------------------------------------------------------------------


class SlaStatus(BaseModel):
    connector: str
    datatype: str
    violated: bool
    max_lag_seconds: int | None = None


# ---------------------------------------------------------------------------
# Writeback audit endpoint (Step 63)
# ---------------------------------------------------------------------------


class WritebackAuditRow(BaseModel):
    id: int
    connector: str
    datatype: str
    action: str
    external_id: str | None = None
    status: str
    processed_at: str
    payload_snapshot: Any = None
    field_diff: Any = None


@router.get(
    "/writeback-audit/{connector}/{datatype}",
    response_model=list[WritebackAuditRow],
)
async def list_writeback_audit(
    connector: str,
    datatype: str,
    since: str | None = Query(default=None, description="Only rows where processed_at >= NOW() - INTERVAL"),
    limit: int = Query(default=20, ge=1, le=1000, description="Max rows to return"),
) -> list[WritebackAuditRow]:
    """Return recent writeback audit rows for a connector/datatype."""
    if _pool is None:
        return []

    since_secs: float | None = None
    if since:
        try:
            since_secs = parse_duration(since)
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid since duration: {since!r}")

    try:
        async with _pool.connection() as conn:
            if since_secs is not None:
                rows = await (await conn.execute(
                    """
                    SELECT id, connector, datatype, action, external_id, status,
                           processed_at, payload_snapshot, field_diff
                    FROM inout_ops_writeback_result
                    WHERE connector = %s AND datatype = %s
                      AND processed_at >= NOW() - INTERVAL '1 second' * %s
                    ORDER BY processed_at DESC
                    LIMIT %s
                    """,
                    [connector, datatype, since_secs, limit],
                )).fetchall()
            else:
                rows = await (await conn.execute(
                    """
                    SELECT id, connector, datatype, action, external_id, status,
                           processed_at, payload_snapshot, field_diff
                    FROM inout_ops_writeback_result
                    WHERE connector = %s AND datatype = %s
                    ORDER BY processed_at DESC
                    LIMIT %s
                    """,
                    [connector, datatype, limit],
                )).fetchall()
            return [
                WritebackAuditRow(
                    id=r[0],
                    connector=r[1],
                    datatype=r[2],
                    action=r[3],
                    external_id=r[4],
                    status=r[5],
                    processed_at=str(r[6]),
                    payload_snapshot=r[7],
                    field_diff=r[8],
                )
                for r in rows
            ]
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("api_writeback_audit_error", error=str(exc))
        return []


# ---------------------------------------------------------------------------
# Billing / cost attribution metrics endpoint (Step 78)
# ---------------------------------------------------------------------------


class NamespaceMetricsSummary(BaseModel):
    namespace: str
    connectors: int
    total_records_processed_24h: int
    total_quality_violations_24h: int
    connectors_healthy: int
    connectors_degraded: int
    connectors_unhealthy: int
    dead_letter_total: int


@router.get(
    "/namespaces/{namespace}/metrics-summary",
    response_model=NamespaceMetricsSummary,
)
async def get_namespace_metrics_summary(namespace: str) -> NamespaceMetricsSummary:
    """Return a billing/cost metrics summary for a namespace."""
    if _pool is None:
        return NamespaceMetricsSummary(
            namespace=namespace,
            connectors=0,
            total_records_processed_24h=0,
            total_quality_violations_24h=0,
            connectors_healthy=0,
            connectors_degraded=0,
            connectors_unhealthy=0,
            dead_letter_total=0,
        )

    # Table prefix pattern for this namespace
    table_prefix = f"inout_{namespace}_"

    total_records_24h = 0
    total_qv_24h = 0
    connectors_set: set[str] = set()
    connectors_healthy = 0
    connectors_degraded = 0
    connectors_unhealthy = 0
    dead_letter_total = 0

    try:
        async with _pool.connection() as conn:
            # Count records and connectors from sync run log
            rows = await (await conn.execute(
                """
                SELECT connector, datatype,
                       COALESCE(SUM(records_inserted + records_updated), 0) AS records_24h
                FROM inout_ops_sync_run
                WHERE started_at >= NOW() - INTERVAL '24 hours'
                GROUP BY connector, datatype
                """
            )).fetchall()

            for row in rows:
                connector_name, datatype, recs = row[0], row[1], row[2]
                connectors_set.add(connector_name)
                total_records_24h += int(recs or 0)

            # Compute health brackets for all known connector/datatype pairs
            from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState

            all_pairs = await (await conn.execute(
                """
                SELECT DISTINCT connector, datatype
                FROM inout_ops_sync_run
                """
            )).fetchall()

            for pair_row in all_pairs:
                c, d = pair_row[0], pair_row[1]
                cb = get_circuit_breaker(c, d)
                if cb.state == CircuitState.open:
                    cb_score = 0.0
                elif cb.state == CircuitState.half_open:
                    cb_score = 0.5
                else:
                    cb_score = 1.0
                # Simple health score based on CB state alone
                bracket = _health_bracket(cb_score)
                if bracket == "healthy":
                    connectors_healthy += 1
                elif bracket == "degraded":
                    connectors_degraded += 1
                else:
                    connectors_unhealthy += 1

    except Exception as exc:
        logger.warning("namespace_metrics_summary_error", namespace=namespace, error=str(exc))

    return NamespaceMetricsSummary(
        namespace=namespace,
        connectors=len(connectors_set),
        total_records_processed_24h=total_records_24h,
        total_quality_violations_24h=total_qv_24h,
        connectors_healthy=connectors_healthy,
        connectors_degraded=connectors_degraded,
        connectors_unhealthy=connectors_unhealthy,
        dead_letter_total=dead_letter_total,
    )


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


# ---------------------------------------------------------------------------
# Lineage endpoint (Step 86)
# ---------------------------------------------------------------------------


class LineageEntry(BaseModel):
    run_id: str | None = None
    fetched_at: str | None = None
    api_path: str | None = None
    watermark_at_fetch: str | None = None
    page_number: int | None = None
    source: str = "source_table"


@router.get(
    "/lineage/{connector}/{datatype}/{external_id}",
    response_model=list[LineageEntry],
)
async def get_record_lineage(
    connector: str,
    datatype: str,
    external_id: str,
) -> list[LineageEntry]:
    """Return lineage history for a specific record, newest first."""
    if _pool is None:
        return []

    from inandout.postgres.schema import source_table_name, source_history_table_name

    entries: list[LineageEntry] = []

    try:
        source_table = source_table_name(connector, datatype)
        hist_table = source_history_table_name(connector, datatype)

        async with _pool.connection() as conn:
            # Check source table for current lineage
            try:
                row = await (await conn.execute(
                    f"SELECT _lineage, _ingested_at FROM {source_table} WHERE external_id = %s",
                    [external_id],
                )).fetchone()
                if row and row[0] is not None:
                    lineage = row[0] if isinstance(row[0], dict) else {}
                    entries.append(LineageEntry(
                        run_id=lineage.get("run_id"),
                        fetched_at=lineage.get("fetched_at"),
                        api_path=lineage.get("api_path"),
                        watermark_at_fetch=lineage.get("watermark_at_fetch"),
                        page_number=lineage.get("page_number"),
                        source="source_table",
                    ))
            except Exception:
                pass

            # Check history table for historical lineage
            try:
                hist_rows = await (await conn.execute(
                    f"""
                    SELECT _sync_run_id, _ingested_at
                    FROM {hist_table}
                    WHERE external_id = %s
                    ORDER BY _ingested_at DESC
                    """,
                    [external_id],
                )).fetchall()
                for hr in hist_rows:
                    run_id_val = str(hr[0]) if hr[0] else None
                    entries.append(LineageEntry(
                        run_id=run_id_val,
                        fetched_at=str(hr[1]) if hr[1] else None,
                        source="history_table",
                    ))
            except Exception:
                pass

        return entries
    except Exception as exc:
        logger.warning("api_lineage_error", connector=connector, datatype=datatype, error=str(exc))
        return []


# ---------------------------------------------------------------------------
# Identity map endpoint (Priority 5)
# ---------------------------------------------------------------------------


class IdentityMapRow(BaseModel):
    connector: str
    datatype: str
    external_id: str
    internal_id: str
    created_at: str
    updated_at: str


@router.get(
    "/identity-map/{connector}/{datatype}",
    response_model=list[IdentityMapRow],
)
async def get_identity_map(
    connector: str,
    datatype: str,
    external_id: str | None = Query(default=None, description="Filter by specific external_id"),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[IdentityMapRow]:
    """Return identity map entries mapping external IDs to internal IDs."""
    if _pool is None:
        return []

    try:
        async with _pool.connection() as conn:
            if external_id is not None:
                rows = await (await conn.execute(
                    """
                    SELECT connector, datatype, external_id, internal_id, created_at, updated_at
                    FROM inout_ops_identity_map
                    WHERE connector = %s AND datatype = %s AND external_id = %s
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    [connector, datatype, external_id, limit],
                )).fetchall()
            else:
                rows = await (await conn.execute(
                    """
                    SELECT connector, datatype, external_id, internal_id, created_at, updated_at
                    FROM inout_ops_identity_map
                    WHERE connector = %s AND datatype = %s
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    [connector, datatype, limit],
                )).fetchall()
        return [
            IdentityMapRow(
                connector=r[0],
                datatype=r[1],
                external_id=r[2],
                internal_id=r[3],
                created_at=str(r[4]),
                updated_at=str(r[5]),
            )
            for r in rows
        ]
    except Exception as exc:
        logger.warning("api_identity_map_error", connector=connector, datatype=datatype, error=str(exc))
        return []
