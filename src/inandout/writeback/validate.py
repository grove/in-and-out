"""Writeback connector validation (T2 #37).

Performs non-destructive checks:
  1. Config syntax (Pydantic schema validation)
  2. Connectivity to target API
  3. Authentication (400/401/403 check)
  4. Per-datatype ETag/If-Match conditional-write probe
  5. Effective ProtectionLevel reporting
  6. Field-mapping / operation-path sanity

Intended to run before a writeback connector is activated for the first time
or after config changes that affect the target API surface.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

from inandout.transport.http import HttpTransportAdapter

logger = structlog.get_logger(__name__)


@dataclass
class DatatypeValidationResult:
    """Per-datatype findings from the validation probe."""

    datatype: str
    configured_protection_level: str
    effective_protection_level: str   # actual capability detected on the server
    etag_support: bool
    if_match_support: bool
    operations_ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class WritebackValidateResult:
    """Full result of validating a writeback connector."""

    connector: str
    connectivity: str = "unknown"   # "ok" | "failed" | "unknown"
    auth: str = "unknown"           # "ok" | "failed" | "unknown"
    datatypes: list[DatatypeValidationResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            self.connectivity == "ok"
            and self.auth == "ok"
            and not self.errors
            and all(not d.errors for d in self.datatypes)
        )


# ---------------------------------------------------------------------------
# ETag probing
# ---------------------------------------------------------------------------

async def probe_etag_support(
    transport: Any,
    lookup_path: str,
    etag_header: str = "ETag",
    if_match_header: str = "If-Match",
) -> tuple[bool, bool]:
    """Probe whether the target endpoint supports ETag / If-Match headers.

    Performs a GET against *lookup_path* (with template variables stripped)
    and inspects the response headers.

    Returns ``(etag_supported, if_match_supported)``.  Both may be False when
    connectivity fails or the path is not available. Failures are caught and
    logged rather than propagated.
    """
    # Sanitise path: replace template variables like ${external_id} with a
    # placeholder value so we get a real response (even a 404 carries headers)
    import re
    bare_path = re.sub(r"\$\{[^}]+\}", "probe", lookup_path).rstrip("/") or "/"

    try:
        resp = await transport._raw_request("GET", bare_path)
        etag_val = resp.headers.get(etag_header) or resp.headers.get(etag_header.lower())
        etag_supported = bool(etag_val)

        # Some APIs return ETag but only honour If-Match on PATCH/PUT; we
        # cannot reliably detect If-Match support without sending a write.
        # Assume If-Match is supported when ETags are present.
        if_match_supported = etag_supported

        return etag_supported, if_match_supported
    except Exception as exc:
        logger.debug("etag_probe_failed", path=bare_path, error=str(exc))
        return False, False


# ---------------------------------------------------------------------------
# Per-connector validation
# ---------------------------------------------------------------------------

async def validate_writeback_connector(
    connector_cfg: Any,
    datatype_names: list[str] | None = None,
) -> WritebackValidateResult:
    """Validate *connector_cfg* non-destructively.

    Parameters
    ----------
    connector_cfg:
        A loaded ``ConnectorConfig`` object.
    datatype_names:
        Subset of datatypes to validate.  Defaults to all datatypes that have a
        writeback config.

    Returns
    -------
    WritebackValidateResult
    """
    # Remove the inline import now that HttpTransportAdapter is at module level
    from inandout.config.writeback import ProtectionLevel

    result = WritebackValidateResult(connector=connector_cfg.name)

    # Choose datatypes to validate
    all_datatypes = connector_cfg.datatypes
    if datatype_names is None:
        datatype_names = [
            name for name, dt in all_datatypes.items()
            if getattr(dt, "writeback", None) is not None
        ]
    if not datatype_names:
        result.errors.append("No datatypes with writeback configuration found")
        result.connectivity = "unknown"
        result.auth = "unknown"
        return result

    try:
        async with HttpTransportAdapter(connector_cfg) as transport:
            # ------------------------------------------------------------------
            # 1. Connectivity + auth: check the base URL reachability
            # ------------------------------------------------------------------
            try:
                ping_resp = await transport._raw_request("GET", "/")
                result.connectivity = "ok"
                if ping_resp.status_code == 401:
                    result.auth = "failed"
                    result.errors.append(f"401 Unauthorized from {connector_cfg.connection.base_url}")
                elif ping_resp.status_code == 403:
                    result.auth = "failed"
                    result.errors.append(f"403 Forbidden from {connector_cfg.connection.base_url}")
                else:
                    result.auth = "ok"
            except Exception as conn_exc:
                result.connectivity = "failed"
                result.auth = "unknown"
                result.errors.append(f"connectivity failed: {conn_exc}")
                # Cannot probe further without connectivity
                for dtype_name in datatype_names:
                    result.datatypes.append(
                        DatatypeValidationResult(
                            datatype=dtype_name,
                            configured_protection_level="unknown",
                            effective_protection_level="unknown",
                            etag_support=False,
                            if_match_support=False,
                            operations_ok=False,
                            errors=[f"skipped — connectivity failed: {conn_exc}"],
                        )
                    )
                return result

            # ------------------------------------------------------------------
            # 2. Per-datatype probes
            # ------------------------------------------------------------------
            for dtype_name in datatype_names:
                dt_cfg = all_datatypes.get(dtype_name)
                if dt_cfg is None:
                    result.datatypes.append(
                        DatatypeValidationResult(
                            datatype=dtype_name,
                            configured_protection_level="unknown",
                            effective_protection_level="unknown",
                            etag_support=False,
                            if_match_support=False,
                            operations_ok=False,
                            errors=[f"datatype '{dtype_name}' not found in connector config"],
                        )
                    )
                    continue

                wb_cfg = getattr(dt_cfg, "writeback", None)
                if wb_cfg is None:
                    result.datatypes.append(
                        DatatypeValidationResult(
                            datatype=dtype_name,
                            configured_protection_level="n/a",
                            effective_protection_level="n/a",
                            etag_support=False,
                            if_match_support=False,
                            operations_ok=False,
                            errors=[f"datatype '{dtype_name}' has no writeback configuration"],
                        )
                    )
                    continue

                dt_result = DatatypeValidationResult(
                    datatype=dtype_name,
                    configured_protection_level=wb_cfg.protection_level.name,
                    effective_protection_level="unknown",
                    etag_support=False,
                    if_match_support=False,
                    operations_ok=True,
                )

                # Operation-path sanity
                ops = wb_cfg.operations
                path_errors: list[str] = []
                for op_name in ("lookup", "insert", "update", "delete"):
                    op = getattr(ops, op_name, None)
                    if op is not None and (not getattr(op, "path", None)):
                        path_errors.append(f"operations.{op_name}.path is empty")
                if path_errors:
                    dt_result.operations_ok = False
                    dt_result.errors.extend(path_errors)

                # ETag probe (only if connectivity is ok and lookup path exists)
                lookup_op = getattr(ops, "lookup", None)
                lookup_path = getattr(lookup_op, "path", None) if lookup_op else None
                if lookup_path and result.connectivity == "ok":
                    etag_ok, if_match_ok = await probe_etag_support(
                        transport, lookup_path,
                        etag_header=wb_cfg.etag_header,
                        if_match_header=wb_cfg.if_match_header,
                    )
                    dt_result.etag_support = etag_ok
                    dt_result.if_match_support = if_match_ok

                # Determine effective protection level
                cfg_pl = wb_cfg.protection_level
                if cfg_pl == ProtectionLevel.optimistic or cfg_pl == ProtectionLevel.conditional_write_required:
                    if dt_result.etag_support:
                        dt_result.effective_protection_level = cfg_pl.name
                    else:
                        dt_result.effective_protection_level = ProtectionLevel.none.name
                        dt_result.warnings.append(
                            f"configured protection_level={cfg_pl.name!r} but target does not "
                            "appear to return ETags — effective protection will be 'none'"
                        )
                elif cfg_pl == ProtectionLevel.post_write_verify:
                    dt_result.effective_protection_level = ProtectionLevel.post_write_verify.name
                else:
                    dt_result.effective_protection_level = ProtectionLevel.none.name

                result.datatypes.append(dt_result)

    except Exception as outer_exc:
        result.connectivity = "failed"
        result.errors.append(f"validation error: {outer_exc}")

    logger.info(
        "writeback_validate_complete",
        connector=connector_cfg.name,
        datatypes=[d.datatype for d in result.datatypes],
        ok=result.ok,
        errors=result.errors,
    )
    return result
