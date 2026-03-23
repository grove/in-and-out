"""Partial-success batch response handling (T2 #29).

Parses API responses that contain per-record success/failure status
(e.g. HTTP 207 Multi-Status) so the writeback engine can classify each
record individually rather than treating the whole batch as pass/fail.
"""
from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def _dot_get(obj: Any, path: str) -> Any:
    """Traverse *obj* using dot-notation *path*. Returns None if any step is missing."""
    parts = path.split(".")
    cur: Any = obj
    for p in parts:
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            try:
                cur = cur[int(p)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cur


def parse_batch_response(
    response_body: dict[str, Any],
    config: Any,  # BatchResponseConfig — avoid circular import
) -> dict[str, bool]:
    """Parse a batch response body into ``{external_id: success_bool}``.

    Parameters
    ----------
    response_body:
        Parsed JSON response body from the API.
    config:
        ``BatchResponseConfig`` instance.

    Returns
    -------
    dict[str, bool]
        Maps each external_id found in the response to True (success) or False (failure).
    """
    results: dict[str, bool] = {}

    # Locate the array of per-record results
    if config.success_path is not None:
        items = _dot_get(response_body, config.success_path)
    else:
        # If no success_path, treat the top-level body as the array
        items = response_body if isinstance(response_body, list) else None

    if not isinstance(items, list):
        logger.warning(
            "batch_response_no_items_array",
            success_path=config.success_path,
        )
        return results

    for item in items:
        if not isinstance(item, dict):
            logger.warning("batch_response_item_not_dict", item=item)
            continue

        external_id = _dot_get(item, config.record_id_path)
        if external_id is None:
            logger.warning(
                "batch_response_missing_record_id",
                record_id_path=config.record_id_path,
                item_keys=list(item.keys()),
            )
            continue

        external_id_str = str(external_id)
        status_val = _dot_get(item, config.status_path)
        success = str(status_val) in config.success_statuses if status_val is not None else False
        results[external_id_str] = success

    return results


def extract_batch_errors(
    response_body: dict[str, Any],
    config: Any,  # BatchResponseConfig
) -> dict[str, str]:
    """Return ``{external_id: error_message}`` for failed records in a batch response.

    Parameters
    ----------
    response_body:
        Parsed JSON response body.
    config:
        ``BatchResponseConfig`` instance.
    """
    errors: dict[str, str] = {}

    if config.success_path is not None:
        items = _dot_get(response_body, config.success_path)
    else:
        items = response_body if isinstance(response_body, list) else None

    if not isinstance(items, list):
        return errors

    for item in items:
        if not isinstance(item, dict):
            continue

        external_id = _dot_get(item, config.record_id_path)
        if external_id is None:
            continue

        external_id_str = str(external_id)
        status_val = _dot_get(item, config.status_path)
        is_success = str(status_val) in config.success_statuses if status_val is not None else False

        if not is_success:
            if config.error_path is not None:
                err_msg = _dot_get(item, config.error_path)
                errors[external_id_str] = str(err_msg) if err_msg is not None else "unknown error"
            else:
                errors[external_id_str] = f"status={status_val!r}"

    return errors
