"""HTTP error classification and retry policy."""
from __future__ import annotations

from enum import StrEnum

import httpx


class ErrorClass(StrEnum):
    transient = "transient"          # retry with backoff
    rate_limit = "rate_limit"        # honour Retry-After
    auth = "auth"                    # attempt token refresh once
    data_error = "data_error"        # dead-letter immediately
    config_error = "config_error"    # halt connector


def classify_http_error(exc: httpx.HTTPStatusError) -> ErrorClass:
    status = exc.response.status_code
    if status == 429:
        return ErrorClass.rate_limit
    if status in (401, 403):
        return ErrorClass.auth
    if status in (400, 404, 409, 422):
        return ErrorClass.data_error
    if 500 <= status < 600:
        return ErrorClass.transient
    return ErrorClass.transient


def classify_request_error(exc: Exception) -> ErrorClass:
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError)):
        return ErrorClass.transient
    if isinstance(exc, httpx.HTTPStatusError):
        return classify_http_error(exc)
    return ErrorClass.transient


def is_retryable(exc: Exception) -> bool:
    return classify_request_error(exc) == ErrorClass.transient


def retry_after_seconds(exc: httpx.HTTPStatusError) -> float | None:
    """Extract Retry-After value in seconds from a 429 response."""
    header = exc.response.headers.get("Retry-After")
    if header is None:
        return None
    try:
        return float(header)
    except ValueError:
        return 60.0  # default if header is a date string
