"""HTTP transport adapter — drives the pagination loop for ingestion."""
from __future__ import annotations

import re
from typing import Any, AsyncGenerator

import anyio
import httpx
import jmespath
import orjson
from aiolimiter import AsyncLimiter

from inandout.config.connector import ConnectorConfig
from inandout.config.ingestion import ListConfig
from inandout.config.pagination import PaginationStrategy
from inandout.transport.auth import build_auth_provider
from inandout.transport.errors import (
    ErrorClass,
    classify_http_error,
    retry_after_seconds,
)
from inandout.transport.rate_limiter import TokenBucket, get_rate_limiter


def _extract_records(data: Any, record_selector: str | None) -> list[dict[str, Any]]:
    if record_selector is None:
        if isinstance(data, list):
            return data
        return [data]
    result = jmespath.search(record_selector, data)
    if result is None:
        return []
    if isinstance(result, list):
        return result
    return [result]


def _substitute(value: str, watermark: str | None) -> str:
    if watermark is not None:
        value = value.replace("${watermark}", watermark)
    return value


def _parse_next_link(link_header: str) -> str | None:
    """Parse RFC 5988 Link header and return the 'next' URL if present."""
    for part in link_header.split(","):
        part = part.strip()
        m = re.match(r'<([^>]+)>.*rel="next"', part)
        if m:
            return m.group(1)
    return None


class HttpTransportAdapter:
    """Drives the HTTP pagination loop for a single connector."""

    def __init__(
        self,
        connector: ConnectorConfig,
        max_retries: int = 5,
    ) -> None:
        self._connector = connector
        self._auth = build_auth_provider(connector.auth)
        self._limiter: AsyncLimiter | None = None
        self._token_bucket: TokenBucket | None = None
        self._max_retries = max_retries
        rate_limit = connector.rate_limit
        if rate_limit and rate_limit.requests_per_second:
            self._limiter = AsyncLimiter(
                max_rate=rate_limit.requests_per_second,
                time_period=1.0,
            )
            burst = float(rate_limit.burst) if rate_limit.burst else rate_limit.requests_per_second * 2
            self._token_bucket = get_rate_limiter(
                connector.name,
                rate_per_second=rate_limit.requests_per_second,
                burst=burst,
            )
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "HttpTransportAdapter":
        conn = self._connector.connection
        timeout_cfg = conn.timeout
        timeout = httpx.Timeout(
            connect=float(timeout_cfg.connect.rstrip("s")) if timeout_cfg and timeout_cfg.connect else 10.0,
            read=float(timeout_cfg.read.rstrip("s")) if timeout_cfg and timeout_cfg.read else 30.0,
            write=float(timeout_cfg.write.rstrip("s")) if timeout_cfg and timeout_cfg.write else 30.0,
        ) if timeout_cfg else httpx.Timeout(10.0, read=30.0)
        self._client = httpx.AsyncClient(
            base_url=self._connector.connection.base_url,
            auth=self._auth,
            timeout=timeout,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _raw_request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a single raw request through the client (with rate limiting)."""
        assert self._client is not None, "Must be used as async context manager"
        # Token-bucket rate limiting (our own implementation)
        if self._token_bucket is not None:
            await self._token_bucket.acquire()
        if self._limiter:
            async with self._limiter:
                return await self._client.request(method, path, **kwargs)
        return await self._client.request(method, path, **kwargs)

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue a request with retry/backoff logic."""
        from inandout.observability.metrics import http_errors_total
        from inandout.transport.retry_budget import RetryBudgetExhaustedError, get_retry_budget

        max_retries = self._max_retries
        attempt = 0
        last_exc: Exception | None = None

        # Resolve retry budget if configured
        retry_budget = None
        retry_budget_cfg = self._connector.connection.retry_budget
        if retry_budget_cfg is not None:
            retry_budget = get_retry_budget(
                self._connector.name,
                retry_budget_cfg.max_attempts,
                retry_budget_cfg.window_secs,
            )

        while attempt <= max_retries:
            try:
                resp = await self._raw_request(method, path, **kwargs)

                if resp.status_code == 429:
                    exc = httpx.HTTPStatusError("429", request=resp.request, response=resp)
                    try:
                        http_errors_total.labels(
                            connector=self._connector.name,
                            datatype="",
                            status_code="429",
                            namespace="public",
                        ).inc()
                    except Exception:
                        pass
                    # Check budget before retrying
                    if retry_budget is not None and attempt > 0:
                        allowed = await retry_budget.consume()
                        if not allowed:
                            raise RetryBudgetExhaustedError(
                                f"Retry budget exhausted for connector {self._connector.name!r}"
                            )
                    wait = retry_after_seconds(exc) or (2 ** attempt)
                    await anyio.sleep(min(wait, 60.0))
                    attempt += 1
                    last_exc = exc
                    continue

                resp.raise_for_status()
                return resp

            except RetryBudgetExhaustedError:
                raise

            except httpx.HTTPStatusError as exc:
                try:
                    http_errors_total.labels(
                        connector=self._connector.name,
                        datatype="",
                        status_code=str(exc.response.status_code),
                        namespace="public",
                    ).inc()
                except Exception:
                    pass
                ec = classify_http_error(exc)
                if ec == ErrorClass.transient and attempt < max_retries:
                    # Check budget before retrying
                    if retry_budget is not None:
                        allowed = await retry_budget.consume()
                        if not allowed:
                            raise RetryBudgetExhaustedError(
                                f"Retry budget exhausted for connector {self._connector.name!r}"
                            )
                    wait = min(2 ** attempt, 60.0)
                    await anyio.sleep(wait)
                    attempt += 1
                    last_exc = exc
                    continue
                raise

            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as exc:
                if attempt < max_retries:
                    # Check budget before retrying
                    if retry_budget is not None:
                        allowed = await retry_budget.consume()
                        if not allowed:
                            raise RetryBudgetExhaustedError(
                                f"Retry budget exhausted for connector {self._connector.name!r}"
                            )
                    wait = min(2 ** attempt, 60.0)
                    await anyio.sleep(wait)
                    attempt += 1
                    last_exc = exc
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError("Retry loop exited unexpectedly")

    async def fetch_pages(
        self,
        list_config: ListConfig,
        watermark: str | None = None,
        window_end: str | None = None,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        # GraphQL mode: detect by presence of graphql_query
        graphql_query = getattr(list_config, "graphql_query", None)
        if graphql_query is not None:
            async for page in self._fetch_graphql_pages(list_config, watermark=watermark):
                yield page
            return

        method = list_config.method.upper()
        path = list_config.path
        record_selector = list_config.record_selector
        pagination = list_config.pagination
        incremental = list_config.incremental

        # Build base params from incremental config
        base_params: dict[str, str] = {}
        if incremental and incremental.enabled and watermark and incremental.request_filter:
            rf = incremental.request_filter
            if rf.mode == "query_param":
                extra = rf.model_extra or {}
                param_name = str(extra.get("param", "since"))
                param_value = _substitute(str(extra.get("value", "${watermark}")), watermark)
                base_params[param_name] = param_value
                # Inject window_end via until_param if configured
                until_param = getattr(rf, "until_param", None)
                if until_param and window_end is not None:
                    base_params[until_param] = window_end

        termination = set(pagination.termination or [])

        if pagination.strategy == PaginationStrategy.cursor:
            assert pagination.cursor is not None
            cursor_value: str | None = None
            while True:
                params = dict(base_params)
                if cursor_value is not None:
                    params[pagination.cursor.request_param] = cursor_value
                resp = await self._request(method, path, params=params)
                data = orjson.loads(resp.content)
                records = _extract_records(data, record_selector)
                yield records
                if not records and ("empty_page" in termination or not termination):
                    break
                next_cursor = jmespath.search(pagination.cursor.response_path, data)
                if next_cursor is None:
                    break
                cursor_value = str(next_cursor)

        elif pagination.strategy == PaginationStrategy.offset:
            offset_cfg = pagination.offset or {}
            page_size = int(offset_cfg.get("page_size", 100))
            offset_param = str(offset_cfg.get("offset_param", "offset"))
            limit_param = str(offset_cfg.get("limit_param", "limit"))
            offset = 0
            while True:
                params = {**base_params, offset_param: str(offset), limit_param: str(page_size)}
                resp = await self._request(method, path, params=params)
                data = orjson.loads(resp.content)
                records = _extract_records(data, record_selector)
                yield records
                if len(records) < page_size:
                    break
                offset += len(records)

        elif pagination.strategy == PaginationStrategy.link_header:
            url: str | None = path
            while url:
                resp = await self._request(method, url, params=base_params if url == path else {})
                data = orjson.loads(resp.content)
                records = _extract_records(data, record_selector)
                yield records
                if not records:
                    break
                url = _parse_next_link(resp.headers.get("link", ""))

        elif pagination.strategy == PaginationStrategy.page_number:
            pn_cfg = pagination.page_number or {}
            page_size = int(pn_cfg.get("page_size", 100))
            page_param = str(pn_cfg.get("page_param", "page"))
            per_page_param = str(pn_cfg.get("per_page_param", "per_page"))
            page = 1
            while True:
                params = {**base_params, page_param: str(page), per_page_param: str(page_size)}
                resp = await self._request(method, path, params=params)
                data = orjson.loads(resp.content)
                records = _extract_records(data, record_selector)
                yield records
                if len(records) < page_size:
                    break
                page += 1

    async def _fetch_graphql_pages(
        self,
        list_config: ListConfig,
        watermark: str | None = None,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Fetch pages using GraphQL POST requests."""
        from inandout.ingestion.graphql import build_graphql_request_body, extract_graphql_records

        graphql_query: str = list_config.graphql_query  # type: ignore[assignment]
        graphql_variables: dict[str, Any] = getattr(list_config, "graphql_variables", {}) or {}
        graphql_data_path: str | None = getattr(list_config, "graphql_data_path", None)
        path = list_config.path
        pagination = list_config.pagination

        cursor_value: str | None = None
        while True:
            body = build_graphql_request_body(
                graphql_query, graphql_variables, cursor=cursor_value
            )
            resp = await self._request("POST", path, json=body)
            data = orjson.loads(resp.content)

            if graphql_data_path:
                records = extract_graphql_records(data, graphql_data_path)
            else:
                records = _extract_records(data, list_config.record_selector)

            yield records

            if not records:
                break

            # Cursor-based pagination for GraphQL
            if pagination.strategy == PaginationStrategy.cursor and pagination.cursor is not None:
                next_cursor = jmespath.search(pagination.cursor.response_path, data)
                if next_cursor is None:
                    break
                cursor_value = str(next_cursor)
            else:
                break

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
