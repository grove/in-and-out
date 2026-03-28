"""Configuration-driven HTTP mock server for connector integration testing.

``GenericSimulator`` derives all HTTP route structure from a ``ConnectorConfig``
and populates it with data and behaviour declared in a ``SimulatorConfig``.
No per-system Python code is required.

What is derived from ``ConnectorConfig``
-----------------------------------------
* **List (GET) endpoints** — URL path and all four pagination strategies
  (cursor, offset, link_header, page_number) including response-envelope
  construction (cursor token placement, Link headers, total / page counts).
* **Auth endpoints** — OAuth2 token URL and token-response shape.
* **Write endpoints** — HTTP method + URL for any operations declared in
  ``writeback.operations`` (insert, update, delete, archive, upsert).

What comes from ``SimulatorConfig``
-------------------------------------
* Fixture records (the data to serve).
* ``route_discriminator`` — regex-based dispatch for shared-path APIs
  (e.g. Salesforce SOQL ``/query?q=SELECT … FROM Contact``).
* ``cursor_url_template`` — generates next-page URLs for cursor-as-URL
  pagination (e.g. Salesforce ``nextRecordsUrl``).
* ``response_envelope`` — computed envelope fields (``totalSize``, ``done``).
* ``extra_routes`` — detail-lookup GET, write endpoints absent from the
  connector config, or any other route the test needs.
* ``errors`` — per-datatype error injection for circuit-breaker testing.

Usage::

    from inandout.stubs.config import SimulatorConfig, SimulatorDatatypeConfig
    from inandout.stubs.generic import GenericSimulator

    sim_cfg = SimulatorConfig(
        datatypes={
            "contacts": SimulatorDatatypeConfig(
                fixtures=[{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
                page_size=1,
            ),
        },
    )
    connector = load_connector("hubspot.yaml").connector
    with GenericSimulator(connector, sim_cfg):
        async with HttpTransportAdapter(connector) as t:
            async for page in t.fetch_pages(connector.datatypes["contacts"].ingestion.list):
                process(page)
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

import httpx
import respx

from inandout.config.connector import ConnectorConfig
from inandout.config.pagination import PaginationStrategy
from inandout.stubs.config import ExtraRoute, SimulatorConfig, SimulatorDatatypeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_path(obj: dict, path: str, value: Any) -> None:
    """Set *value* at a dot-notation *path* in *obj*, creating intermediate dicts."""
    parts = path.split(".")
    for part in parts[:-1]:
        obj = obj.setdefault(part, {})
    obj[parts[-1]] = value


def _resolve_envelope_value(token: Any, total: int, has_more: bool) -> Any:
    """Replace magic strings used in ``SimulatorDatatypeConfig.response_envelope``."""
    if token == "${total_count}":
        return total
    if token == "${done}":
        return not has_more
    if token == "${has_more}":
        return has_more
    return token


# ---------------------------------------------------------------------------
# GenericSimulator
# ---------------------------------------------------------------------------


class GenericSimulator:
    """Config-driven HTTP mock server for connector integration testing.

    Implements the ``respx`` context-manager protocol so it can be used as::

        with GenericSimulator(connector, sim_cfg):
            # All httpx calls are intercepted by respx
            ...
    """

    def __init__(
        self,
        connector: ConnectorConfig,
        sim_config: SimulatorConfig | None = None,
    ) -> None:
        self._connector = connector
        self._sim = sim_config or SimulatorConfig()
        self._mock: respx.MockRouter | None = None
        # cursor_id → (full records list, next-page start offset)
        self._cursors: dict[str, tuple[list, int]] = {}
        # (datatype, "list") → cumulative request count for error injection
        self._req_counts: dict[tuple[str, str], int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Context-manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "GenericSimulator":
        self._mock = respx.mock(assert_all_called=False)
        self._mock.__enter__()
        self._register_all_routes()
        return self

    def __exit__(self, *args: object) -> None:
        if self._mock:
            self._mock.__exit__(*args)
        self._cursors.clear()
        self._req_counts.clear()

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_all_routes(self) -> None:
        assert self._mock is not None
        base = self._connector.connection.base_url.rstrip("/")

        # 1. Auth routes (OAuth2 token endpoint)
        self._register_auth_routes()

        # 2. List routes — group datatypes that share a path to detect shared-path APIs
        path_to_dts: dict[str, list[str]] = defaultdict(list)
        for dt_name, dt_cfg in self._connector.datatypes.items():
            if dt_cfg.ingestion is not None:
                path_to_dts[dt_cfg.ingestion.list.path].append(dt_name)

        for path, dt_names in path_to_dts.items():
            full_url = f"{base}{path}"
            if len(dt_names) == 1:
                dt_name = dt_names[0]
                sim_dt = self._sim.datatypes.get(dt_name, SimulatorDatatypeConfig())
                self._mock.get(full_url).mock(
                    side_effect=self._make_list_handler(dt_name, sim_dt)
                )
                if sim_dt.cursor_url_template:
                    self._register_cursor_url_route(base, dt_name, sim_dt)
            else:
                # Shared-path API — dispatch by route_discriminator
                dt_sim_pairs = [
                    (dt, self._sim.datatypes.get(dt, SimulatorDatatypeConfig()))
                    for dt in dt_names
                ]
                self._mock.get(full_url).mock(
                    side_effect=self._make_discriminated_handler(dt_sim_pairs)
                )
                for dt_name, sim_dt in dt_sim_pairs:
                    if sim_dt.cursor_url_template:
                        self._register_cursor_url_route(base, dt_name, sim_dt)

        # 3. Write routes from writeback config
        for dt_name, dt_cfg in self._connector.datatypes.items():
            if dt_cfg.writeback is None:
                continue
            sim_dt = self._sim.datatypes.get(dt_name, SimulatorDatatypeConfig())
            ops = dt_cfg.writeback.operations
            for action in ("lookup", "insert", "update", "delete", "archive", "upsert"):
                op_cfg = getattr(ops, action, None)
                if op_cfg is None:
                    continue
                method = op_cfg.method.upper()
                op_path = op_cfg.path
                if "{" in op_path:
                    # Convert path template to regex (e.g. /contacts/{id} → /contacts/[^/]+)
                    prefix = op_path[: op_path.index("{")]
                    pattern = re.compile(re.escape(f"{base}{prefix}") + r"[^/]+")
                    getattr(self._mock, method.lower())(pattern).mock(
                        side_effect=self._make_write_handler(dt_name, sim_dt, action)
                    )
                else:
                    getattr(self._mock, method.lower())(f"{base}{op_path}").mock(
                        side_effect=self._make_write_handler(dt_name, sim_dt, action)
                    )

        # 4. Extra routes (explicit overrides / additions)
        for route in self._sim.extra_routes:
            self._register_extra_route(base, route)

    def _register_auth_routes(self) -> None:
        assert self._mock is not None
        auth = self._connector.auth
        if getattr(auth, "type", None) == "oauth2":
            token_url: str = auth.oauth2.token_url
            token_resp = self._sim.auth.token_response
            self._mock.post(token_url).mock(
                return_value=httpx.Response(200, json=token_resp)
            )

    def _register_cursor_url_route(
        self,
        base: str,
        dt_name: str,
        sim_dt: SimulatorDatatypeConfig,
    ) -> None:
        assert self._mock is not None
        template = sim_dt.cursor_url_template
        if not template:
            return
        # Turn "/path/{cursor_id}" into a regex matching the full URL
        prefix = template[: template.index("{cursor_id}")]
        pattern = re.compile(re.escape(f"{base}{prefix}") + r"[A-Za-z0-9_-]+")
        self._mock.get(pattern).mock(
            side_effect=self._make_cursor_continuation_handler(dt_name)
        )

    def _register_extra_route(self, base: str, route: ExtraRoute) -> None:
        assert self._mock is not None
        method = route.method.upper()
        path = route.path
        http_method = getattr(self._mock, method.lower(), None)
        if http_method is None:
            return  # unsupported method; skip silently
        if path.startswith("^"):
            # Regex path: strip the leading ^ and prepend the escaped base URL
            pattern = re.compile(re.escape(base) + path[1:])
            http_method(pattern).mock(side_effect=self._make_extra_route_handler(route))
        else:
            http_method(f"{base}{path}").mock(
                side_effect=self._make_extra_route_handler(route)
            )

    # ------------------------------------------------------------------
    # List / pagination handlers
    # ------------------------------------------------------------------

    def _make_list_handler(self, dt_name: str, sim_dt: SimulatorDatatypeConfig):
        def handler(request: httpx.Request) -> httpx.Response:
            self._req_counts[(dt_name, "list")] += 1
            err = self._maybe_inject_error(dt_name, sim_dt)
            if err is not None:
                return err
            return self._paginate(request, dt_name, sim_dt)

        return handler

    def _make_discriminated_handler(
        self,
        dt_sim_pairs: list[tuple[str, SimulatorDatatypeConfig]],
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            for dt_name, sim_dt in dt_sim_pairs:
                disc = sim_dt.route_discriminator
                if disc is None:
                    continue
                param_val = request.url.params.get(disc.param, "")
                if re.search(disc.pattern, param_val, re.IGNORECASE):
                    self._req_counts[(dt_name, "list")] += 1
                    err = self._maybe_inject_error(dt_name, sim_dt)
                    if err is not None:
                        return err
                    return self._paginate(request, dt_name, sim_dt)
            return httpx.Response(
                400,
                json={"error": "No matching datatype discriminator for this request"},
            )

        return handler

    def _make_cursor_continuation_handler(self, dt_name: str):
        def handler(request: httpx.Request) -> httpx.Response:
            cursor_id = request.url.path.split("/")[-1]
            entry = self._cursors.get(cursor_id)
            if entry is None:
                return httpx.Response(404, json={"error": f"Cursor not found: {cursor_id}"})
            records, offset = entry
            sim_dt = self._sim.datatypes.get(dt_name, SimulatorDatatypeConfig())
            page_size = sim_dt.page_size or self._sim.default_page_size
            return self._build_page(records, offset, page_size, dt_name, sim_dt)

        return handler

    def _paginate(
        self,
        request: httpx.Request,
        dt_name: str,
        sim_dt: SimulatorDatatypeConfig,
    ) -> httpx.Response:
        ingestion = self._connector.datatypes[dt_name].ingestion
        assert ingestion is not None
        pagination = ingestion.list.pagination
        page_size = sim_dt.page_size or self._sim.default_page_size
        records = sim_dt.fixtures

        if pagination.strategy == PaginationStrategy.cursor:
            cursor_param = pagination.cursor.request_param
            raw = request.url.params.get(cursor_param) if cursor_param else None
            if raw and raw in self._cursors:
                _, offset = self._cursors[raw]
            elif raw and raw.lstrip("-").isdigit():
                offset = int(raw)
            else:
                offset = 0
            # Honour ?limit=N (or whatever page_size_param is) sent by the engine.
            # sim_dt.page_size is a test override that always takes precedence.
            if sim_dt.page_size is None and pagination.cursor.page_size_param is not None:
                raw_ps = request.url.params.get(pagination.cursor.page_size_param)
                if raw_ps and raw_ps.isdigit():
                    page_size = int(raw_ps)
        elif pagination.strategy == PaginationStrategy.offset:
            offset_cfg = pagination.offset or {}
            param = (
                offset_cfg.get("param", "offset")
                if isinstance(offset_cfg, dict)
                else "offset"
            )
            raw = request.url.params.get(param, "0")
            offset = int(raw) if str(raw).isdigit() else 0
        elif pagination.strategy == PaginationStrategy.page_number:
            pn_cfg = pagination.page_number or {}
            param = (
                pn_cfg.get("page_param", "page") if isinstance(pn_cfg, dict) else "page"
            )
            raw = request.url.params.get(param, "1")
            page_num = int(raw) if str(raw).isdigit() else 1
            offset = (page_num - 1) * page_size
        else:
            # link_header — always starts at offset 0
            offset = 0

        return self._build_page(records, offset, page_size, dt_name, sim_dt)

    def _build_page(
        self,
        records: list[dict],
        offset: int,
        page_size: int,
        dt_name: str,
        sim_dt: SimulatorDatatypeConfig,
    ) -> httpx.Response:
        ingestion = self._connector.datatypes[dt_name].ingestion
        assert ingestion is not None
        pagination = ingestion.list.pagination
        selector = ingestion.list.record_selector or "results"

        page = records[offset : offset + page_size]
        next_offset = offset + page_size
        has_more = next_offset < len(records)

        body: dict[str, Any] = {selector: page}

        # Inject configured envelope fields (e.g. totalSize, done)
        for key, token in sim_dt.response_envelope.items():
            body[key] = _resolve_envelope_value(token, len(records), has_more)

        if has_more:
            if pagination.strategy == PaginationStrategy.cursor:
                response_path = pagination.cursor.response_path
                if sim_dt.cursor_url_template:
                    cursor_id = f"cursor{next_offset:04d}"
                    self._cursors[cursor_id] = (records, next_offset)
                    next_val: Any = sim_dt.cursor_url_template.format(cursor_id=cursor_id)
                else:
                    next_val = str(next_offset)
                    self._cursors[next_val] = (records, next_offset)
                _set_path(body, response_path, next_val)

            elif pagination.strategy == PaginationStrategy.offset:
                offset_cfg = pagination.offset or {}
                if isinstance(offset_cfg, dict) and offset_cfg.get("total_path"):
                    _set_path(body, offset_cfg["total_path"], len(records))

            elif pagination.strategy == PaginationStrategy.page_number:
                pn_cfg = pagination.page_number or {}
                if isinstance(pn_cfg, dict) and pn_cfg.get("total_pages_path"):
                    total_pages = (len(records) + page_size - 1) // page_size
                    _set_path(body, pn_cfg["total_pages_path"], total_pages)

            elif pagination.strategy == PaginationStrategy.link_header:
                link_cfg = pagination.link_header or {}
                header_name = (
                    link_cfg.get("header", "Link") if isinstance(link_cfg, dict) else "Link"
                )
                return httpx.Response(
                    200,
                    json=body,
                    headers={header_name: f"<{ingestion.list.path}?after={next_offset}>; rel=\"next\""},
                )

        return httpx.Response(200, json=body)

    # ------------------------------------------------------------------
    # Write handler (for writeback-derived routes)
    # ------------------------------------------------------------------

    def _make_write_handler(
        self,
        dt_name: str,
        sim_dt: SimulatorDatatypeConfig,
        action: str,
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            if action == "delete":
                return httpx.Response(204)
            if action == "insert":
                return httpx.Response(201, json={"id": "sim-new-id"})
            # lookup, update, archive, upsert → 200
            return httpx.Response(200, json={})

        return handler

    # ------------------------------------------------------------------
    # Extra-route handler
    # ------------------------------------------------------------------

    def _make_extra_route_handler(self, route: ExtraRoute):
        def handler(request: httpx.Request) -> httpx.Response:
            if route.return_fixture_datatype is not None:
                record_id = request.url.path.split("/")[-1]
                sim_dt = self._sim.datatypes.get(
                    route.return_fixture_datatype, SimulatorDatatypeConfig()
                )
                pk = route.pk_field or "id"
                for rec in sim_dt.fixtures:
                    if str(rec.get(pk, "")) == record_id:
                        return httpx.Response(route.status_code, json=rec)
                return httpx.Response(route.not_found_status, json=route.not_found_body)

            body = dict(route.body_template)
            if body:
                return httpx.Response(route.status_code, json=body)
            return httpx.Response(route.status_code)

        return handler

    # ------------------------------------------------------------------
    # Error injection
    # ------------------------------------------------------------------

    def _maybe_inject_error(
        self,
        dt_name: str,
        sim_dt: SimulatorDatatypeConfig,
    ) -> httpx.Response | None:
        count = self._req_counts[(dt_name, "list")]
        for rule in sim_dt.errors:
            if rule.at_request_n is not None and count != rule.at_request_n:
                continue
            ingestion = self._connector.datatypes[dt_name].ingestion
            selector = (
                ingestion.list.record_selector if ingestion else None
            ) or "results"
            if rule.trigger == "empty_page":
                return httpx.Response(200, json={selector: []})
            if rule.trigger == "rate_limit":
                headers: dict[str, str] = {}
                if rule.retry_after:
                    headers["Retry-After"] = str(rule.retry_after)
                return httpx.Response(
                    rule.status_code or 429,
                    json={"error": "rate limited"},
                    headers=headers,
                )
            if rule.trigger == "server_error":
                return httpx.Response(
                    rule.status_code or 500, json={"error": "internal server error"}
                )
            if rule.trigger == "auth_error":
                return httpx.Response(
                    rule.status_code or 401, json={"error": "unauthorized"}
                )
        return None
