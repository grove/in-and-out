"""Declarative simulator configuration models.

A ``SimulatorConfig`` paired with a ``ConnectorConfig`` is sufficient to
instantiate a ``GenericSimulator`` — no per-system Python code is required.

Key concepts
------------
* ``SimulatorDatatypeConfig`` — fixture records + pagination hints per datatype.
* ``RouteDiscriminator`` — for APIs where multiple datatypes share one URL path
  (e.g. Salesforce SOQL ``/query?q=SELECT ... FROM Contact``).  A regex matched
  against a query-parameter value routes the request to the right datatype.
* ``cursor_url_template`` — for cursor-as-URL pagination (e.g. Salesforce
  ``nextRecordsUrl``).  Use ``{cursor_id}`` as the placeholder.
* ``response_envelope`` — inject computed fields into every page response using
  the magic values ``"${total_count}"``, ``"${done}"``, or ``"${has_more}"``.
  Common for APIs that wrap pages in ``{"totalSize": N, "done": false, ...}``.
* ``ExtraRoute`` — register arbitrary HTTP routes not derivable from the
  ``ConnectorConfig`` (e.g. detail-lookup GET, write endpoints when the
  connector factory has no writeback config).
* ``ErrorInjection`` — inject error responses at specific request counts to
  exercise circuit-breaker and retry logic.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class RouteDiscriminator(BaseModel):
    """Route a shared-path endpoint to a specific datatype via a query-param regex."""

    param: str
    """Query-parameter name to inspect (e.g. ``"q"`` for SOQL)."""

    pattern: str
    """Python ``re.search`` pattern applied to the param value (case-insensitive)."""


class ErrorInjection(BaseModel):
    """Inject an HTTP error at a specific point during simulation."""

    trigger: Literal["empty_page", "rate_limit", "server_error", "auth_error"]
    at_request_n: int = 1
    """1-indexed: trigger on the Nth request to this datatype's list endpoint."""

    status_code: int | None = None
    """Override the default status code (rate_limit→429, server_error→500, auth_error→401)."""

    retry_after: int | None = None
    """Seconds; added as ``Retry-After`` header on ``rate_limit`` responses."""


class ExtraRoute(BaseModel):
    """Declare an HTTP route not derivable from the ConnectorConfig alone.

    Use this for:
    * Detail-lookup GET endpoints (``GET /contacts/{id}``).
    * Write endpoints when the connector factory omits a writeback config.
    * Any other endpoint the test needs to exercise.
    """

    method: str
    """HTTP method: ``GET``, ``POST``, ``PATCH``, ``PUT``, ``DELETE``."""

    path: str
    """URL path.  Plain paths are matched exactly (``/crm/v3/objects/contacts``).
    Paths beginning with ``^`` are treated as regex applied against the full URL
    (e.g. ``^/crm/v3/objects/contacts/\\d+$``)."""

    status_code: int = 200

    body_template: dict[str, Any] = Field(default_factory=dict)
    """Static JSON body to return.  Ignored when ``return_fixture_datatype`` is set."""

    return_fixture_datatype: str | None = None
    """When set, look up the fixture record whose ``pk_field`` equals the last URL
    path segment and return it.  If not found, return ``not_found_status``."""

    pk_field: str | None = None
    """Fixture field to match against the path segment.  Defaults to ``"id"``."""

    not_found_status: int = 404
    not_found_body: dict[str, Any] = Field(
        default_factory=lambda: {"status": "error", "message": "Not found"}
    )


class SimulatorDatatypeConfig(BaseModel):
    """Per-datatype simulator configuration."""

    fixtures: list[dict[str, Any]] = Field(default_factory=list)
    """Records to serve from the list endpoint."""

    page_size: int | None = None
    """Overrides ``SimulatorConfig.default_page_size`` for this datatype."""

    route_discriminator: RouteDiscriminator | None = None
    """Required when multiple datatypes share a single list path."""

    cursor_url_template: str | None = None
    """For cursor-as-URL pagination.  Use ``{cursor_id}`` as the placeholder.
    Example: ``"/services/data/v59.0/query/{cursor_id}"``."""

    response_envelope: dict[str, Any] = Field(default_factory=dict)
    """Extra fields injected into every page response body.
    Supports magic string values:

    * ``"${total_count}"`` — total number of fixture records.
    * ``"${done}"``        — ``True`` when this is the last page.
    * ``"${has_more}"``    — ``True`` when more pages follow.

    Example for Salesforce-style wrapping::

        response_envelope = {"totalSize": "${total_count}", "done": "${done}"}
    """

    errors: list[ErrorInjection] = Field(default_factory=list)
    """Error-injection rules for testing retry / circuit-breaker logic."""


class SimulatorAuthConfig(BaseModel):
    """Configure the simulated OAuth2 token endpoint response."""

    token_response: dict[str, Any] = Field(
        default_factory=lambda: {
            "access_token": "sim_access_token",
            "token_type": "Bearer",
            "expires_in": 7200,
        }
    )


class SimulatorConfig(BaseModel):
    """Fully declarative simulator specification.

    Paired with a ``ConnectorConfig``, this is the sole input required to build
    a ``GenericSimulator`` — no per-system Python routing code is needed.
    """

    datatypes: dict[str, SimulatorDatatypeConfig] = Field(default_factory=dict)
    auth: SimulatorAuthConfig = Field(default_factory=SimulatorAuthConfig)
    default_page_size: int = 10

    extra_routes: list[ExtraRoute] = Field(default_factory=list)
    """Routes not derivable from the ConnectorConfig (detail lookups, write
    endpoints when the connector factory has no writeback config, etc.)."""
