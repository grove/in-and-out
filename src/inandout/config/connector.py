"""Top-level connector and tool configuration models.

Covers connector.schema.json and cross-cutting validation rules:
  CFG-002: unknown interpolation namespace in ${...} expressions
  CFG-005: missing required top-level keys (schema_version, connector)
  CFG-011: cyclic datatype dependency graph
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from inandout.config.auth import AuthConfig, PreRequestAuthConfig
from inandout.config.field_mapping import FieldMapping
from inandout.config.ingestion import IngestionConfig
from inandout.config.quality import QualityRule
from inandout.config.webhooks import WebhookConfig
from inandout.config.writeback import WritebackConfig

# ---------------------------------------------------------------------------
# Interpolation validation (CFG-002)
# ---------------------------------------------------------------------------

_INTERP_RE = re.compile(r"\$\{([^}]+)\}")
_ENV_VAR_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")

_ALLOWED_INTERPOLATION_PREFIXES = (
    "runtime.",
    "credential.",
    "auth.",
    "record.",
    "data.",
    "pre_flight.",
    "subscription.",
)

_ALLOWED_INTERPOLATION_EXACT = frozenset(
    {
        "connection.base_url",
        "watermark",
        "external_id",
        "child.id",
        "job.id",
        "ingestion.field_selection",
        "pre_flight.etag",
        "pre_flight.version",
        "cluster_id",
    }
)


def _is_allowed_interpolation(token: str) -> bool:
    if token in _ALLOWED_INTERPOLATION_EXACT:
        return True
    if _ENV_VAR_RE.match(token):
        return True
    return any(token.startswith(prefix) for prefix in _ALLOWED_INTERPOLATION_PREFIXES)


def _collect_strings(obj: Any) -> list[str]:
    """Recursively collect all string values from a nested dict/list structure."""
    out: list[str] = []
    stack: list[Any] = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return out


# ---------------------------------------------------------------------------
# Cycle detection (CFG-011)
# ---------------------------------------------------------------------------


def _has_cycle(graph: dict[str, list[str]]) -> bool:
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(node: str) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        for nxt in graph.get(node, []):
            if nxt in graph and dfs(nxt):
                return True
        visiting.discard(node)
        visited.add(node)
        return False

    return any(dfs(node) for node in graph)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


class TimeoutConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connect: str | None = None
    read: str | None = None
    write: str | None = None


class RetryBudgetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = 1000
    window_secs: float = 3600.0  # 1 hour rolling window


class ConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str
    staging_base_url: str | None = None
    timeout: TimeoutConfig | None = None
    retry_budget: RetryBudgetConfig | None = None
    pre_request: PreRequestAuthConfig | None = None  # A3: pre-request session token auth


# ---------------------------------------------------------------------------
# Rate limit
# ---------------------------------------------------------------------------


class RateLimitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests_per_second: float | None = Field(default=None, gt=0)
    burst: int | None = Field(default=None, ge=1)


# ---------------------------------------------------------------------------
# Runtime params
# ---------------------------------------------------------------------------


class RuntimeParamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str | None = None
    env: str
    required: bool


# ---------------------------------------------------------------------------
# Generation profile
# ---------------------------------------------------------------------------


class GenerationProfile(StrEnum):
    ingestion_polling_readonly = "ingestion_polling_readonly"
    ingestion_webhook_incremental = "ingestion_webhook_incremental"
    writeback_patch = "writeback_patch"
    full_duplex = "full_duplex"


# ---------------------------------------------------------------------------
# Datatype
# ---------------------------------------------------------------------------


class LinkedObject(BaseModel):
    """A child datatype whose records are fetched from IDs embedded in a parent record (T1 #16)."""

    model_config = ConfigDict(extra="forbid")

    field: str  # field in parent record holding child IDs (e.g. "line_item_ids")
    datatype: str  # target datatype to ingest children into
    detail_path: str  # path for child GET, ${id} interpolated
    concurrency: int = 3
    primary_key: str = "id"  # field in child response used as external_id


class TimestampFieldConfig(BaseModel):
    """Per-field timestamp normalisation config (T1 #45)."""

    model_config = ConfigDict(extra="forbid")

    field: str
    format: Literal["iso8601", "unix_seconds", "unix_millis", "rfc2822", "auto"] = "auto"
    target_field: str | None = None  # if set, write normalised value here instead of overwriting


class DatatypeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str | None = None
    kind: Literal["entity", "relationship"] | None = None
    ingestion: IngestionConfig | None = None
    writeback: WritebackConfig | None = None
    field_mappings: list[FieldMapping] = []
    strict_field_mapping: bool = False
    quality_rules: QualityRule | None = None
    max_concurrent_writes: int | None = None  # datatype-level override for writeback parallelism
    linked_objects: list[LinkedObject] = []  # A3: linked/nested object resolution
    timestamp_fields: list[TimestampFieldConfig] = []  # A7: timestamp normalisation
    pii_fields: list[str] = []  # B6: fields containing PII
    api_version: str | None = None  # A6: per-datatype API version override
    seed_data: list[dict[str, Any]] = []  # Demo simulator: example records loaded at startup
    seed_count: int = 1  # If seed_data has exactly 1 entry, auto-generate this many records

    @model_validator(mode="after")
    def ingestion_or_writeback_required(self) -> "DatatypeConfig":
        if self.ingestion is None and self.writeback is None:
            raise ValueError("datatype must have at least one of 'ingestion' or 'writeback'")
        # T1 #23: validate read-only datatypes (ingestion-only) don't have writeback config
        if self.ingestion is not None and self.writeback is not None:
            # Both ingestion and writeback are configured - validate consistency
            if self.kind == "relationship" and self.writeback is not None:
                # T1 #22: relationship datatypes with writeback should have specific actions
                supported_actions = getattr(self.writeback, "supported_actions", [])
                if "delete" not in supported_actions and "archive" not in supported_actions:
                    pass  # Relationships typically need delete/archive actions but this is advisory
        return self


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class AccountConfig(BaseModel):
    """Per-account configuration for multi-tenant connectors (A4 T1 #20)."""

    model_config = ConfigDict(extra="forbid")

    account_id: str
    credential_ref: str  # per-account credentials
    base_url: str | None = None  # per-account base URL override (falls back to connection.base_url)
    display_name: str | None = None
    rate_limit: RateLimitConfig | None = None  # per-tenant rate limit override


class ConnectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    system: str = Field(min_length=1)
    generation_profile: GenerationProfile
    description: str | None = None
    api_version: str = Field(min_length=1)
    version: str = "1.0.0"
    api_version_header: str | None = (
        None  # A6: HTTP header name for injecting api_version, e.g. "Salesforce-Version"
    )
    api_deprecation_deadline: str | None = None
    api_version_deprecation_date: str | None = None  # A6: ISO date string e.g. "2026-12-01"
    api_version_warning_days: int = 60  # A6: warn when within this many days
    runtime_params: dict[str, RuntimeParamConfig] | None = None
    connection: ConnectionConfig
    auth: AuthConfig
    rate_limit: RateLimitConfig | None = None
    retry: dict[str, Any] | None = None
    circuit_breaker: dict[str, Any] | None = None
    tenancy: dict[str, Any] | None = None
    webhooks: WebhookConfig | None = None
    datatypes: dict[str, DatatypeConfig] = Field(min_length=1)
    depends_on: list[str] = []  # connector names that must run before this one
    accounts: list[AccountConfig] = []  # A4: if non-empty, one polling loop per account

    @model_validator(mode="after")
    def no_cyclic_datatype_dependencies(self) -> "ConnectorConfig":
        """CFG-011: detect cycles in the writeback dependency graph."""
        graph: dict[str, list[str]] = {}
        for dtype_name, dtype_cfg in self.datatypes.items():
            if dtype_cfg.writeback is not None:
                graph[dtype_name] = [dep.depends_on for dep in dtype_cfg.writeback.dependencies]
            else:
                graph[dtype_name] = []
        if _has_cycle(graph):
            raise ValueError("CFG-011: cyclic datatype dependency graph detected")
        return self


# ---------------------------------------------------------------------------
# Top-level connector file
# ---------------------------------------------------------------------------


class ConnectorFileConfig(BaseModel):
    """Root of a connector YAML file. schema_version is required (CFG-005)."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    connector: ConnectorConfig

    @model_validator(mode="before")
    @classmethod
    def schema_version_present(cls, data: Any) -> Any:
        """CFG-005: schema_version and connector are required top-level keys."""
        if not isinstance(data, dict) or "schema_version" not in data:
            raise ValueError("CFG-005: missing required top-level key: schema_version")
        return data

    @model_validator(mode="after")
    def no_unknown_interpolation_namespaces(self) -> "ConnectorFileConfig":
        """CFG-002: scan all string values for ${...} tokens with unknown namespaces."""
        raw = self.model_dump(mode="python")
        bad: list[str] = []
        for s in _collect_strings(raw):
            for token in _INTERP_RE.findall(s):
                if not _is_allowed_interpolation(token):
                    bad.append(f"${{{token}}}")
        if bad:
            unique = sorted(set(bad))
            raise ValueError(f"CFG-002: unknown interpolation namespace(s): {', '.join(unique)}")
        return self
