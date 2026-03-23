"""Tool-level configuration models for ingestion.yaml and writeback.yaml."""
from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from inandout.config._duration import parse_duration


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dsn: str
    max_open_conns: int = 20
    max_idle_conns: int = 5
    conn_max_lifetime: str = "30m"


class TLSConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    cert_file: str | None = None
    key_file: str | None = None


class WebhookServerRateLimitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests_per_second: float = 100
    burst: int = 200


class WebhookServerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    listen: str = "0.0.0.0:8443"
    tls: TLSConfig = Field(default_factory=TLSConfig)
    rate_limit: WebhookServerRateLimitConfig = Field(default_factory=WebhookServerRateLimitConfig)


class HealthServerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    listen: str = "0.0.0.0:9090"


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["json", "text"] = "json"
    level: Literal["debug", "info", "warn", "error"] = "info"


class MetricsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    listen: str = "0.0.0.0:9090"
    path: str = "/metrics"


class TracingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    otlp_endpoint: str | None = None
    sample_rate: float = Field(default=1.0, ge=0.0, le=1.0)


class ObservabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    tracing: TracingConfig = Field(default_factory=TracingConfig)


class ShutdownConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    drain_timeout: str = "30s"

    @field_validator("drain_timeout")
    @classmethod
    def validate_duration(cls, v: str) -> str:
        parse_duration(v)
        return v


class ControlTableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    poll_interval: str = "5s"

    @field_validator("poll_interval")
    @classmethod
    def validate_duration(cls, v: str) -> str:
        parse_duration(v)
        return v


class BackoffConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    initial: str = "1s"
    max: str = "60s"
    multiplier: float = 2.0
    jitter: bool = True


class RetryDefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_retries: int = 5
    backoff: BackoffConfig = Field(default_factory=BackoffConfig)


class RateLimitDefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests_per_second: float = 10.0
    burst: int = 20


class CircuitBreakerDefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error_threshold: int = 10
    pause_duration: str = "60s"
    probe_count: int = 1
    backoff_multiplier: float = 2.0
    max_pause_duration: str = "30m"


class SchedulingDefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_interval: str = "5m"


class BatchDefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_records: int = 50
    max_payload_bytes: int = 1_048_576
    max_wait: str = "5s"


class DefaultsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retry: RetryDefaultsConfig = Field(default_factory=RetryDefaultsConfig)
    rate_limit: RateLimitDefaultsConfig = Field(default_factory=RateLimitDefaultsConfig)
    circuit_breaker: CircuitBreakerDefaultsConfig = Field(default_factory=CircuitBreakerDefaultsConfig)
    scheduling: SchedulingDefaultsConfig = Field(default_factory=SchedulingDefaultsConfig)
    batch: BatchDefaultsConfig = Field(default_factory=BatchDefaultsConfig)


class RetentionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sync_run_log: str = "90d"
    dead_letter: str = "30d"
    history_table: str = "365d"


class HousekeepingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interval: str = "1h"
    retention: RetentionConfig = Field(default_factory=RetentionConfig)


class ChangeDetectionMode(StrEnum):
    logical_replication = "logical_replication"
    polling = "polling"


class ChangeDetectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: ChangeDetectionMode = ChangeDetectionMode.logical_replication
    replication_slot: str = "inout_writeback"
    publication: str = "inout_desired_state"
    lag_warning_threshold: str = "100MB"
    lag_max_threshold: str = "500MB"
    poll_interval: str = "5s"


class IngestionToolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: DatabaseConfig
    connectors_dir: str = "./connectors"
    namespace: str = "public"
    webhook_server: WebhookServerConfig = Field(default_factory=WebhookServerConfig)
    health_server: HealthServerConfig = Field(default_factory=HealthServerConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    shutdown: ShutdownConfig = Field(default_factory=ShutdownConfig)
    control_table: ControlTableConfig = Field(default_factory=ControlTableConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    housekeeping: HousekeepingConfig = Field(default_factory=HousekeepingConfig)


class _WritebackHealthServerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    listen: str = "0.0.0.0:9091"


class WritebackToolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: DatabaseConfig
    connectors_dir: str = "./connectors"
    namespace: str = "public"
    change_detection: ChangeDetectionConfig = Field(default_factory=ChangeDetectionConfig)
    health_server: _WritebackHealthServerConfig = Field(default_factory=_WritebackHealthServerConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    shutdown: ShutdownConfig = Field(default_factory=ShutdownConfig)
    control_table: ControlTableConfig = Field(default_factory=ControlTableConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    housekeeping: HousekeepingConfig = Field(default_factory=HousekeepingConfig)
