"""inandout.schema — the engine-agnostic connector schema layer.

This package re-exports every public symbol from ``inandout.config`` so that
the simulator (and any alternative engine implementation) can depend on schema
models without importing engine runtime code.

Engine code continues to import from ``inandout.config``; the simulator imports
exclusively from ``inandout.schema``.  When the two are eventually split into
separate Python packages, only the ``inandout.schema`` import paths need to be
kept stable.
"""

from inandout.config import (
    # auth
    ApiKeyAuth,
    ApiKeyConfig,
    AuthConfig,
    CustomAuth,
    CustomConfig,
    JwtAuth,
    JwtConfig,
    OAuth2Auth,
    OAuth2Config,
    # connector
    ConnectorConfig,
    ConnectorFileConfig,
    DatatypeConfig,
    GenerationProfile,
    # ingestion
    HistoryMode,
    IngestionConfig,
    IncrementalConfig,
    ListConfig,
    PrimaryKey,
    PrimaryKeyExpression,
    ScheduleConfig,
    WebhookEventsConfig,
    # loader
    load_connector,
    load_connector_from_string,
    # pagination
    CursorConfig,
    PaginationConfig,
    PaginationStrategy,
    # webhooks
    FanOutConfig,
    SignatureConfig,
    WebhookConfig,
    # writeback
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)

__all__ = [
    # auth
    "ApiKeyAuth",
    "ApiKeyConfig",
    "AuthConfig",
    "CustomAuth",
    "CustomConfig",
    "JwtAuth",
    "JwtConfig",
    "OAuth2Auth",
    "OAuth2Config",
    # connector
    "ConnectorConfig",
    "ConnectorFileConfig",
    "DatatypeConfig",
    "GenerationProfile",
    # ingestion
    "HistoryMode",
    "IngestionConfig",
    "IncrementalConfig",
    "ListConfig",
    "PrimaryKey",
    "PrimaryKeyExpression",
    "ScheduleConfig",
    "WebhookEventsConfig",
    # loader
    "load_connector",
    "load_connector_from_string",
    # pagination
    "CursorConfig",
    "PaginationConfig",
    "PaginationStrategy",
    # webhooks
    "FanOutConfig",
    "SignatureConfig",
    "WebhookConfig",
    # writeback
    "ConflictResolution",
    "OperationConfig",
    "OperationsConfig",
    "ProtectionLevel",
    "UpdateOperationConfig",
    "WritebackConfig",
]
