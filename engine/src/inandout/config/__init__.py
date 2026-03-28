"""Pydantic models for connector and tool configuration.

Public API:
  ConnectorFileConfig  — top-level connector YAML document
  ConnectorConfig      — the nested connector object
  AuthConfig           — discriminated union of all auth schemes
  IngestionConfig      — ingestion datatype config
  WritebackConfig      — writeback datatype config
  load_connector()     — load + validate a connector YAML file
"""

from inandout.config.auth import (
    ApiKeyAuth,
    ApiKeyConfig,
    AuthConfig,
    CustomAuth,
    CustomConfig,
    JwtAuth,
    JwtConfig,
    OAuth2Auth,
    OAuth2Config,
)
from inandout.config.connector import (
    ConnectorConfig,
    ConnectorFileConfig,
    DatatypeConfig,
    DatatypeScopes,
    GenerationProfile,
)
from inandout.config.ingestion import (
    HistoryMode,
    IngestionConfig,
    IncrementalConfig,
    ListConfig,
    PrimaryKey,
    PrimaryKeyExpression,
    ScheduleConfig,
    WebhookEventsConfig,
)
from inandout.config.loader import load_connector, load_connector_from_string
from inandout.config.pagination import (
    CursorConfig,
    PaginationConfig,
    PaginationStrategy,
)
from inandout.config.webhooks import (
    FanOutConfig,
    SignatureConfig,
    WebhookConfig,
)
from inandout.config.writeback import (
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
    "DatatypeScopes",
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
