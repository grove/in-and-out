"""Unit tests for tool-level config models."""
from __future__ import annotations

import pytest
import yaml

from inandout.config.tool import (
    IngestionToolConfig,
    WritebackToolConfig,
)
from inandout.config._duration import parse_duration


# ---------------------------------------------------------------------------
# parse_duration tests
# ---------------------------------------------------------------------------

class TestParseDuration:
    def test_seconds(self):
        assert parse_duration("30s") == 30.0

    def test_minutes(self):
        assert parse_duration("5m") == 300.0

    def test_hours(self):
        assert parse_duration("1h") == 3600.0

    def test_days(self):
        assert parse_duration("90d") == 90 * 86400.0

    def test_float_value(self):
        assert parse_duration("1.5m") == 90.0

    def test_whitespace_stripped(self):
        assert parse_duration("  10s  ") == 10.0

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid duration"):
            parse_duration("10x")

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            parse_duration("")

    def test_no_unit_raises(self):
        with pytest.raises(ValueError):
            parse_duration("100")


# ---------------------------------------------------------------------------
# IngestionToolConfig tests
# ---------------------------------------------------------------------------

MINIMAL_INGESTION_YAML = """
database:
  dsn: "postgresql://user:pass@localhost:5432/mydb"
"""


class TestIngestionToolConfig:
    def test_minimal_valid(self):
        raw = yaml.safe_load(MINIMAL_INGESTION_YAML)
        cfg = IngestionToolConfig.model_validate(raw)
        assert cfg.database.dsn == "postgresql://user:pass@localhost:5432/mydb"

    def test_defaults_applied(self):
        raw = yaml.safe_load(MINIMAL_INGESTION_YAML)
        cfg = IngestionToolConfig.model_validate(raw)
        # database defaults
        assert cfg.database.max_open_conns == 20
        assert cfg.database.max_idle_conns == 5
        assert cfg.database.conn_max_lifetime == "30m"
        # webhook server defaults
        assert cfg.webhook_server.listen == "0.0.0.0:8443"
        # health server defaults
        assert cfg.health_server.listen == "0.0.0.0:9090"
        # shutdown default
        assert cfg.shutdown.drain_timeout == "30s"
        # control_table default
        assert cfg.control_table.poll_interval == "5s"
        # observability defaults
        assert cfg.observability.logging.format == "json"
        assert cfg.observability.logging.level == "info"
        assert cfg.observability.metrics.enabled is True
        assert cfg.observability.tracing.enabled is False

    def test_custom_values_parsed(self):
        raw = yaml.safe_load("""
database:
  dsn: "postgresql://u:p@host/db"
  max_open_conns: 50
  max_idle_conns: 10
  conn_max_lifetime: "1h"
connectors_dir: "/etc/connectors"
shutdown:
  drain_timeout: "60s"
control_table:
  poll_interval: "30s"
""")
        cfg = IngestionToolConfig.model_validate(raw)
        assert cfg.database.max_open_conns == 50
        assert cfg.database.max_idle_conns == 10
        assert cfg.database.conn_max_lifetime == "1h"
        assert cfg.connectors_dir == "/etc/connectors"
        assert cfg.shutdown.drain_timeout == "60s"
        assert cfg.control_table.poll_interval == "30s"

    def test_bad_duration_raises(self):
        raw = yaml.safe_load("""
database:
  dsn: "postgresql://u:p@host/db"
shutdown:
  drain_timeout: "10x"
""")
        with pytest.raises(Exception):
            IngestionToolConfig.model_validate(raw)

    def test_extra_fields_forbidden(self):
        raw = yaml.safe_load("""
database:
  dsn: "postgresql://u:p@host/db"
unknown_field: true
""")
        with pytest.raises(Exception):
            IngestionToolConfig.model_validate(raw)

    def test_defaults_config_has_scheduling(self):
        raw = yaml.safe_load(MINIMAL_INGESTION_YAML)
        cfg = IngestionToolConfig.model_validate(raw)
        assert cfg.defaults.scheduling.default_interval == "5m"

    def test_housekeeping_defaults(self):
        raw = yaml.safe_load(MINIMAL_INGESTION_YAML)
        cfg = IngestionToolConfig.model_validate(raw)
        assert cfg.housekeeping.interval == "1h"
        assert cfg.housekeeping.retention.sync_run_log == "90d"
        assert cfg.housekeeping.retention.dead_letter == "30d"
        assert cfg.housekeeping.retention.history_table == "365d"


# ---------------------------------------------------------------------------
# WritebackToolConfig tests
# ---------------------------------------------------------------------------

MINIMAL_WRITEBACK_YAML = """
database:
  dsn: "postgresql://user:pass@localhost:5432/mydb"
"""


class TestWritebackToolConfig:
    def test_minimal_valid(self):
        raw = yaml.safe_load(MINIMAL_WRITEBACK_YAML)
        cfg = WritebackToolConfig.model_validate(raw)
        assert cfg.database.dsn == "postgresql://user:pass@localhost:5432/mydb"

    def test_health_server_default_port_differs(self):
        raw = yaml.safe_load(MINIMAL_WRITEBACK_YAML)
        cfg = WritebackToolConfig.model_validate(raw)
        assert cfg.health_server.listen == "0.0.0.0:9091"

    def test_change_detection_defaults(self):
        raw = yaml.safe_load(MINIMAL_WRITEBACK_YAML)
        cfg = WritebackToolConfig.model_validate(raw)
        from inandout.config.tool import ChangeDetectionMode
        assert cfg.change_detection.mode == ChangeDetectionMode.logical_replication
        assert cfg.change_detection.replication_slot == "inout_writeback"
        assert cfg.change_detection.publication == "inout_desired_state"

    def test_change_detection_polling_mode(self):
        raw = yaml.safe_load("""
database:
  dsn: "postgresql://u:p@host/db"
change_detection:
  mode: polling
  poll_interval: "10s"
""")
        cfg = WritebackToolConfig.model_validate(raw)
        from inandout.config.tool import ChangeDetectionMode
        assert cfg.change_detection.mode == ChangeDetectionMode.polling
        assert cfg.change_detection.poll_interval == "10s"

    def test_defaults_match_ingestion(self):
        raw = yaml.safe_load(MINIMAL_WRITEBACK_YAML)
        cfg = WritebackToolConfig.model_validate(raw)
        assert cfg.shutdown.drain_timeout == "30s"
        assert cfg.control_table.poll_interval == "5s"
        assert cfg.defaults.retry.max_retries == 5
