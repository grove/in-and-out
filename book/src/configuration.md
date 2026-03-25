# Configuration Reference

This is the complete reference for the tool configuration files: `config/ingestion.yaml` and `config/writeback.yaml`. These files control daemon-level settings — not connector-specific HTTP mechanics (see the [Connector Authoring Guide](./connector-authoring.md) for those).

## File locations and loading order

The daemon loads its tool config from, in order of precedence:

1. The `--config` / `-c` CLI flag
2. The `INOUT_CONFIG_PATH` environment variable
3. The default path: `config/ingestion.yaml` (ingestion) or `config/writeback.yaml` (writeback)

## Ingestion tool config

Full annotated example of `config/ingestion.yaml`:

```yaml
database:
  dsn: "${INOUT_DATABASE_URL}"           # PostgreSQL connection string (required)

connectors_dir: /connectors              # Where connector YAML files are loaded from

health_server:
  listen: "0.0.0.0:9090"                # Health/readiness + metrics endpoint

observability:
  logging:
    format: json                         # json | text
    level: info                          # debug | info | warning | error
  tracing:
    enabled: false                       # Enable OpenTelemetry tracing
    otlp_endpoint: ""                    # OTLP collector endpoint (e.g. http://localhost:4317)
    sample_rate: 1.0                     # Trace sampling rate (0.0 – 1.0)

defaults:
  scheduling:
    default_interval: 5m                 # Default poll interval if not set per-datatype

housekeeping:
  interval: "1h"                         # How often the cleanup loop runs
  retention:
    sync_run_log: "90d"                  # inout_ops_sync_run rows
    dead_letter: "30d"                   # inout_dl_ingestion_* rows
    history_table: "365d"                # inout_src_*_history rows
    webhook_route_seq: "7d"              # inout_ops_webhook_route_seq rows
    writeback_result: "30d"              # inout_ops_writeback_result rows
    writeback_dead_letter: "30d"         # inout_dl_writeback_* rows
```

## Writeback tool config

`config/writeback.yaml` shares the same schema:

```yaml
database:
  dsn: "${INOUT_DATABASE_URL}"

connectors_dir: /connectors

health_server:
  listen: "0.0.0.0:9090"

observability:
  logging:
    format: json
    level: info
  tracing:
    enabled: false

housekeeping:
  interval: "1h"
  retention:
    sync_run_log: "90d"
    dead_letter: "30d"
    history_table: "365d"
    webhook_route_seq: "7d"
    writeback_result: "30d"
    writeback_dead_letter: "30d"
```

## Field reference

### `database`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `dsn` | string | yes | — | PostgreSQL connection string. Use `${INOUT_DATABASE_URL}` to inject from env. |

### `connectors_dir`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `connectors_dir` | string | no | `/connectors` | Directory where connector YAML files are loaded from. All `*.yaml` files in this directory are loaded. |

### `health_server`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `listen` | string | no | `0.0.0.0:9090` | Bind address for the health/readiness HTTP server and Prometheus metrics endpoint. |

Endpoints served:

| Path | Purpose |
|---|---|
| `GET /health` | Liveness probe — always returns `{"status": "ok"}` |
| `GET /ready` | Readiness probe — returns `{"status": "ready"}` or `{"status": "draining"}` |
| `GET /metrics` | Prometheus metrics in text exposition format |

### `observability.logging`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `format` | string | no | `json` | Log output format: `json` (structured, for production) or `text` (human-readable, for development). |
| `level` | string | no | `info` | Minimum log level: `debug`, `info`, `warning`, `error`. |

Logs are emitted via [structlog](https://www.structlog.org/) and include contextual fields:

| Field | Example | Description |
|---|---|---|
| `connector` | `hubspot` | Connector being processed |
| `datatype` | `contacts` | Datatype being processed |
| `sync_run_id` | `a1b2c3d4-...` | Unique run identifier |
| `action` | `upsert` | Current operation |
| `duration_ms` | `1234` | Operation duration |
| `record_count` | `150` | Number of records processed |

### `observability.tracing`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `enabled` | bool | no | `false` | Enable OpenTelemetry distributed tracing. |
| `otlp_endpoint` | string | no | `""` | OTLP collector gRPC endpoint. Example: `http://localhost:4317`. |
| `sample_rate` | float | no | `1.0` | Fraction of traces to sample (0.0 = none, 1.0 = all). |

When enabled, the following operations are instrumented:
- HTTP calls (via `httpx` instrumentation)
- Database queries (via `psycopg` instrumentation)
- Sync run lifecycle (start → pages → commit → finish)

### `defaults.scheduling`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `default_interval` | duration | no | `5m` | Default poll interval for datatypes that don't specify their own `schedule.interval`. |

### `housekeeping`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `interval` | duration | no | `1h` | How often the housekeeping loop runs. |
| `retention.sync_run_log` | duration | no | `90d` | Retention window for `inout_ops_sync_run` rows. |
| `retention.dead_letter` | duration | no | `30d` | Retention for `inout_dl_ingestion_*` rows. |
| `retention.history_table` | duration | no | `365d` | Retention for `inout_src_*_history` rows. |
| `retention.webhook_route_seq` | duration | no | `7d` | Retention for `inout_ops_webhook_route_seq` rows. |
| `retention.writeback_result` | duration | no | `30d` | Retention for `inout_ops_writeback_result` rows. |
| `retention.writeback_dead_letter` | duration | no | `30d` | Retention for `inout_dl_writeback_*` rows. |

> **Note**: `writeback_result` rows from the last 24 hours are always preserved regardless of the configured retention window. This anchors crash-recovery deduplication.

### Duration format

Duration values use a human-readable format:

| Example | Meaning |
|---|---|
| `30s` | 30 seconds |
| `5m` | 5 minutes |
| `1h` | 1 hour |
| `7d` | 7 days |
| `90d` | 90 days |
| `365d` | 365 days |

## Environment variable substitution

Any value in the tool config can use `${ENV_VAR}` syntax to inject environment variables at load time. This is the recommended approach for secrets:

```yaml
database:
  dsn: "${INOUT_DATABASE_URL}"          # Resolved from environment
```

## Secrets and sensitive values

**Rule**: Never put passwords, API keys, or tokens in config files that are committed to version control.

- Database credentials: use `${INOUT_DATABASE_URL}` environment variable
- API credentials: use `credential_ref` in connector YAML, resolved at runtime via the configured secrets backend
- Webhook secrets: use `credential_ref` in webhook signature config

See the [Connector Authoring Guide](./connector-authoring.md) for details on credential referencing.
