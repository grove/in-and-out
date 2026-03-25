# CLI Reference

Complete command, flag, and output reference for the `inandout` CLI.

## Global flags

| Flag | Description |
|---|---|
| `--help` | Show help for any command or subcommand |
| `--version` | Print the installed version |

## `inandout version`

Print the installed version of in-and-out.

```bash
inandout version
```

---

## `inandout ingest`

Ingestion daemon commands.

### `inandout ingest run`

Start the ingestion daemon. This is a long-running blocking process.

```bash
inandout ingest run --config config/ingestion.yaml
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `config/ingestion.yaml` | Path to the ingestion tool config YAML |

The daemon:
1. Loads all connector YAML files from `connectors_dir`
2. Starts a polling loop per connector/datatype pair
3. Starts the webhook HTTP server (if any connector uses webhooks)
4. Starts the health/metrics endpoint
5. Runs until SIGTERM/SIGINT

### `inandout ingest validate`

Validate all connector YAML files in a directory.

```bash
inandout ingest validate --connectors-dir connectors/
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--connectors-dir` | `-d` | `connectors/` | Directory containing connector YAML files |
| `--strict` | | `false` | Exit 1 on any warning (not just errors) |

### `inandout ingest validate-connector`

Validate a single connector YAML file against the Pydantic schema.

```bash
inandout ingest validate-connector --connector connectors/hubspot.yaml
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Path to the connector YAML file |
| `--check-connectivity` | `true` | Probe the `base_url` with an HTTP GET |
| `--skip-connectivity` | `false` | Skip the connectivity check |

### `inandout ingest dry-run`

Fetch one page from a real API and preview records — no database writes.

```bash
inandout ingest dry-run \
  --connector connectors/hubspot.yaml \
  --datatype contacts \
  --limit 5
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Path to connector YAML file |
| `--datatype` | all | Datatype to test (omit to test all) |
| `--limit` | `10` | Maximum records to preview |
| `--env` | `production` | Environment: `production` or `staging` |

Output includes:
- Raw HTTP response body
- Parsed records after applying `record_selector`
- Pagination state (cursor/offset for next page)

---

## `inandout writeback`

Writeback daemon commands.

### `inandout writeback run`

Start the writeback daemon. Long-running blocking process.

```bash
inandout writeback run --config config/writeback.yaml
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `config/writeback.yaml` | Path to the writeback tool config YAML |

### `inandout writeback validate-connector`

Validate writeback configuration for a connector.

```bash
inandout writeback validate-connector --connector connectors/hubspot.yaml
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Path to connector YAML file |
| `--datatype` | all | Validate only this datatype |

Reports the effective write-anomaly protection level for each datatype.

### `inandout writeback dry-run`

Preview a writeback cycle without issuing HTTP writes.

```bash
inandout writeback dry-run \
  --connector connectors/hubspot.yaml \
  --datatype contacts \
  --limit 20
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Path to connector YAML file |
| `--datatype` | all | Datatype to preview |
| `--limit` | `20` | Maximum delta rows to preview |

---

## `inandout db`

Database migration commands. Powered by Alembic.

### `inandout db upgrade`

Apply pending migrations up to the target revision.

```bash
inandout db upgrade                     # Apply all pending (up to head)
inandout db upgrade --config config/ingestion.yaml
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `config/ingestion.yaml` | Tool config YAML (used for database URL) |
| *positional* | | `head` | Target Alembic revision (default: latest) |

> **Important**: Always run `db upgrade` before starting daemons after an update. Daemons check the schema version at startup and refuse to start if the database is behind.

### `inandout db downgrade`

Roll back migrations to a target revision.

```bash
inandout db downgrade -1                # Roll back one step
inandout db downgrade abc123            # Roll back to a specific revision
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `config/ingestion.yaml` | Tool config YAML |
| *positional* | | *(required)* | Target revision (e.g., `-1` or a revision ID) |

> **Warning**: Some rollbacks are destructive and may cause data loss. Always back up the database before downgrading.

### `inandout db status`

Show the current migration status.

```bash
inandout db status
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `config/ingestion.yaml` | Tool config YAML |

---

## `inandout connector`

Connector management commands.

### `inandout connector status`

Show deployed connector versions from the database.

```bash
inandout connector status
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--config` | `-c` | `config/ingestion.yaml` | Tool config YAML |

### `inandout connector test`

Run automated tests against a connector YAML file.

```bash
inandout connector test --connector connectors/hubspot.yaml
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Path to connector YAML file |
| `--output` | `text` | Output format: `text` or `junit` |
| `--output-file` | stdout | Path to write results |

---

## `inandout webhook`

Webhook management commands.

### `inandout webhook replay`

Replay webhook events from the audit log.

```bash
inandout webhook replay \
  --connector hubspot \
  --datatype contacts \
  --since 1h
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Connector name |
| `--datatype` | *(required)* | Datatype name |
| `--since` | `1h` | Time window (e.g., `1h`, `30m`, `7d`) |
| `--limit` | `100` | Maximum events to replay |
| `--config` / `-c` | `config/ingestion.yaml` | Tool config YAML |

---

## `inandout control`

Runtime control commands. These insert operator commands into `inout_ops_control` for the running daemons to pick up.

### `inandout control send`

Send a control command to running daemons.

```bash
inandout control send \
  --command pause_connector \
  --connector hubspot
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--command` | `-c` | *(required)* | Command to send (see table below) |
| `--connector` | | | Target connector name |
| `--datatype` | | | Target datatype |
| `--payload` | | | JSON payload for the command |
| `--target-tool` | | | `ingestion` or `writeback` |
| `--dsn` | | `$INOUT_DATABASE_URL` | PostgreSQL connection string |
| `--issued-by` | | `cli` | Operator identifier for audit trail |

#### Available commands

| Command | Description |
|---|---|
| `pause_connector` | Pause syncing for a connector |
| `resume_connector` | Resume a paused connector |
| `force_full_sync` | Reset watermark and trigger full resync |
| `reset-watermark` | Reset the watermark for a connector/datatype |
| `reload-config` | Reload connector configuration from disk |
| `reset-circuit-breaker` | Reset a tripped circuit breaker |
| `resync` | Trigger an immediate sync cycle |
| `trigger-writeback` | Trigger an immediate writeback cycle |
| `requeue_dead_letter` | Replay all dead-letter entries |
| `validate` | Run config validation in the running daemon |
| `drain` | Gracefully stop processing (complete in-flight work) |

### `inandout control list`

List recent control table entries.

```bash
inandout control list --status pending --limit 10
```

| Flag | Default | Description |
|---|---|---|
| `--status` | all | Filter: `pending`, `acknowledged`, `completed`, `failed` |
| `--connector` | all | Filter by connector name |
| `--limit` | `20` | Maximum rows to return |
| `--dsn` | `$INOUT_DATABASE_URL` | PostgreSQL connection string |

---

## `inandout dead-letter`

Dead-letter queue inspection and management.

### `inandout dead-letter inspect`

Inspect ingestion dead-letter entries.

```bash
inandout dead-letter inspect \
  --connector hubspot \
  --datatype contacts
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Connector name |
| `--datatype` | *(required)* | Datatype name |
| `--config` / `-c` | `config/ingestion.yaml` | Tool config YAML |
| `--limit` | `20` | Maximum rows to display |

### `inandout dead-letter writeback-inspect`

Inspect writeback dead-letter entries.

```bash
inandout dead-letter writeback-inspect \
  --connector hubspot \
  --datatype contacts
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Connector name |
| `--datatype` | *(required)* | Datatype name |
| `--config` / `-c` | `config/writeback.yaml` | Tool config YAML |
| `--limit` | `20` | Maximum rows to display |

### `inandout dead-letter writeback-replay`

Replay writeback dead-letter entries.

```bash
inandout dead-letter writeback-replay \
  --connector hubspot \
  --datatype contacts \
  --limit 50
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Connector name |
| `--datatype` | *(required)* | Datatype name |
| `--config` / `-c` | `config/writeback.yaml` | Tool config YAML |
| `--limit` | `50` | Maximum rows to replay |
| `--dry-run` | `false` | Preview what would be replayed without requeuing |

### `inandout dead-letter transform`

Apply a Python transform script to dead-letter entries before replaying.

```bash
inandout dead-letter transform \
  --connector hubspot \
  --datatype contacts \
  --script fix_payloads.py \
  --dry-run
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | *(required)* | Connector name |
| `--datatype` | *(required)* | Datatype name |
| `--script` | *(required)* | Path to Python transform script |
| `--config` / `-c` | `config/ingestion.yaml` | Tool config YAML |
| `--dry-run` | `false` | Show results without writing |

---

## `inandout lint`

Static analysis on connector YAML files.

```bash
inandout lint --connectors-dir connectors/
inandout lint --connector connectors/hubspot.yaml
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--connectors-dir` | `-d` | `connectors/` | Directory to lint |
| `--connector` | | | Single file to lint (overrides `--connectors-dir`) |

---

## `inandout api`

API specification and SDK generation.

### `inandout api spec`

Dump the OpenAPI specification as JSON.

```bash
inandout api spec
inandout api spec --output openapi.json
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--output` | `-o` | stdout | Path to write the OpenAPI JSON |

### `inandout api generate-sdk`

Generate a client SDK from the OpenAPI specification.

```bash
inandout api generate-sdk --lang python --output ./sdk/
```

| Flag | Default | Description |
|---|---|---|
| `--lang` | *(required)* | Target language: `python`, `typescript`, `go` |
| `--output` | *(required)* | Output directory |
| `--config` | | Path to openapi-generator config file |

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | General error |
| `2` | Configuration error (invalid YAML, missing required fields) |
