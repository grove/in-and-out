# Getting Started

This guide walks you from zero to a running ingestion cycle using the bundled Docker Compose stack.

## What we'll build

By the end of this guide you will have:

1. A running PostgreSQL database with the in-and-out schema
2. A validated connector configuration
3. A successful dry-run fetch from an API
4. A running ingestion daemon writing records to PostgreSQL

## 1. Start the local stack

Using `just` (recommended):

```bash
just up-db        # Start PostgreSQL
just db-upgrade   # Apply all migrations
```

Or manually:

```bash
docker compose up -d postgres
export INOUT_DATABASE_URL="postgresql://inandout:inandout@localhost:5432/inandout"
inandout db upgrade
```

Verify the database is ready:

```bash
inandout db status
```

## 2. Write your first connector

Copy the bundled HubSpot example as a starting point:

```bash
cp connectors/hubspot.example.yaml connectors/my-connector.yaml
```

Edit the file to match your API. At minimum, change:

```yaml
schema_version: 1

connector:
  name: my-api                          # unique snake_case name
  system: My External API
  generation_profile: ingestion_polling_readonly  # read-only for now
  api_version: "v1"

  connection:
    base_url: "https://api.example.com"
    timeout:
      connect: "10s"
      read: "30s"

  auth:
    type: api_key
    credential_ref: my_api_key
    api_key:
      location: header
      name: "Authorization"

  rate_limit:
    requests_per_second: 5.0
    burst: 10

  datatypes:
    contacts:
      description: "Contact records"
      kind: entity

      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: "5m"
        list:
          method: GET
          path: "/v1/contacts"
          record_selector: "data"        # JMESPath to extract records from response
          pagination:
            strategy: offset
            offset:
              request_param: "offset"
              page_size_param: "limit"
              page_size: 100
            termination:
              - empty_results
```

Set your API credential:

```bash
export MY_API_KEY="your-api-key-here"
```

> **Security**: Never put credentials directly in YAML files. Always use `${ENV_VAR}` interpolation or a `credential_ref` that points to a secrets backend.

## 3. Validate the connector

```bash
inandout ingest validate-connector --connector connectors/my-connector.yaml
```

If validation fails, you'll see error codes like `CFG-001`, `CFG-002`, etc. with a description of what's wrong. Common issues:

| Code | Meaning | Fix |
|---|---|---|
| `CFG-001` | Missing required field | Add the missing field to your YAML |
| `CFG-002` | Invalid field value | Check allowed values in the schema |
| `CFG-003` | Profile mismatch | Ensure config matches `generation_profile` |

To skip connectivity checks (useful when the API isn't reachable from your machine):

```bash
inandout ingest validate-connector --connector connectors/my-connector.yaml --skip-connectivity
```

## 4. Dry-run (fetch without writing to DB)

```bash
inandout ingest dry-run \
  --connector connectors/my-connector.yaml \
  --datatype contacts \
  --limit 5
```

Dry-run fetches one page from the real API and shows you:
- The raw HTTP response
- Parsed records after applying `record_selector`
- Pagination state (cursor/offset for the next page)
- No writes to the database

This is the safest way to test a new connector against a live API.

### Using a staging endpoint

If your connector defines a `staging_base_url`, you can test against staging:

```bash
inandout ingest dry-run \
  --connector connectors/my-connector.yaml \
  --datatype contacts \
  --env staging
```

## 5. Start the ingestion daemon

Once dry-run looks correct, start the daemon:

```bash
inandout ingest run --config config/ingestion.yaml
```

The daemon will:
1. Load all connector YAML files from the configured `connectors_dir`
2. Start a polling loop for each connector/datatype pair
3. Begin syncing on each datatype's configured schedule

### What to look for in the logs

```
INFO  Starting sync run         connector=my-api datatype=contacts mode=full
INFO  Sync run completed        connector=my-api datatype=contacts records_fetched=150 records_inserted=150
INFO  Watermark saved           connector=my-api datatype=contacts watermark=2026-03-25T12:00:00Z
```

## 6. Verify the data in PostgreSQL

Connect to the database and query the source table:

```sql
-- Check the source table
SELECT external_id, data->>'name' AS name, _ingested_at
FROM inout_src_my_api_contacts
ORDER BY _ingested_at DESC
LIMIT 10;

-- Check the sync run log
SELECT id, connector, datatype, status, records_fetched, records_inserted,
       started_at, finished_at
FROM inout_ops_sync_run
WHERE connector = 'my-api'
ORDER BY started_at DESC
LIMIT 5;

-- Check the watermark
SELECT * FROM inout_ops_watermark
WHERE connector = 'my-api';
```

## 7. Next steps

You now have a working ingestion pipeline. Here's where to go next:

| Goal | Guide |
|---|---|
| Add more datatypes | [Connector Authoring Guide](./connector-authoring.md) — §Datatypes |
| Enable incremental sync | [Connector Authoring Guide](./connector-authoring.md) — §Incremental Sync |
| Set up writeback | [Connector Authoring Guide](./connector-authoring.md) — §Writeback Config |
| Tune daemon settings | [Configuration Reference](./configuration.md) |
| Understand the database schema | [Database & Migrations Guide](./database.md) |
| Operate in production | [CLI Reference](./cli.md) for available commands |
