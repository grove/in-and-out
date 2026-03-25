# Connector Authoring Guide

This guide teaches you to write correct, production-ready connector YAML files from scratch.

## What is a connector file?

A **connector** is a declarative YAML file that describes how in-and-out communicates with a specific external HTTP API. Each file lives in the `connectors/` directory and maps to one external system.

The connector declares HTTP mechanics only. The engine handles all orchestration: scheduling, retry, circuit breakers, watermarks, dead-letter queuing, observability, and schema management.

A connector contains:
- Connection details (base URL, timeouts)
- Authentication scheme
- Rate limiting and retry configuration
- One or more **datatypes** — each maps to one source table in PostgreSQL
- Per-datatype ingestion and/or writeback configuration

### Naming convention

```
connectors/{system-name}.yaml
```

The `connector.name` field must match the pattern `^[a-z][a-z0-9_-]*$`. This name is used in database table names (`inout_src_{name}_{datatype}`), so keep it short and lowercase.

## Generation profiles

Each connector declares a **generation profile** that determines which features are required and validated:

| Profile | When to use | Requires ingestion? | Requires writeback? |
|---|---|---|---|
| `ingestion_polling_readonly` | Read-only: poll an API, no writeback | Yes | No |
| `ingestion_webhook_incremental` | Real-time events + polling for full sync | Yes (+ webhooks) | No |
| `writeback_patch` | Push changes to an external API, no ingestion | No | Yes |
| `full_duplex` | Both ingestion and writeback | Yes | Yes |

Choose the simplest profile that matches your use case. You can upgrade later (e.g., from `ingestion_polling_readonly` to `full_duplex`).

## Connector YAML structure

Here's the full top-level structure:

```yaml
schema_version: 1

connector:
  name: my-api
  system: "My External API"
  description: "Brief description of what this connector does"
  generation_profile: full_duplex
  api_version: "v3"
  connector_version: "1.0.0"

  connection:
    base_url: "https://api.example.com"
    timeout:
      connect: "10s"
      read: "30s"
      write: "30s"

  auth:
    # ... authentication config

  rate_limit:
    requests_per_second: 10.0
    burst: 20

  retry:
    max_retries: 5
    backoff:
      initial: "1s"
      max: "60s"
      multiplier: 2.0
      jitter: true

  circuit_breaker:
    error_threshold: 10
    pause_duration: "60s"

  webhooks:
    # ... webhook config (if applicable)

  datatypes:
    contacts:
      # ... datatype config
```

### Required fields

| Field | Required | Description |
|---|---|---|
| `schema_version` | yes | Must be `1` |
| `connector.name` | yes | Unique identifier, `^[a-z][a-z0-9_-]*$` |
| `connector.system` | yes | Human-readable system name |
| `connector.generation_profile` | yes | One of the four profiles |
| `connector.api_version` | yes | API version string |
| `connector.connection.base_url` | yes | Base URL for all API calls |
| `connector.auth` | yes | Authentication configuration |
| `connector.datatypes` | yes | At least one datatype |

## Connection configuration

```yaml
connection:
  base_url: "https://api.example.com"
  timeout:
    connect: "10s"     # TCP connection timeout
    read: "30s"        # Response read timeout
    write: "30s"       # Request write timeout
```

All timeouts default to `30s` if not specified.

## Authentication

Every connector must declare an `auth` section. in-and-out supports five authentication schemes.

### API Key (header)

```yaml
auth:
  type: api_key
  credential_ref: my_api_key
  api_key:
    location: header
    name: "X-API-Key"
```

### API Key (query parameter)

```yaml
auth:
  type: api_key
  credential_ref: my_api_key
  api_key:
    location: query
    name: "api_key"
```

### OAuth2 (client credentials)

```yaml
auth:
  type: oauth2
  credential_ref: my_oauth_creds
  oauth2:
    grant_type: client_credentials
    token_url: "https://auth.example.com/oauth/token"
    scopes:
      - "read:contacts"
      - "write:contacts"
```

### JWT

```yaml
auth:
  type: jwt
  credential_ref: my_jwt_key
  jwt:
    algorithm: RS256
    issuer: "my-service"
    audience: "external-api"
    expiry: 3600
    claims:
      custom_claim: "value"
```

### Custom (multi-step authentication)

For APIs with non-standard auth flows:

```yaml
auth:
  type: custom
  credential_ref: my_custom_creds
  custom:
    steps:
      - method: POST
        url: "https://auth.example.com/login"
        body:
          username: "${credential.username}"
          password: "${credential.password}"
        extract:
          token: "response.body.access_token"
    inject:
      header: "Authorization"
      value: "Bearer ${token}"
```

### Credential referencing

The `credential_ref` field names a credential that is resolved at runtime — **never** embedded in the YAML file. The resolution order is:

1. Environment variable matching the credential ref name
2. Encrypted PostgreSQL column
3. HashiCorp Vault
4. AWS Secrets Manager
5. GCP Secret Manager

The secrets backend is configured in the tool config. Most deployments use environment variables or Kubernetes secrets.

> **Security rule**: Never embed actual secrets in connector YAML files. Use `credential_ref` for all sensitive values.

## Rate limiting, retry, and circuit breaker

### Rate limiting

```yaml
rate_limit:
  requests_per_second: 9.0     # Sustained rate
  burst: 20                     # Max burst above sustained rate
```

### Retry

```yaml
retry:
  max_retries: 5
  backoff:
    initial: "1s"              # First retry delay
    max: "60s"                 # Maximum delay cap
    multiplier: 2.0            # Exponential backoff factor
    jitter: true               # Add random jitter to prevent thundering herd
```

Retries apply to transient errors (5xx, timeouts, connection failures). Permanent errors (4xx except 429) are not retried.

### Circuit breaker

```yaml
circuit_breaker:
  error_threshold: 10          # Consecutive errors before opening
  pause_duration: "60s"        # How long to stay open before half-open probe
```

The circuit breaker prevents the daemon from hammering a failing API. States:

1. **Closed** — normal operation
2. **Open** — all requests short-circuited after threshold errors
3. **Half-open** — after `pause_duration`, one probe request is sent
4. If the probe succeeds → **Closed**; if it fails → **Open** again

Reset manually: `inandout control send --command reset-circuit-breaker --connector my-api`

## Datatypes

A datatype represents one type of record from the external system. Each datatype maps to a PostgreSQL table.

```yaml
datatypes:
  contacts:
    description: "CRM contact records"
    kind: entity                    # entity | relationship
    pii_fields:                     # Fields to redact in logs
      - email
      - phone
      - firstname

    ingestion:
      # ... ingestion config

    writeback:
      # ... writeback config (optional)
```

### Kind

| Kind | Use for | Example |
|---|---|---|
| `entity` | Primary records | contacts, companies, deals |
| `relationship` | Associations between entities | contact-to-company links |

### PII fields

Fields listed in `pii_fields` are redacted in structured logs (replaced with `[REDACTED]`). Data stored in `inout_src_*` tables is not masked — access controls are the operator's responsibility.

### Naming and table mapping

The datatype name becomes part of the PostgreSQL table name:

```
inout_src_{connector_name}_{datatype_name}
```

For example, `connector.name: hubspot` with datatype `contacts` creates `inout_src_hubspot_contacts`.

## Ingestion configuration

### Primary key

```yaml
ingestion:
  primary_key: id                  # Single field
  # or
  primary_key: [tenant_id, id]     # Composite key
  # or
  primary_key:
    expression: "data.id"          # JMESPath expression
```

### History mode

| Mode | Behaviour |
|---|---|
| `overwrite` | Source table always has the latest version of each record. Previous versions are overwritten. |
| `append` | Each ingested version is appended to a history table (`inout_src_*_history`) alongside the current-state table. |

### Schedule

```yaml
schedule:
  interval: "5m"           # Poll every 5 minutes
  # or
  cron: "*/10 * * * *"     # Cron expression (every 10 minutes)
```

### List endpoint

The `list` section defines how to fetch records:

```yaml
list:
  method: GET
  path: "/v3/objects/contacts"
  headers:                         # Additional headers (optional)
    Accept: "application/json"
  query_params:                    # Static query parameters (optional)
    properties: "email,firstname,lastname"
  record_selector: "results"       # JMESPath expression to extract records from response
  record_count_selector: "total"   # JMESPath for total record count (optional)
  pagination:
    # ... pagination config
  incremental:
    # ... incremental sync config
```

### Pagination

in-and-out supports four pagination strategies:

#### Cursor-based

```yaml
pagination:
  strategy: cursor
  cursor:
    response_path: "paging.next.after"    # Where to find the next cursor in the response
    request_param: "after"                 # Query parameter name for the cursor
  page_size_param: "limit"
  page_size: 100
  termination:
    - empty_results                        # Stop when no records returned
    - no_cursor                            # Stop when cursor is absent from response
```

#### Offset-based

```yaml
pagination:
  strategy: offset
  offset:
    request_param: "offset"
    page_size_param: "limit"
    page_size: 50
  termination:
    - empty_results
    - not_full_page                        # Stop when fewer records than page_size
```

#### Link header (RFC 5988)

```yaml
pagination:
  strategy: link_header
  termination:
    - no_next_link
```

#### Keyset

```yaml
pagination:
  strategy: keyset
  cursor:
    response_path: "next_key"
    request_param: "start_key"
  termination:
    - empty_results
    - no_cursor
```

### Incremental sync

Incremental sync uses a watermark (high-water mark) to fetch only records changed since the last sync:

```yaml
incremental:
  enabled: true
  cursor_field: "updatedAt"              # Field in each record holding the timestamp/cursor
  cursor_type: timestamp                  # timestamp | cursor | offset | sequence
  request_filter:
    mode: query_param                     # query_param | body_param | sort_filter
    param: "updatedAfter"
    value: "${watermark}"                 # Interpolated with the stored watermark value
```

The watermark is stored in `inout_ops_watermark` and updated atomically after each successful sync cycle. To force a full resync, reset the watermark:

```bash
inandout control send --command reset-watermark --connector my-api --datatype contacts
```

### Drift protection

Protects against pagination drift (records changing while paginating):

```yaml
list:
  drift_protection: true
  drift_max_shrink_pct: 50.0    # Alert if total records shrinks > 50% mid-pagination
```

### Fan-in (shared tables)

Multiple connectors can write to the same source table:

```yaml
ingestion:
  fan_in:
    shared_table: "all_contacts"
```

When fan-in is enabled, an additional `_connector` column is added to the shared table to discriminate records by source connector.

## Writeback configuration

The writeback section tells the daemon how to push changes back to the external API.

```yaml
writeback:
  protection_level: 2                    # 0 | 1 | 2 | 3
  conflict_resolution: dead_letter       # Strategy when conflicts are detected
  supported_actions:
    - insert
    - update
    - delete
  max_concurrent_writes: 10
  operations:
    lookup:
      method: GET
      path: "/v3/objects/contacts/${external_id}"
    insert:
      method: POST
      path: "/v3/objects/contacts"
    update:
      method: PATCH
      path: "/v3/objects/contacts/${external_id}"
      conditional_write:
        enabled: true
        header: "If-Match"
        value: "${pre_flight.etag}"
    delete:
      method: DELETE
      path: "/v3/objects/contacts/${external_id}"
```

### Protection levels

| Level | Name | Mechanism | Trade-off |
|---|---|---|---|
| 0 | None | No conflict detection | Fastest; no safety |
| 1 | Conditional writes | ETag / `If-Match` header | Closes TOCTOU fully; requires API support |
| 2 | Optimistic | Pre-flight read + 3-way compare | Millisecond residual TOCTOU window |
| 3 | Post-write verify | Read-after-write confirmation | Safest; doubles API call count |

### Conflict resolution strategies

| Strategy | Behaviour |
|---|---|
| `dead_letter` | Route to dead-letter queue with full context (default) |
| `last_writer_wins` | Overwrite regardless of conflict |
| `skip_and_warn` | Discard the write, log a warning |
| `re_ingest_and_recompute` | Signal re-ingestion; MDM recomputes desired state |
| `server_wins` | Accept the target system's current state as authoritative |
| `merge_fields` | Field-level merge of non-conflicting fields |

### Operations

Every writeback datatype must define a `lookup` operation (for pre-flight reads). Other operations are optional depending on `supported_actions`:

| Operation | HTTP method | When used |
|---|---|---|
| `lookup` | GET | Pre-flight read before every write (required) |
| `insert` | POST | When `action = "insert"` |
| `update` | PATCH/PUT | When `action = "update"` |
| `delete` | DELETE | When `action = "delete"` |
| `archive` | POST | When `action = "archive"` |
| `upsert` | PUT/POST | When `action = "upsert"` (combined insert/update) |

### Variable interpolation

Operation paths and headers support variable interpolation:

| Variable | Available in | Description |
|---|---|---|
| `${external_id}` | All operations | Source system record ID |
| `${watermark}` | Ingestion list | Current watermark value |
| `${pre_flight.etag}` | Update/delete | ETag from the lookup response |
| `${credential.*}` | Auth steps | Credential fields |

## Webhook configuration

For `ingestion_webhook_incremental` and `full_duplex` profiles:

```yaml
webhooks:
  path: /webhooks/my-api                 # URL path for inbound webhooks
  signature:
    algorithm: hmac-sha256
    header: "X-Signature"
    credential_ref: my_webhook_secret
  fan_out:
    discriminator: "eventType"           # JMESPath to extract event type from body
    unmatched: log_and_discard           # log_and_discard | reject_400
    routes:
      - match: "contact.created"
        datatype: contacts
      - match: "contact.updated"
        datatype: contacts
      - match: "company.updated"
        datatype: companies
```

### Signature verification

**Required for production.** Invalid signatures are rejected with HTTP 401 and logged in `inout_ops_webhook_log`.

Supported algorithms: `hmac-sha256`, `hmac-sha1`, `rsa-sha256`.

### Event routing

The `discriminator` is a JMESPath expression evaluated on the request body to extract the event type. The `routes` list maps event types to datatypes. Use `unmatched: log_and_discard` to safely ignore unknown event types.

## Validation and linting

Always validate before deploying:

```bash
# Validate a single connector
inandout ingest validate-connector --connector connectors/my-api.yaml

# Lint all connectors in a directory
inandout lint --connectors-dir connectors/

# Validate with connectivity check (probes the base_url)
inandout ingest validate-connector --connector connectors/my-api.yaml --check-connectivity
```

Validation checks include:
- YAML schema compliance
- Required fields for the declared generation profile
- `credential_ref` names are resolvable
- Logical consistency (e.g., protection_level=1 requires conditional_write.enabled=true)
- Pagination termination conditions are defined

## Testing a connector locally

### Dry-run (no database writes)

```bash
inandout ingest dry-run \
  --connector connectors/my-api.yaml \
  --datatype contacts \
  --limit 10
```

### Using simulators for CI

The project includes HTTP simulators (built on `respx`) that mimic external API behaviour without network calls. Located in `tests/simulators/`, they're ideal for CI pipelines.

## Full annotated example

Here's a complete `full_duplex` connector with all major features:

```yaml
schema_version: 1

connector:
  name: hubspot
  system: HubSpot CRM
  generation_profile: full_duplex
  description: "HubSpot CRM contacts — full-duplex sync"
  api_version: "v3"
  connector_version: "1.0.0"

  connection:
    base_url: https://api.hubapi.com
    timeout:
      connect: "10s"
      read: "30s"
      write: "30s"

  auth:
    type: oauth2
    credential_ref: hubspot_oauth
    oauth2:
      grant_type: client_credentials
      token_url: https://api.hubapi.com/oauth/v1/token
      scopes:
        - crm.objects.contacts.read
        - crm.objects.contacts.write

  rate_limit:
    requests_per_second: 9.0
    burst: 20

  retry:
    max_retries: 5
    backoff:
      initial: "1s"
      max: "60s"
      multiplier: 2.0
      jitter: true

  circuit_breaker:
    error_threshold: 10
    pause_duration: "60s"

  webhooks:
    path: /webhooks/hubspot
    signature:
      algorithm: hmac-sha256
      header: X-HubSpot-Signature-V3
      credential_ref: hubspot_webhook_secret
    fan_out:
      discriminator: objectType
      unmatched: log_and_discard
      routes:
        - match: contact.creation
          datatype: contacts
        - match: contact.propertyChange
          datatype: contacts

  datatypes:
    contacts:
      description: "HubSpot CRM contacts"
      kind: entity
      pii_fields: [email, phone, firstname, lastname]

      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: "5m"
        list:
          method: GET
          path: /crm/v3/objects/contacts
          record_selector: results
          pagination:
            strategy: cursor
            cursor:
              request_param: after
              response_path: "paging.next.after"
            termination: [empty_page]
          incremental:
            enabled: true
            cursor_field: lastmodifieddate
            cursor_type: timestamp
            request_filter:
              mode: query_param
              param: lastmodifieddate__gt
              value: "${watermark}"

      writeback:
        protection_level: 2
        conflict_resolution: last_writer_wins
        supported_actions: [update]
        operations:
          lookup:
            method: GET
            path: /crm/v3/objects/contacts/${external_id}
          update:
            method: PATCH
            path: /crm/v3/objects/contacts/${external_id}
```
