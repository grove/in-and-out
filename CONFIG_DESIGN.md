# Configuration Design

## Architecture Context: OSI-Mapping Integration

**As of March 2026**, this configuration design reflects a simplified scope: In-and-Out focuses on **HTTP API specification and mechanics**. Identity resolution, consolidation mapping, and multi-source conflict strategies are declared in **OSI-Mapping's YAML config** (separate from in-and-out).

**Configuration Layers:**
1. **OSI-Mapping Config** (`osi/consolidation.yaml`): Declares sources, targets, field mappings, identity rules, conflict strategies. OSI's delta views (`_delta_{mapping}`) already produce action-classified, per-source output with `_cluster_id` and `_base` — essentially the desired-state source.
2. **In-and-Out Config** (`connectors/*.yaml`): Declares HTTP endpoints, auth, pagination, field selection, writeback operations, and `transform.template` for reshaping desired-state data into target API payloads.
3. **Optional business-filter query** (SQL view / dbt model): Thin `WHERE` clause on OSI delta views for app-specific filtering (e.g., "only sync contacts where email IS NOT NULL"). Not a separate architectural layer — just a query.

This document specifies the In-and-Out connector configuration schema.

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [File Organization](#2-file-organization)
3. [Tool Configuration](#3-tool-configuration)
4. [Connector Configuration](#4-connector-configuration)
5. [Expression & Path Language](#5-expression--path-language)
6. [Variable Interpolation](#6-variable-interpolation)
7. [Concrete Example: HubSpot Connector](#7-concrete-example-hubspot-connector)
8. [Validation Rules](#8-validation-rules)
9. [Open Design Questions](#9-open-design-questions)
10. [Agent Generation Contract](#10-agent-generation-contract)

---

## 1. Design Principles

**P1 — YAML-first, JSON-compatible.** All configuration is authored in YAML for human readability. The parser must also accept equivalent JSON. YAML anchors and merge keys (`<<:`) are supported for DRY reuse within a file.

**P2 — No credentials in config files.** Credentials are always referenced by name (`credential_ref`). The runtime resolves them from encrypted PostgreSQL columns, environment variables, or an external secrets manager. Config files are safe to commit to version control.

**P3 — Explicit over implicit (HTTP & protocol mechanics).** Every HTTP-specific operational behaviour — pagination strategy, auth scheme, field selection, write operation endpoints — must be explicit. Conflict resolution strategies, identity matching rules, and consolidation logic are NOT in in-and-out config (they're in OSI-Mapping).

**P4 — IaC-compatible.** Files are plain text, diff-able, and renderable by templating engines (Helm, Kustomize, Jsonnet, envsubst). Variable interpolation uses a `${...}` syntax compatible with these tools.

**P5 — One connector per file.** Each YAML file defines a single connector. This keeps files focused and enables per-connector ownership, review, and deployment.

**P6 — Shared connection, split concerns.** A connector file contains a single `connection` block (base URL, auth, rate limits) shared by both tools. Ingestion-specific and writeback-specific configuration lives under each datatype's `ingestion` and `writeback` sub-sections. Each tool reads only the sections it needs and ignores the rest.

**P7 — Hierarchical overrides.** Settings cascade: tool-level defaults → connector-level overrides → datatype-level overrides → operation-level overrides. More specific settings always win.

---

## 2. File Organization

```
config/
├── ingestion.yaml          # Tool-level config for the ingestion daemon
├── writeback.yaml          # Tool-level config for the writeback daemon
└── connectors/
    ├── hubspot.yaml         # One file per external system
    ├── salesforce.yaml
    └── acme-crm.yaml
```

The connector directory path is specified in the tool config (`connectors_dir`). The tool watches this directory for changes and hot-reloads modified connector files at the start of the next sync cycle (GOAL.md: Configuration Hot-Reloading).

Both tools read from the **same** connector files. The ingestion tool uses each connector's `connection` block and per-datatype `ingestion` sections. The writeback tool uses each connector's `connection` block and per-datatype `writeback` sections. A datatype with only an `ingestion` section is ingestion-only (T1 #23); a datatype with only a `writeback` section is writeback-only.

---

## 3. Tool Configuration

### 3.1 Ingestion Tool (`ingestion.yaml`)

```yaml
# ─── Database ───────────────────────────────────────────────────
database:
  # Connection string or DSN. Supports ${ENV_VAR} interpolation.
  dsn: "${INOUT_DATABASE_URL}"
  # Connection pool limits.
  max_open_conns: 20
  max_idle_conns: 5
  conn_max_lifetime: 30m

# ─── Connector Directory ───────────────────────────────────────
connectors_dir: ./connectors

# ─── Webhook HTTP Server ───────────────────────────────────────
# Persistent HTTP server for receiving inbound webhook events.
# (T1 #6, #42)
webhook_server:
  listen: "0.0.0.0:8443"
  tls:
    enabled: true
    cert_file: "${TLS_CERT_PATH}"
    key_file: "${TLS_KEY_PATH}"
  # Per-endpoint inbound rate limiting (requests/sec).
  rate_limit:
    requests_per_second: 100
    burst: 200

# ─── Health / Readiness Endpoints ──────────────────────────────
# Served on a separate port from the webhook receiver.
# (Cross-Cutting: Health & Readiness Endpoints)
health_server:
  listen: "0.0.0.0:9090"

# ─── Observability ─────────────────────────────────────────────
observability:
  metrics:
    # Prometheus-compatible metrics endpoint.
    enabled: true
    listen: "0.0.0.0:9090"   # Served alongside health endpoints.
    path: /metrics
  logging:
    format: json              # json | text
    level: info               # debug | info | warn | error
  tracing:
    enabled: true
    # OpenTelemetry exporter config.
    otlp_endpoint: "${OTEL_EXPORTER_OTLP_ENDPOINT}"
    sample_rate: 1.0

# ─── Graceful Shutdown ─────────────────────────────────────────
# Maximum time to drain in-flight work on SIGTERM/SIGINT.
shutdown:
  drain_timeout: 30s

# ─── Runtime Control Table Polling ─────────────────────────────
control_table:
  poll_interval: 5s

# ─── Default Policies ─────────────────────────────────────────
# These apply to all connectors unless overridden at the connector
# or datatype level.
defaults:
  retry:
    max_retries: 5
    backoff:
      initial: 1s
      max: 60s
      multiplier: 2.0
      jitter: true
  rate_limit:
    requests_per_second: 10
    burst: 20
  circuit_breaker:
    error_threshold: 10          # Consecutive errors to open.
    pause_duration: 60s
    probe_count: 1               # Successful probes to close.
    backoff_multiplier: 2.0      # On repeated opens.
    max_pause_duration: 30m
  scheduling:
    default_interval: 5m

# ─── Housekeeping / Retention ──────────────────────────────────
# (Cross-Cutting: Retention & Data Housekeeping)
housekeeping:
  interval: 1h
  retention:
    sync_run_log: 90d
    dead_letter: 30d
    history_table: 365d
```

### 3.2 Writeback Tool (`writeback.yaml`)

```yaml
database:
  dsn: "${INOUT_DATABASE_URL}"
  max_open_conns: 20
  max_idle_conns: 5
  conn_max_lifetime: 30m

connectors_dir: ./connectors

# ─── Change Detection ─────────────────────────────────────────
# Primary mode for detecting desired-state changes.
# (T2 #10, #22, #32)
change_detection:
  mode: logical_replication      # logical_replication | polling
  replication_slot: inout_writeback
  publication: inout_desired_state
  # Fallback to polling when replication lag exceeds threshold.
  # (T2 #32)
  lag_warning_threshold: 100MB
  lag_max_threshold: 500MB
  # Polling interval when in polling mode or fallback.
  poll_interval: 5s

health_server:
  listen: "0.0.0.0:9091"

observability:
  metrics:
    enabled: true
    listen: "0.0.0.0:9091"
    path: /metrics
  logging:
    format: json
    level: info
  tracing:
    enabled: true
    otlp_endpoint: "${OTEL_EXPORTER_OTLP_ENDPOINT}"
    sample_rate: 1.0

shutdown:
  drain_timeout: 30s

control_table:
  poll_interval: 5s

defaults:
  retry:
    max_retries: 5
    backoff:
      initial: 1s
      max: 60s
      multiplier: 2.0
      jitter: true
  rate_limit:
    requests_per_second: 10
    burst: 20
  circuit_breaker:
    error_threshold: 10
    pause_duration: 60s
    probe_count: 1
    backoff_multiplier: 2.0
    max_pause_duration: 30m
  batch:
    max_records: 50
    max_payload_bytes: 1048576   # 1 MB
    max_wait: 5s

housekeeping:
  interval: 1h
  retention:
    sync_run_log: 90d
    dead_letter: 30d
    last_written_state: 365d
    desired_state_processed: 30d
```

---

## 4. Connector Configuration

A connector YAML defines one integration with a single external system. Both the ingestion and writeback tools read the same file.

### 4.1 Top-Level Structure

Every connector file MUST declare a top-level `schema_version`. Initial version is `1`.

```yaml
schema_version: 1

connector:
  # ─── Identity ──────────────────────────────────────────────
  name: hubspot                  # Unique identifier. Used in table names,
                                 # logs, metrics. Must match [a-z0-9_-]+.
  system: hubspot                # Logical system type (for grouping/display).
  description: "HubSpot CRM"    # Optional human-readable label.

  # ─── API Version ───────────────────────────────────────────
  # (T1 #39)
  api_version: "v3"
  api_deprecation_deadline: "2027-06-01"  # Optional. Emits warnings as
                                          # deadline approaches.

  # ─── Runtime Parameters ────────────────────────────────────
  # (T1 #28) Deploy-time values, not integration logic.
  runtime_params:
    webhook_callback_url:
      description: "Public URL for webhook registration"
      env: HUBSPOT_CALLBACK_URL
      required: true
    account_id:
      env: HUBSPOT_ACCOUNT_ID
      required: true

  # ─── Connection ────────────────────────────────────────────
  connection:
    base_url: "https://api.hubapi.com"
    # Optional per-connector timeout overrides.
    timeout:
      connect: 10s
      read: 30s
      write: 30s

  # ─── Authentication ────────────────────────────────────────
  auth: { ... }          # See §4.2

  # ─── Rate Limiting (connector-level override) ──────────────
  rate_limit:
    requests_per_second: 5
    burst: 10

  # ─── Retry Policy (connector-level override) ───────────────
  retry:
    max_retries: 3
    backoff:
      initial: 2s
      max: 120s
      multiplier: 2.0
      jitter: true

  # ─── Circuit Breaker (connector-level override) ────────────
  circuit_breaker:
    error_threshold: 5
    pause_duration: 120s
    empty_result_threshold: 100   # (T1 #13a) Min known records
                                  # before zero-result triggers.
    shrink_percentage: 50         # (T1 #13b) Max allowed shrinkage %.
    consecutive_empty_pages: 3    # (T1 #13c) Empty pages before trigger.

  # ─── Multi-Tenancy ────────────────────────────────────────
  # (T1 #20)
  tenancy:
    mode: single                  # single | multi
    # When mode=multi, how to inject the tenant scope:
    # tenant_param:
    #   location: header          # header | query | path
    #   name: "X-Account-ID"
    #   value: "${runtime.account_id}"

  # ─── Webhooks ──────────────────────────────────────────────
  webhooks: { ... }      # See §4.3

  # ─── Datatypes ─────────────────────────────────────────────
  datatypes: { ... }     # See §4.4
```

### 4.2 Authentication

Authentication is declared once per connector. The engine handles token lifecycle automatically. Credentials are always referenced by name, never inline.

#### OAuth2

```yaml
auth:
  type: oauth2
  oauth2:
    grant_type: authorization_code  # authorization_code | client_credentials
    token_url: "https://api.hubapi.com/oauth/v1/token"
    # refresh_url is optional; defaults to token_url.
    refresh_url: "https://api.hubapi.com/oauth/v1/token"
    scopes:
      - crm.objects.contacts.read
      - crm.objects.contacts.write
    # Where to inject the access token.
    token_injection:
      location: header              # header | query
      name: Authorization
      prefix: "Bearer "             # Added before the token value.
  credential_ref: hubspot_oauth     # Resolves to client_id, client_secret,
                                    # refresh_token from credential store.
```

#### API Key

```yaml
auth:
  type: api_key
  api_key:
    location: header                # header | query
    name: "X-API-Key"               # Header name or query param name.
  credential_ref: acme_api_key      # Resolves to the key value.
```

#### JWT

```yaml
auth:
  type: jwt
  jwt:
    algorithm: RS256
    issuer: "my-integration@example.com"
    audience: "https://api.target.com"
    expiry: 3600                    # Token lifetime in seconds.
    claims:                         # Additional custom claims.
      scope: "read write"
    token_injection:
      location: header
      name: Authorization
      prefix: "Bearer "
  credential_ref: acme_jwt_key     # Resolves to the signing key.
```

#### Custom Pre-Request Flow

For systems requiring a dedicated authentication call before any data requests (T1 #24).

```yaml
auth:
  type: custom
  custom:
    # Sequence of HTTP requests to acquire a session/token.
    steps:
      - name: acquire_session
        method: POST
        url: "${connection.base_url}/auth/login"
        headers:
          Content-Type: application/json
        body:
          username: "${credential.username}"
          password: "${credential.password}"
        # Extract values from the response for later use.
        extract:
          session_token:
            path: body.token
          expires_at:
            path: body.expires_at

    # How to inject the acquired values into subsequent requests.
    inject:
      header:
        X-Session-Token: "${auth.session_token}"

    # When and how to refresh.
    refresh:
      trigger:
        on_status: [401, 403]
        # Or time-based:
        # before_expiry: 60s       # Re-auth 60s before expiry.
      steps:                       # Same structure as above, or different.
        - name: renew_session
          method: POST
          url: "${connection.base_url}/auth/refresh"
          headers:
            X-Session-Token: "${auth.session_token}"
          extract:
            session_token:
              path: body.token

  credential_ref: acme_credentials
```

### 4.3 Webhooks

Configures the ingestion tool's inbound webhook receiver for this connector.

```yaml
webhooks:
  # Path under the webhook server where this connector receives events.
  # Full URL becomes: {webhook_server.listen}/{path}
  path: /webhooks/hubspot

  # ─── Signature Verification (T1 #34) ──────────────────────
  signature:
    algorithm: hmac-sha256         # hmac-sha256 | hmac-sha1 | rsa-sha256
    header: X-HubSpot-Signature-v3
    # The version of the signature scheme (some systems evolve
    # their signing algorithm across versions).
    version: "v3"
    credential_ref: hubspot_webhook_secret

  # ─── IP Allowlist (T1 #42) ────────────────────────────────
  ip_allowlist:
    - 34.226.11.0/24
    - 52.89.0.0/16

  # ─── Fan-Out Routing (T1 #19) ─────────────────────────────
  # When the external system delivers all event types to a single
  # endpoint, route events to the correct datatype handler.
  fan_out:
    # Expression evaluated against the incoming event payload.
    discriminator: body.subscriptionType
    routes:
      - match: "contact.*"         # Glob pattern against discriminator value.
        datatype: contacts
      - match: "company.*"
        datatype: companies
      - match: "deal.*"
        datatype: deals
    # Events that match no route.
    unmatched: log_and_discard     # log_and_discard | reject_400

  # ─── Lifecycle Management (T1 #7) ─────────────────────────
  registration:
    # How to register/renew webhooks with the external system.
    register:
      method: POST
      url: "${connection.base_url}/webhooks/v3/${runtime.account_id}/subscriptions"
      body:
        eventType: "${subscription.event_type}"
        propertyName: "${subscription.property_name}"
        active: true
    deregister:
      method: DELETE
      url: "${connection.base_url}/webhooks/v3/${runtime.account_id}/subscriptions/${subscription.id}"
    list:
      method: GET
      url: "${connection.base_url}/webhooks/v3/${runtime.account_id}/subscriptions"
      record_selector: results
    # Renewal interval — re-verify registration health.
    renewal_interval: 24h
    # Only manage subscriptions matching this callback URL (T1 #26).
    ownership_tag: "${runtime.webhook_callback_url}"

  # ─── Deduplication (T1 #25) ────────────────────────────────
  deduplication:
    event_id_path: body.eventId    # Path to the unique event identifier.
    window: 24h                    # How long to remember processed event IDs.
```

### 4.4 Datatypes

Each key under `datatypes` defines one logical datatype. The key becomes part of the table name (e.g., `inout_src_hubspot_contacts`).

```yaml
datatypes:
  contacts:
    # ─── Common Settings ───────────────────────────────────────
    description: "HubSpot Contact objects"

    # ─── Ingestion ─────────────────────────────────────────────
    ingestion: { ... }   # See §4.4.1

    # ─── Writeback ─────────────────────────────────────────────
    writeback: { ... }   # See §4.4.2
```

A datatype with only `ingestion` is ingestion-only (T1 #23). A datatype with only `writeback` is writeback-only.

#### 4.4.1 Ingestion Configuration

```yaml
ingestion:
  # ─── Primary Key ─────────────────────────────────────────
  # How to extract the external_id from each record.
  # (T1 #27: configurable response expressions)
  primary_key: id
  # Composite key:
  # primary_key: ["accountId", "contactId"]
  # Expression-based:
  # primary_key:
  #   expression: "${record.objectType}_${record.objectId}"

  # ─── History Mode (T1 #15, #30) ─────────────────────────
  history_mode: overwrite          # overwrite | append

  # ─── Fan-In (T1 #46) ────────────────────────────────────
  fan_in:
    enabled: false                 # Default: false (one table per connector).
    # When true, writes to a shared table across connectors.
    # Requires connector discriminator column.
    # shared_table: contacts

  # ─── Schema Tracking (T1 #31) ───────────────────────────
  schema_tracking:
    enabled: true                  # Detect and record schema drift.

  # ─── Timestamp Normalisation (T1 #45) ───────────────────
  timestamps:
    # Map of field paths to their source format.
    fields:
      properties.createdate:
        format: epoch_millis       # epoch_s | epoch_ms | iso8601 |
                                   # rfc2822 | custom
      properties.lastmodifieddate:
        format: epoch_millis
      # Custom format example:
      # properties.some_date:
      #   format: custom
      #   pattern: "2006-01-02T15:04:05"   # Go time layout

  # ─── API Version Override (T1 #39) ─────────────────────
  # Override the connector-level api_version for this datatype.
  # api_version: "v4"

  # ─── Scheduling (T1 #37) ────────────────────────────────
  schedule:
    interval: 5m                   # Fixed interval.
    # Or cron-style:
    # cron: "*/5 * * * *"

  # ─── Checkpoint Granularity (T1 #29) ────────────────────
  checkpoint:
    every_n_records: 500           # Checkpoint every N records.
    # Or: every_n_pages: 10

  # ─── Delta-Only Source (T1 #14) ─────────────────────────
  delta_only: false                # If true, never reset the stream.

  # ─── List Endpoint ──────────────────────────────────────
  # The primary endpoint for fetching records.
  list:
    method: GET
    path: "/crm/v3/objects/contacts"
    headers: {}                    # Additional per-request headers.
    query_params:
      limit: 100
      # archived: false

    # ─── Field / Property Selection (T1 #21) ──────────────
    field_selection:
      param: properties            # Query parameter name.
      fields:
        - "firstname"
        - "lastname"
        - "email"
        - "phone"
        - "company"
        - "!*.internal_*"          # Exclusion glob.

    # ─── Record Selector ──────────────────────────────────
    # Path to the array of records in the response body.
    # (T1 #27)
    record_selector: results

    # ─── Pagination (T1 #12) ─────────────────────────────
    pagination:
      strategy: cursor             # cursor | offset | link_header | page_number

      cursor:
        # Path in response to the next-page cursor.
        response_path: paging.next.after
        # Query parameter to send the cursor in.
        request_param: after

      # offset:
      #   start: 0
      #   request_param: offset
      #   limit_param: limit
      #   page_size: 100
      #   # How to determine total: from response field or auto.
      #   total_path: total

      # link_header:
      #   rel: next

      # page_number:
      #   start: 1
      #   request_param: page
      #   page_size_param: per_page
      #   page_size: 100

      termination:
        - empty_results            # No records returned.
        - missing_next_link        # Cursor/link absent from response.
        # - not_full_page          # Fewer records than page_size.
        # - max_pages: 1000        # Safety limit.

    # ─── Incremental Sync / High-Water Mark (T1 #3, #40) ──
    incremental:
      enabled: true
      # Which field to use as the watermark.
      cursor_field: properties.lastmodifieddate
      # Type of watermark value.
      cursor_type: timestamp       # timestamp | cursor | offset | sequence
      # Add to the list request to filter by watermark.
      request_filter:
        # How to inject the watermark into the request.
        mode: query_param          # query_param | body_param | sort_filter
        # For APIs that use filter expressions:
        filter_template:
          filterGroups:
            - filters:
                - propertyName: lastmodifieddate
                  operator: GTE
                  value: "${watermark}"
        # Or for simpler APIs:
        # param: updated_since
        # value: "${watermark}"
      # Sort order to ensure deterministic pagination.
      sort:
        field: properties.lastmodifieddate
        direction: asc
      # Lookback window to catch near-boundary records.
      lookback: 30s

  # ─── Detail Endpoint (T1 #9 — Parameterized Sources) ────
  # When the list endpoint returns only IDs or stubs,
  # fetch full records individually.
  detail:
    enabled: false
    method: GET
    path: "/crm/v3/objects/contacts/${record.id}"
    query_params:
      properties: "${ingestion.field_selection}"
    record_selector: null          # Response is the record itself.

  # ─── Linked Entities (T1 #16) ──────────────────────────
  # Parent records that embed child IDs requiring resolution.
  linked_entities:
    - name: line_items
      # Path in the parent record to the array of child IDs.
      id_path: associations.line_items.results[*].id
      # The datatype to resolve them into (must also be defined).
      target_datatype: line_items
      # Endpoint to fetch each child.
      detail:
        method: GET
        path: "/crm/v3/objects/line_items/${child.id}"

  # ─── Deletion Tracking (T1 #4, #5, #32) ────────────────
  deletion:
    # How to detect deletions.
    detection:
      mode: diff                   # diff | api_events | tombstone_endpoint
      # For diff mode (full sync): compare current fetch against
      # known records. Records missing from the fetch are candidates.

    # Verification: confirm deletion with a targeted lookup (T1 #5).
    verification:
      enabled: true
      method: GET
      path: "/crm/v3/objects/contacts/${external_id}"
      # Expected status code when record is truly deleted.
      deleted_status: 404
      # Or for systems that return a deleted flag:
      # deleted_field: archived
      # deleted_value: true

  # ─── Webhook Events (per-datatype) ─────────────────────
  # When this datatype receives webhook events.
  # (T1 #6, #8, #35)
  webhook_events:
    # Subscriptions to register for this datatype.
    subscriptions:
      - event_type: contact.creation
      - event_type: contact.propertyChange
        property_name: "*"
      - event_type: contact.deletion

    # Path to the affected record's ID in the event payload.
    record_id_path: body.objectId

    # Whether the event carries the full record or just a notification.
    # (T1 #8)
    payload_type: notification     # notification | full_state | partial
    # When notification or partial: fetch the full state via the
    # detail endpoint after receiving the event.

    # ─── Out-of-Order Handling (T1 #35) ──────────────────
    ordering:
      strategy: accept_latest_timestamp
      # accept_latest_timestamp | accept_highest_sequence | buffer_and_reorder
      sequence_path: body.occurredAt
      # For buffer_and_reorder:
      # buffer_window: 5s

    # Debounce: coalesce rapid-fire events for the same record.
    # (T1 #18)
    debounce:
      window: 2s

  # ─── Bulk Export (T1 #48) ───────────────────────────────
  bulk_export:
    enabled: false
    # submit:
    #   method: POST
    #   path: "/crm/v3/exports"
    #   body:
    #     exportType: contacts
    #     format: JSON
    # poll:
    #   method: GET
    #   path: "/crm/v3/exports/${job.id}"
    #   status_path: status
    #   completed_value: COMPLETE
    #   poll_interval: 10s
    #   max_poll_time: 1h
    # download:
    #   url_path: result.url
    #   record_selector: results

  # ─── Source Version / ETag (T1 #2) ─────────────────────
  source_version:
    # Path to the version/etag field in each record, if available.
    field: properties.hs_object_version
    # Or from response headers:
    # header: ETag
```

#### 4.4.2 Writeback Configuration

```yaml
writeback:
  # ─── Write-Anomaly Protection Level (T2 #38) ────────────
  protection_level: 2              # 1 | 2 | 3
  # Level 1: Full protection (conditional writes).
  # Level 2: Practical protection (pre-flight read only).
  # Level 3: Level 2 + post-write verification read.
  # Note: In an OSI-integrated architecture, OSI-Mapping's delta views
  # provide _action, _cluster_id, and _base. This connector
  # performs 3-way conflict detection (base vs current vs desired)
  # to determine if writes are safe.

  # ─── Conflict Resolution (T2 #3, #30) ───────────────────
  conflict_resolution: dead_letter
  # dead_letter | last_writer_wins | skip_and_warn
  # Options specific to OSI-integrated workflows:
  #   - dead_letter: Route conflicts to queue for manual review
  #   - last_writer_wins: Desired-state overrides current (force write)
  #   - skip_and_warn: Skip change if conflict detected, log warning
  # Note: "re_ingest_and_recompute" is not applicable here since OSI-Mapping
  # handles cluster resolution and desired-state computation once, upstream.

  # ─── Identity Tracking (T2 #16) ─────────────────────────
  # Track the cluster_id → external_id (target system ID) mapping.
  # In OSI-integrated workflow, cluster_id is provided by OSI-Mapping's
  # delta views (_delta_{mapping}). This connector writes it to the target
  # system and maintains reverse mapping for identity lookups.
  identity_tracking:
    enabled: true
    target_field: properties.inout_cluster_id
    lookback: 30d                  # Days to maintain reverse mapping

  # ─── Batch Composition (T2 #33) ─────────────────────────
  batch:
    max_records: 10
    max_payload_bytes: 524288      # 512 KB
    max_wait: 2s

  # ─── Dependency Ordering (T2 #26) ───────────────────────
  # dependencies:
  #   - depends_on: companies      # This datatype depends on companies
  #                                # existing first.

  # ─── Scheduling (polling fallback mode, T2 #35) ─────────
  schedule:
    interval: 10s
    # cron: "*/10 * * * * *"

  # ─── Dead-Letter Config (T2 #24) ────────────────────────
  dead_letter:
    # Max retries before routing to dead-letter.
    max_retries: 3

  # ─── Operations (T2 #18) ────────────────────────────────
  # Per-operation HTTP definitions.
  # The desired-state table provides the data; these operations
  # define how to execute writes to the target system.
  operations:

    # ── Lookup (pre-flight read for 3-way conflict detection) ─
    lookup:
      method: GET
      path: "/crm/v3/objects/contacts/${external_id}"
      query_params:
        properties: []             # Fetch all properties for diff
      record_selector: null        # Response is the record itself.
      # Does this endpoint return an ETag/version?
      version_header: ETag         # Or null if not supported.
      version_field: null           # Or a field path in the response body.

    # ── Insert ─────────────────────────────────────────────
    insert:
      method: POST
      path: "/crm/v3/objects/contacts"
      # Transform: reshape desired-state data into target payload.
      # OSI's delta views provide source-shaped fields; this template
      # handles any final HTTP-specific reshaping.
      transform:
        template:
          properties:
            email: "${data.email}"
            firstname: "${data.first_name}"
            lastname: "${data.last_name}"
            phone: "${data.phone}"
            inout_cluster_id: "${data.cluster_id}"
      # Where to find the generated ID in the response.
      response_id_path: id
      # Pre-write validation schema (T2 #23).
      validation:
        required_fields:
          - email

    # ── Update ─────────────────────────────────────────────
    update:
      method: PATCH
      path: "/crm/v3/objects/contacts/${external_id}"
      # Support conditional writes when available (T2 #3, #38).
      conditional_write:
        enabled: true
        header: If-Match
        value: "${pre_flight.etag}"      # From the lookup response.
      transform:
        template:
          properties:
            email: "${data.email}"
            firstname: "${data.first_name}"
            lastname: "${data.last_name}"
            phone: "${data.phone}"
      # Client-side patching: compute minimal diff (T2 #5).
      patch_mode: diff             # diff | full
      # diff: Only send changed fields (compare base vs desired).
      # full: Send all fields regardless.

    # ── Delete ─────────────────────────────────────────────
    delete:
      method: DELETE
      path: "/crm/v3/objects/contacts/${external_id}"
      # Expected success status codes.
      success_status: [200, 204]

    # ── Archive (soft-delete, T2 #20) ─────────────────────
    archive:
      method: POST
      path: "/crm/v3/objects/contacts/${external_id}/archive"
      success_status: [200, 204]

    # ── Upsert (T2 #19) ──────────────────────────────────
    # upsert:
    #   method: PUT
    #   path: "/crm/v3/objects/contacts/upsert"
    #   transform:
    #     template:
    #       idProperty: email
    #       properties: "${data}"
    #   response_id_path: id

  # ─── API Asymmetry Mapping (T2 #12) ─────────────────────
  # When this target's read schema (GET response) differs from its
  # write schema (POST/PATCH request), map GET fields to canonical
  # names used in 3-way conflict detection.
  # This handles per-target asymmetry (e.g., CRM returns "properties"
  # object, but also supports "custom_field_123"). Consolidation
  # asymmetry (e.g., CRM field ≠ ERP field) is OSI-Mapping's concern.
  read_write_mapping:
    # Target system field (from GET) → field name in lookup + conflict detection
    properties.email: email
    properties.firstname: first_name
    properties.lastname: last_name
    properties.phone: phone

  # ─── Partial-Success Response Parsing (T2 #29) ──────────
  # For batch endpoints that return mixed success/failure.
  # batch_response:
  #   results_path: results
  #   status_path: status
  #   success_value: COMPLETE
  #   error_path: errors
  #   record_id_path: id

  # ─── CRDT Support (T2 #6) ──────────────────────────────
  # crdt:
  #   fields:
  #     - name: tags
  #       type: add_only_set       # add_only_set | counter | lww_register
  #     - name: view_count
  #       type: counter
```

### 4.5 Relationships as First-Class Datatypes (T1 #22)

Many-to-many relationships are defined as their own datatype entries with `kind: relationship`.

```yaml
datatypes:
  contact_to_company:
    kind: relationship             # Default is "entity" when omitted.
    description: "Contact ↔ Company associations"
    ingestion:
      primary_key:
        expression: "${record.fromObjectId}_${record.toObjectId}"
      list:
        method: GET
        path: "/crm/v4/associations/contact/company"
        record_selector: results
        pagination:
          strategy: cursor
          cursor:
            response_path: paging.next.after
            request_param: after
          termination:
            - missing_next_link
      history_mode: overwrite
      schedule:
        interval: 15m
```

### 4.6 Merge & Split Actions (T2 #34)

In an OSI-integrated architecture, **OSI-Mapping's delta views** detect cluster merges and splits via identity resolution changes (e.g., two clusters becoming one, or one splitting into multiple). The `_delta_{mapping}` views emit `merge` and `split` action classifications. The writeback tool recognises these actions in the `_action` column and executes them via standard operations (`insert`, `update`, `delete`/`archive`, and identity-mapping updates).

No additional per-datatype config is needed beyond standard operations. The connector must simply declare which operations are available so the tool knows what the target system supports.

```yaml
writeback:
  # Declare which action types this datatype supports.
  # OSI-Mapping's delta views classify cluster events as merge/split.
  # This connector executes them.
  supported_actions:
    - insert
    - update
    - delete
    - archive
    - merge                        # Consolidated clusters becoming one
    - split                        # Cluster breaking into multiple
```

---

## 5. Expression & Path Language

Configuration values that reference fields in HTTP responses, record payloads, or other dynamic data use a **dot-notation path language**.

### 5.1 Path Expressions

Used in `record_selector`, `cursor.response_path`, `primary_key`, `record_id_path`, etc.

| Expression | Meaning |
|---|---|
| `results` | Top-level key `results` in the response body |
| `data.results` | Nested key `results` under `data` |
| `paging.next.after` | Deeply nested value |
| `results[0].id` | First element's `id` field |
| `results[*].id` | All elements' `id` field (array projection) |
| `headers.ETag` | Response header value (when context is full response) |

Path expressions are evaluated against the **response body** by default. Use `headers.` prefix to access response headers. Use `body.` prefix for explicit body access (required in webhook event contexts where both headers and body are available).

### 5.2 Primary Key Expressions

Simple field reference:
```yaml
primary_key: id
```

Composite key (multiple fields):
```yaml
primary_key: ["accountId", "contactId"]
```

Computed key (expression):
```yaml
primary_key:
  expression: "${record.objectType}_${record.objectId}"
```

### 5.3 Field Selection Patterns (T1 #21)

The `field_selection.fields` list supports:

| Pattern | Meaning |
|---|---|
| `email` | Include the `email` field |
| `properties.*` | Include all fields under `properties` |
| `!internal_notes` | Exclude `internal_notes` |
| `!*.internal_*` | Exclude any field matching `internal_*` under any parent |

Inclusion and exclusion patterns are processed in order. Exclusions (`!` prefix) remove from the set accumulated by prior inclusions.

---

## 6. Variable Interpolation

All string values in connector YAML support `${...}` interpolation. Variables are resolved at different phases depending on their namespace.

### 6.1 Namespaces

| Namespace | Resolved When | Examples |
|---|---|---|
| `${ENV_VAR}` | Process start (from environment) | `${INOUT_DATABASE_URL}` |
| `${runtime.param_name}` | Config load (from `runtime_params`) | `${runtime.webhook_callback_url}` |
| `${credential.field}` | Auth setup (from credential store) | `${credential.client_id}` |
| `${auth.field}` | After auth step (from extracted tokens) | `${auth.session_token}` |
| `${connection.base_url}` | Config load | `${connection.base_url}` |
| `${watermark}` | Sync execution (current watermark value) | `${watermark}` |
| `${external_id}` | Per-record execution | `${external_id}` |
| `${record.path}` | Per-record field access | `${record.properties.email}` |
| `${data.field}` | Writeback per-record (from desired-state `data`) | `${data.email}` |
| `${pre_flight.etag}` | Writeback pre-flight result | `${pre_flight.etag}` |
| `${pre_flight.version}` | Writeback pre-flight result | `${pre_flight.version}` |
| `${child.id}` | Linked entity resolution | `${child.id}` |
| `${job.id}` | Bulk export job lifecycle | `${job.id}` |
| `${subscription.*}` | Webhook registration | `${subscription.event_type}` |
| `${ingestion.field_selection}` | Resolved to the comma-joined field list | `${ingestion.field_selection}` |

### 6.2 Resolution Order

1. **Environment variables** — resolved once at process startup.
2. **Runtime parameters** — resolved when the connector config is loaded.
3. **Connection / auth values** — resolved when the connector initializes.
4. **Execution-time values** (`watermark`, `external_id`, `record.*`, `data.*`, `pre_flight.*`) — resolved per-request at runtime.

Unresolved variables at their expected resolution phase cause a validation error. A `${...}` that references an undefined namespace is a config error caught at load time.

---

## 7. Concrete Example: HubSpot Connector

A realistic (but simplified) HubSpot connector demonstrating key features.

```yaml
connector:
  name: hubspot
  system: hubspot
  description: "HubSpot CRM — contacts and companies"
  api_version: "v3"

  runtime_params:
    callback_url:
      description: "Public URL for webhook delivery"
      env: HUBSPOT_CALLBACK_URL
      required: true
    portal_id:
      env: HUBSPOT_PORTAL_ID
      required: true

  connection:
    base_url: "https://api.hubapi.com"
    timeout:
      connect: 10s
      read: 30s

  auth:
    type: oauth2
    oauth2:
      grant_type: authorization_code
      token_url: "https://api.hubapi.com/oauth/v1/token"
      scopes:
        - crm.objects.contacts.read
        - crm.objects.contacts.write
        - crm.objects.companies.read
      token_injection:
        location: header
        name: Authorization
        prefix: "Bearer "
    credential_ref: hubspot_oauth

  rate_limit:
    requests_per_second: 5
    burst: 10

  retry:
    max_retries: 3
    backoff:
      initial: 1s
      max: 60s
      multiplier: 2.0
      jitter: true

  circuit_breaker:
    error_threshold: 5
    pause_duration: 120s
    empty_result_threshold: 50
    shrink_percentage: 40

  webhooks:
    path: /webhooks/hubspot
    signature:
      algorithm: hmac-sha256
      header: X-HubSpot-Signature-v3
      version: "v3"
      credential_ref: hubspot_webhook_secret
    ip_allowlist:
      - 34.226.11.0/24
    fan_out:
      discriminator: body.subscriptionType
      routes:
        - match: "contact.*"
          datatype: contacts
        - match: "company.*"
          datatype: companies
      unmatched: log_and_discard
    registration:
      register:
        method: POST
        url: "${connection.base_url}/webhooks/v3/${runtime.portal_id}/subscriptions"
        body:
          eventType: "${subscription.event_type}"
          active: true
      deregister:
        method: DELETE
        url: "${connection.base_url}/webhooks/v3/${runtime.portal_id}/subscriptions/${subscription.id}"
      list:
        method: GET
        url: "${connection.base_url}/webhooks/v3/${runtime.portal_id}/subscriptions"
        record_selector: results
      renewal_interval: 24h
      ownership_tag: "${runtime.callback_url}"
    deduplication:
      event_id_path: body.eventId
      window: 24h

  # ───────────────────────────────────────────────────────────
  #  DATATYPES
  # ───────────────────────────────────────────────────────────
  datatypes:

    # ── Contacts ───────────────────────────────────────────
    contacts:
      description: "HubSpot Contact objects"

      ingestion:
        primary_key: id
        history_mode: overwrite
        schema_tracking:
          enabled: true
        timestamps:
          fields:
            properties.createdate:
              format: epoch_millis
            properties.lastmodifieddate:
              format: epoch_millis
        schedule:
          interval: 5m
        checkpoint:
          every_n_records: 500

        list:
          method: GET
          path: "/crm/v3/objects/contacts"
          query_params:
            limit: 100
          field_selection:
            param: properties
            fields:
              - firstname
              - lastname
              - email
              - phone
              - company
              - lifecyclestage
          record_selector: results
          pagination:
            strategy: cursor
            cursor:
              response_path: paging.next.after
              request_param: after
            termination:
              - empty_results
              - missing_next_link
          incremental:
            enabled: true
            cursor_field: properties.lastmodifieddate
            cursor_type: timestamp
            request_filter:
              mode: body_param
              filter_template:
                filterGroups:
                  - filters:
                      - propertyName: lastmodifieddate
                        operator: GTE
                        value: "${watermark}"
            sort:
              field: properties.lastmodifieddate
              direction: asc
            lookback: 30s

        detail:
          enabled: false

        deletion:
          detection:
            mode: diff
          verification:
            enabled: true
            method: GET
            path: "/crm/v3/objects/contacts/${external_id}"
            deleted_status: 404

        webhook_events:
          subscriptions:
            - event_type: contact.creation
            - event_type: contact.propertyChange
              property_name: "*"
            - event_type: contact.deletion
          record_id_path: body.objectId
          payload_type: notification
          ordering:
            strategy: accept_latest_timestamp
            sequence_path: body.occurredAt
          debounce:
            window: 2s

        source_version:
          field: properties.hs_object_version

      writeback:
        protection_level: 2
        conflict_resolution: dead_letter
        identity_tracking:
          enabled: true
          target_field: properties.inout_cluster_id
          lookback: 30d
        batch:
          max_records: 10
          max_payload_bytes: 524288
          max_wait: 2s
        supported_actions:
          - insert
          - update
          - archive
        dead_letter:
          max_retries: 3

        operations:
          lookup:
            method: GET
            path: "/crm/v3/objects/contacts/${external_id}"
            query_params:
              properties: []         # Fetch all for conflict detection
            version_header: null
            version_field: null

          insert:
            method: POST
            path: "/crm/v3/objects/contacts"
            transform:
              template:
                properties:
                  email: "${data.email}"
                  firstname: "${data.first_name}"
                  lastname: "${data.last_name}"
                  phone: "${data.phone}"
                  inout_cluster_id: "${data.cluster_id}"
            response_id_path: id
            validation:
              required_fields:
                - email

          update:
            method: PATCH
            path: "/crm/v3/objects/contacts/${external_id}"
            conditional_write:
              enabled: false
            transform:
              template:
                properties:
                  email: "${data.email}"
                  firstname: "${data.first_name}"
                  lastname: "${data.last_name}"
                  phone: "${data.phone}"
            patch_mode: diff

          archive:
            method: POST
            path: "/crm/v3/objects/contacts/${external_id}/archive"
            success_status: [200, 204]

        read_write_mapping:
          properties.email: email
          properties.firstname: first_name
          properties.lastname: last_name
          properties.phone: phone

    # ── Companies ──────────────────────────────────────────
    companies:
      description: "HubSpot Company objects"

      ingestion:
        primary_key: id
        history_mode: overwrite
        schema_tracking:
          enabled: true
        timestamps:
          fields:
            properties.createdate:
              format: epoch_millis
            properties.lastmodifieddate:
              format: epoch_millis
        schedule:
          interval: 10m
        checkpoint:
          every_n_records: 500

        list:
          method: GET
          path: "/crm/v3/objects/companies"
          query_params:
            limit: 100
          field_selection:
            param: properties
            fields:
              - name
              - domain
              - industry
              - city
              - country
          record_selector: results
          pagination:
            strategy: cursor
            cursor:
              response_path: paging.next.after
              request_param: after
            termination:
              - empty_results
              - missing_next_link
          incremental:
            enabled: true
            cursor_field: properties.lastmodifieddate
            cursor_type: timestamp
            request_filter:
              mode: body_param
              filter_template:
                filterGroups:
                  - filters:
                      - propertyName: lastmodifieddate
                        operator: GTE
                        value: "${watermark}"
            sort:
              field: properties.lastmodifieddate
              direction: asc
            lookback: 30s

        deletion:
          detection:
            mode: diff
          verification:
            enabled: true
            method: GET
            path: "/crm/v3/objects/companies/${external_id}"
            deleted_status: 404

        # Companies: ingestion-only (no writeback section).

    # ── Contact ↔ Company Associations ─────────────────────
    contact_to_company:
      kind: relationship
      description: "Contact ↔ Company associations"

      ingestion:
        primary_key:
          expression: "${record.fromObjectId}_${record.toObjectId}"
        history_mode: overwrite
        schedule:
          interval: 15m
        list:
          method: GET
          path: "/crm/v4/associations/contact/company"
          record_selector: results
          pagination:
            strategy: cursor
            cursor:
              response_path: paging.next.after
              request_param: after
            termination:
              - missing_next_link
```

---

## 8. Validation Rules

The engine validates every connector config file at load time and in connector validation mode (T1 #43, T2 #37). Validation is layered:

### 8.1 Structural Validation (always, at load time)

1. **Required fields present:** `connector.name`, `connector.connection.base_url`, `connector.auth`.
2. **Name format:** `connector.name` matches `[a-z][a-z0-9_-]*` (used in PostgreSQL table names).
3. **At least one datatype** with at least one of `ingestion` or `writeback`.
4. **Auth type valid** and type-specific required fields present.
5. **All `${...}` references** resolve to a known namespace. Unknown namespaces are errors.
6. **Interpolation safety:** `${...}` expressions must not allow arbitrary code execution. Only the defined namespaces in §6.1 are permitted.
7. **Pagination strategy** has its required sub-fields (e.g., `cursor` strategy requires `cursor.response_path` and `cursor.request_param`).
8. **Primary key** is defined for every ingestion datatype.
9. **Conflict resolution** is one of the four allowed strategies.
10. **Protection level** is 1, 2, or 3 and consistent with `conditional_write.enabled` on the update operation.
11. **Dependency graph** (T2 #26): if `dependencies` are declared, the graph must be acyclic. Cycles are reported as config errors.
12. **Credential references** are syntactically valid names (actual credential existence is checked at runtime).
13. **History mode** is explicitly declared per ingestion datatype (P3: explicit over implicit).
14. **Schema version** is required at the top level and must be supported by the running binary (`schema_version: 1` for this spec version).

### 8.2 Connectivity Validation (connector validation mode)

Activated via CLI (`inout validate connector hubspot.yaml`) or via the runtime control table (`command: validate`).

1. **Resolve credentials:** Verify the referenced credential exists and can be loaded.
2. **Authenticate:** Execute the full auth flow and confirm a valid token/session is obtained.
3. **Test connectivity:** Issue a lightweight request to the base URL (e.g., `GET /` or a known health endpoint).
4. **Dry-run fetch:** Execute a single-page fetch for each ingestion datatype — send the real HTTP request, parse the response, extract records via `record_selector`, and verify `primary_key` extraction succeeds. No data is persisted.
5. **Check conditional write support** (writeback): Issue a `GET` to the lookup endpoint and check for `ETag` / version headers. Report the effective protection level.
6. **Validate field mappings** (writeback): Construct a sample payload via the transform template and verify it against the target API's known schema if discoverable.
7. **Report:** Output a structured pass/fail report for each check.

### 8.3 Validation Output Format (machine-readable)

To support coding-agent workflows and CI gates, validation output MUST be available as JSON with stable rule IDs.

```json
{
  "connector": "hubspot",
  "schema_version": 1,
  "valid": false,
  "errors": [
    {
      "rule_id": "CFG-001",
      "severity": "error",
      "path": "$.connector.datatypes.contacts.ingestion.list.pagination",
      "message": "cursor strategy requires cursor.response_path and cursor.request_param",
      "suggested_fix": "Add cursor.response_path and cursor.request_param or switch strategy"
    }
  ],
  "warnings": [
    {
      "rule_id": "CFG-020",
      "severity": "warning",
      "path": "$.connector.api_deprecation_deadline",
      "message": "API version deprecation deadline is within 90 days",
      "suggested_fix": "Upgrade api_version and update operation mappings"
    }
  ]
}
```

Initial required rule IDs:

- `CFG-001`: pagination strategy shape invalid
- `CFG-002`: unknown interpolation namespace
- `CFG-003`: unresolved required runtime parameter
- `CFG-004`: invalid connector name format
- `CFG-005`: missing required top-level keys
- `CFG-006`: unsupported `schema_version`
- `CFG-007`: invalid auth type or missing auth fields
- `CFG-008`: missing ingestion primary key
- `CFG-009`: invalid conflict resolution strategy
- `CFG-010`: invalid protection-level / conditional-write pairing
- `CFG-011`: cyclic dependency graph
- `CFG-012`: invalid credential reference name
- `CFG-013`: missing ingestion history mode

### 8.4 Runtime Validation (continuous)

1. **Schema drift detection** (T1 #31): On every ingested record, compare the response structure against the last known schema. Log new/removed/changed fields and increment `_schema_version`.
2. **API version deprecation** (T1 #39): If `api_deprecation_deadline` is set and approaching (within 90 days), emit WARN-level log entries on every sync cycle.
3. **Unresolved runtime variables:** If a `${runtime.*}` or `${credential.*}` variable cannot be resolved at execution time, the affected connector is halted with a CONFIG_ERROR classification.

---

## 9. Open Design Questions

These are design decisions that need to be resolved during prototyping or early implementation. Each is scoped enough to be answered independently.

### Q1: Transform Language — Templates vs. Mappings vs. Expressions

The current design uses inline YAML templates for writeback payload construction (§4.4.2 `transform.template`). Three alternatives exist:

- **(a) YAML templates** (current): Inline YAML mirroring the target structure with `${...}` interpolation. Simple, readable, limited to field renaming and restructuring.
- **(b) Declarative field mappings**: A flat list of `source → target` pairs with optional type coercion. Simpler but cannot express nested restructuring.
- **(c) Expression language** (e.g., CEL, JMESPath, or a custom DSL): More powerful, handles conditional logic and computed fields, but adds learning curve and a parsing dependency.

**Recommendation:** Start with (a) YAML templates for the common case. Add (b) as syntactic sugar for flat mappings. Defer (c) unless a real connector requires conditional transform logic that templates cannot express.

### Q2: Webhook Registration — Declarative vs. Imperative

The current design declaratively describes HTTP requests for webhook registration/deregistration (§4.3 `registration`). Some systems have complex registration flows (multi-step, OAuth-scoped, app-level vs. subscription-level). Should we:

- **(a)** Keep the declarative HTTP step sequence and extend it as needed?
- **(b)** Define webhook lifecycle as a pluggable adapter (Go interface) per system, with the declarative config handling only simple cases?

**Recommendation:** Start with (a). If a real connector's webhook flow cannot be expressed declaratively, introduce (b) as an escape hatch in the Connector SDK.

### Q3: Multi-File Connector Configs

For connectors with many datatypes (e.g., Salesforce with 50+ objects), a single YAML file grows unwieldy. Should we support:

- **(a)** Splitting a connector across multiple files (e.g., `salesforce/connection.yaml`, `salesforce/contacts.yaml`, `salesforce/accounts.yaml`)?
- **(b)** YAML anchors and `$ref`-style file includes?
- **(c)** Keep single-file but rely on YAML anchors for DRY within the file?

**Recommendation:** Start with (c). If real-world connectors prove too large, add (a) with a convention where a directory named after the connector is merged into a single logical connector config.

### Q4: Pre-Write Validation Schema Format

The current design uses a simple `required_fields` list (§4.4.2 `validation`). Should we support:

- **(a)** Just `required_fields` (current)?
- **(b)** JSON Schema for full structural validation?
- **(c)** A lightweight constraint language (field presence, type checks, regex patterns)?

**Recommendation:** Start with (a). Add (c) if field-level type mismatches become a common failure mode. Defer (b) — shipping a full JSON Schema validator is heavy for the initial implementation.

### Q5: Ingestion `request_filter` Abstraction Level

The current `incremental.request_filter` in §4.4.1 supports two modes: a simple `query_param` injection and a structured `filter_template` for APIs with complex filter schemas (e.g., HubSpot's `filterGroups`). The template approach embeds API-specific JSON structure in the config.

- **(a)** Keep both modes (current): simple param injection for simple APIs, raw template for complex ones.
- **(b)** Standardise a filter DSL that the engine translates to API-specific syntax per system.

**Recommendation:** Keep (a). A universal filter DSL would need per-system translation logic, which is the kind of per-integration code the declarative approach is designed to avoid.

### Q6: Config Schema Versioning

As the config schema evolves, older connector files may become incompatible. Should we:

- **(a)** Add a top-level `schema_version: 1` field and maintain a migration path between versions?
- **(b)** Use semantic versioning on the tool binary and document which config fields were added/changed in each release?

**Recommendation:** (a). A `schema_version` field in each connector file enables the loader to detect and reject incompatible configs with a clear error message, and supports future automated migration tooling.

Status: adopted in this document as a mandatory field (see §4.1 and §8.1).

---

## 10. Agent Generation Contract

This section defines normative constraints to make connector generation deterministic and reliable for coding agents.

### 10.1 Generation Rules (normative)

Generated connector files MUST follow these rules:

1. **Must be schema-valid:** pass structural validation with zero errors.
2. **Must declare schema version:** include top-level `schema_version`.
3. **Must declare one generation profile:** set `connector.generation_profile` to one of the supported profiles in §10.2.
4. **Must materialize required defaults:** generated output must include all profile-required keys, even when values match defaults.
5. **Must avoid unresolved placeholders:** no `${...}` references outside the namespace list in §6.1.
6. **Must avoid ambiguous alternatives:** generated files must not include commented-out alternative blocks.
7. **Must be deterministic:** key ordering follows §10.4 and repeated generation from the same inputs yields byte-identical output (excluding comments).

Generated connector files SHOULD follow these rules:

1. **Should include explicit `description`** at connector and datatype level.
2. **Should include operation-level validation** (`required_fields`) for writeback insert/update.
3. **Should include a minimum viable scheduling block** for every ingestion or polling writeback datatype.

### 10.2 Supported Generation Profiles

Each generated connector MUST declare one profile under `connector.generation_profile`.

```yaml
schema_version: 1
connector:
  generation_profile: ingestion_polling_readonly
  # ...rest of connector
```

Profiles:

| Profile | Description | Typical use |
|---|---|---|
| `ingestion_polling_readonly` | Polling-based ingestion only, no webhooks, no writeback | Initial connector bootstrap |
| `ingestion_webhook_incremental` | Ingestion with webhooks + incremental polling fallback | SaaS APIs with webhook support |
| `writeback_patch` | Writeback-only with lookup + patch/update path | Output integrations |
| `full_duplex` | Both ingestion and writeback in one connector | Bi-directional sync |

### 10.3 Required Paths by Profile

`ingestion_polling_readonly` required paths:

- `schema_version`
- `connector.name`
- `connector.system`
- `connector.generation_profile`
- `connector.api_version`
- `connector.connection.base_url`
- `connector.auth`
- `connector.datatypes.{name}.ingestion.primary_key`
- `connector.datatypes.{name}.ingestion.history_mode`
- `connector.datatypes.{name}.ingestion.schedule`
- `connector.datatypes.{name}.ingestion.list.method`
- `connector.datatypes.{name}.ingestion.list.path`
- `connector.datatypes.{name}.ingestion.list.record_selector`
- `connector.datatypes.{name}.ingestion.list.pagination`

`ingestion_webhook_incremental` required paths:

- all required paths from `ingestion_polling_readonly`
- `connector.webhooks.path`
- `connector.webhooks.signature`
- `connector.webhooks.fan_out`
- `connector.datatypes.{name}.ingestion.incremental`
- `connector.datatypes.{name}.ingestion.webhook_events`

`writeback_patch` required paths:

- `schema_version`
- `connector.name`
- `connector.system`
- `connector.generation_profile`
- `connector.api_version`
- `connector.connection.base_url`
- `connector.auth`
- `connector.datatypes.{name}.writeback.protection_level`
- `connector.datatypes.{name}.writeback.conflict_resolution`
- `connector.datatypes.{name}.writeback.supported_actions`
- `connector.datatypes.{name}.writeback.operations.lookup`
- `connector.datatypes.{name}.writeback.operations.update`

`full_duplex` required paths:

- all required paths from `ingestion_webhook_incremental`
- all required paths from `writeback_patch`

### 10.4 Canonical Serialization Rules

To reduce diffs and make generated output stable, serializer output order SHOULD be:

1. `schema_version`
2. `connector.name`
3. `connector.system`
4. `connector.generation_profile`
5. `connector.description`
6. `connector.api_version`
7. `connector.api_deprecation_deadline`
8. `connector.runtime_params`
9. `connector.connection`
10. `connector.auth`
11. `connector.rate_limit`
12. `connector.retry`
13. `connector.circuit_breaker`
14. `connector.tenancy`
15. `connector.webhooks`
16. `connector.datatypes`

Within each datatype, use: `description`, `kind`, `ingestion`, `writeback`.

### 10.5 JSON Schema Starter Layout

For automation tooling, publish schema artifacts in-repo:

```
schemas/
├── connector.schema.json
├── defs/
│   ├── auth.schema.json
│   ├── pagination.schema.json
│   ├── ingestion.schema.json
│   ├── writeback.schema.json
│   └── webhooks.schema.json
└── profiles/
    ├── ingestion_polling_readonly.schema.json
    ├── ingestion_webhook_incremental.schema.json
    ├── writeback_patch.schema.json
    └── full_duplex.schema.json
```

Implementation note: profile schemas should apply additional constraints (`allOf`) on top of the base `connector.schema.json`.

### 10.6 Structured Error Contract for Validators

Any validator (CLI, CI action, or library) MUST emit a machine-readable format compatible with §8.3 and include:

- `rule_id`
- `severity` (`error` or `warning`)
- JSONPath-like `path` (rooted at `$`)
- human-readable `message`
- actionable `suggested_fix`

This format is the compatibility contract for coding agents that automatically repair or regenerate connector YAML.

### 10.7 Golden Fixtures for Agent Workflows

To reduce model drift and prompt ambiguity, maintain fixtures in-repo:

```
fixtures/connectors/
├── valid/
│   ├── minimal_ingestion_polling.yaml
│   ├── minimal_ingestion_webhook_incremental.yaml
│   ├── minimal_writeback_patch.yaml
│   └── minimal_full_duplex.yaml
└── invalid/
    ├── missing_schema_version.yaml
    ├── unknown_namespace.yaml
    ├── invalid_pagination_shape.yaml
    ├── invalid_protection_level_pairing.yaml
    └── cyclic_dependencies.yaml
```

Each invalid fixture should have an expected error manifest (`.errors.json`) listing exact `rule_id` values.
