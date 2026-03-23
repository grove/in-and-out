# Connector Authoring Guide

This is the canonical guide for writing connector YAML files for **in-and-out**.

---

## 1. Overview

A **connector** is a declarative YAML file that describes how in-and-out should
communicate with a specific external HTTP API. It contains:

- Connection details (base URL, timeouts)
- Authentication scheme
- One or more **datatypes** — each datatype maps to one source table in PostgreSQL
- Per-datatype ingestion configuration (how to paginate, which fields to use as IDs)
- Per-datatype writeback configuration (how to create/update/delete objects)

The **engine** handles all orchestration: scheduling, retry, circuit breakers,
watermarks, dead-letter queuing, observability, and schema management. The connector
file declares HTTP mechanics only — no business logic.

---

## 2. Connector YAML Structure

```yaml
schema_version: 1

connector:
  name: hubspot                          # snake_case, matches ^[a-z][a-z0-9_-]*$
  system: HubSpot CRM
  generation_profile: full_duplex        # ingestion_polling_readonly | writeback_patch | full_duplex
  description: "HubSpot CRM connector"
  api_version: "v3"
  api_version_deprecation_date: "2027-06-01"   # ISO date — warn when within warning_days
  api_version_warning_days: 90
  version: "1.2.0"

  connection:
    base_url: "https://api.hubapi.com"
    timeout:
      connect: "10s"
      read: "30s"
      write: "30s"

  auth:
    scheme: oauth2
    client_id: "${HUBSPOT_CLIENT_ID}"
    client_secret: "${HUBSPOT_CLIENT_SECRET}"
    token_url: "https://api.hubapi.com/oauth/v1/token"
    scopes: ["crm.objects.contacts.read", "crm.objects.contacts.write"]

  rate_limit:
    requests_per_second: 9.0
    burst: 18

  datatypes:
    contacts:
      description: "HubSpot CRM contacts"
      kind: entity
      pii_fields: ["email", "phone", "firstname", "lastname"]

      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: "5m"
        list:
          method: GET
          path: "/crm/v3/objects/contacts"
          record_selector: "results"
          pagination:
            strategy: cursor
            cursor:
              response_path: "paging.next.after"
              request_param: "after"
            page_size_param: "limit"
            page_size: 100
          incremental:
            enabled: true
            cursor_field: "updatedAt"
            cursor_type: timestamp
            request_filter:
              mode: query_param
              param: "updatedAfter"
              value: "${watermark}"
          drift_protection: true
          drift_max_shrink_pct: 50.0
          snapshot_param: "snapshot_id"

      writeback:
        protection_level: 2           # optimistic
        conflict_resolution: last_writer_wins
        supported_actions: [insert, update, delete]
        operations:
          lookup:
            method: GET
            path: "/crm/v3/objects/contacts/${external_id}"
          insert:
            method: POST
            path: "/crm/v3/objects/contacts"
          update:
            method: PATCH
            path: "/crm/v3/objects/contacts/${external_id}"
            conditional_write:
              enabled: false
          delete:
            method: DELETE
            path: "/crm/v3/objects/contacts/${external_id}"
```

### Required top-level keys

| Key             | Required | Description                          |
|-----------------|----------|--------------------------------------|
| `schema_version`| yes      | Must be `1`                          |
| `connector`     | yes      | Root connector object                |

### Connector required fields

| Field               | Required | Description                                      |
|---------------------|----------|--------------------------------------------------|
| `name`              | yes      | Unique identifier, `^[a-z][a-z0-9_-]*$`         |
| `system`            | yes      | Human-readable system name                       |
| `generation_profile`| yes      | One of the four generation profiles              |
| `api_version`       | yes      | API version string (injected into paths/headers) |
| `connection.base_url`| yes     | Base URL for all API calls                       |
| `auth`              | yes      | Authentication configuration                     |
| `datatypes`         | yes      | At least one datatype                            |

---

## 3. Authentication Schemes

### API Key (header)

```yaml
auth:
  scheme: api_key
  header: "X-API-Key"
  value: "${MY_API_KEY}"
```

### API Key (query parameter)

```yaml
auth:
  scheme: api_key
  query_param: "api_key"
  value: "${MY_API_KEY}"
```

### OAuth2 (client credentials)

```yaml
auth:
  scheme: oauth2
  client_id: "${CLIENT_ID}"
  client_secret: "${CLIENT_SECRET}"
  token_url: "https://auth.example.com/oauth/token"
  scopes: ["read:contacts", "write:contacts"]
```

### JWT

```yaml
auth:
  scheme: jwt
  signing_key: "${JWT_SIGNING_KEY}"
  algorithm: RS256
  claims:
    iss: "my-service"
    aud: "external-api"
  expiry_seconds: 3600
```

### Basic Auth

```yaml
auth:
  scheme: basic
  username: "${API_USERNAME}"
  password: "${API_PASSWORD}"
```

### Custom (bearer token)

```yaml
auth:
  scheme: custom
  header: "Authorization"
  value: "Bearer ${MY_TOKEN}"
```

---

## 4. Pagination Strategies

### Cursor-based

```yaml
pagination:
  strategy: cursor
  cursor:
    response_path: "paging.next.after"   # dot-notation path in response JSON
    request_param: "after"               # query param name for next page cursor
  page_size_param: "limit"
  page_size: 100
  termination:
    - empty_results
    - no_cursor
```

### Offset-based

```yaml
pagination:
  strategy: offset
  offset:
    request_param: "offset"
    page_size_param: "limit"
    page_size: 50
  termination:
    - empty_results
    - not_full_page
```

### Link header (RFC 5988)

```yaml
pagination:
  strategy: link_header
  termination:
    - no_next_link
```

### Keyset / cursor from response body

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

---

## 5. Incremental Sync

```yaml
incremental:
  enabled: true
  cursor_field: "updated_at"            # field in each record holding the watermark value
  cursor_type: timestamp                # timestamp | cursor | offset | sequence
  request_filter:
    mode: query_param
    param: "updated_after"
    value: "${watermark}"
  cursor_window: "24h"                  # max time window per poll cycle (optional)
```

The watermark is persisted in `inout_ops_watermark` after each successful sync cycle.
Reset via control command `force_full_sync`.

---

## 6. Writeback Config

```yaml
writeback:
  protection_level: 1                   # 1=conditional_write | 2=optimistic | 3=fire_and_forget
  conflict_resolution: last_writer_wins # dead_letter | last_writer_wins | skip_and_warn | server_wins
  supported_actions: [insert, update, delete, archive]
  max_concurrent_writes: 10
  batch_size: 50
  etag_header: "ETag"
  if_match_header: "If-Match"
  dry_run: false                        # set true to preview writes without executing
  operations:
    lookup:
      method: GET
      path: "/objects/${external_id}"
    insert:
      method: POST
      path: "/objects"
    update:
      method: PATCH
      path: "/objects/${external_id}"
      conditional_write:
        enabled: true
        header: "If-Match"
        value: "${pre_flight.etag}"
    delete:
      method: DELETE
      path: "/objects/${external_id}"
    archive:
      method: POST
      path: "/objects/${external_id}/archive"
```

---

## 7. Simulator Contract

Every connector should ship with a `respx`-based simulator covering:

1. **Happy path pagination** — correct cursor advancement, last page termination
2. **Auth failure** — 401 response triggers circuit breaker / retry
3. **Rate limit** — 429 response with `Retry-After` header
4. **Webhook delivery** — POST to webhook endpoint with correct signature
5. **Empty response** — circuit breaker should not allow mass deletion
6. **Partial failure** — some records fail, others succeed (dead-letter)

```python
# Example simulator skeleton
import respx
import httpx

@pytest.fixture
def hubspot_mock():
    with respx.mock(base_url="https://api.hubapi.com") as mock:
        mock.get("/crm/v3/objects/contacts").mock(
            return_value=httpx.Response(200, json={
                "results": [{"id": "1", "properties": {"email": "alice@example.com"}}],
                "paging": {"next": {"after": "cursor_page2"}}
            })
        )
        yield mock
```

---

## 8. Required Test Suite

Every connector **must** include the following tests:

| Test name                    | What it verifies                                      |
|------------------------------|-------------------------------------------------------|
| `test_yaml_valid`            | YAML parses without errors against `ConnectorFileConfig` |
| `test_credentials_resolvable`| All `${VAR}` references map to known env vars or secrets |
| `test_mock_fetch_one_page`   | Engine fetches one page from simulator, records upserted |
| `test_writeback_dry_run`     | Dry-run mode logs writes without executing HTTP calls |

```python
def test_yaml_valid(connector_yaml_path):
    from inandout.config.loader import load_connector
    cfg = load_connector(connector_yaml_path)
    assert cfg.connector.name is not None

def test_mock_fetch_one_page(pool, hubspot_mock):
    from inandout.ingestion.engine import IngestionEngine
    engine = IngestionEngine(pool)
    # ... run one sync cycle against mock
```

---

## 9. PII Annotation

```yaml
datatypes:
  contacts:
    pii_fields: ["email", "phone", "firstname", "lastname", "address"]
```

Effect:
- PII fields are **redacted in structured logs** (replaced with `[REDACTED]`)
- `inandout purge-by-id --connector hubspot --datatype contacts --id ext_123` will
  NULL-out all PII columns for the given record

---

## 10. Linting

Before submitting a connector, run:

```bash
inandout lint --connector path/to/connector.yaml
```

This validates:
- Schema version
- All interpolation tokens are resolvable
- No cyclic dependencies
- `protection_level=1` paired with `conditional_write.enabled=true`
- All operation paths use only allowed interpolation tokens

---

## 11. Publishing

```bash
# Validate locally
inandout lint --connector hubspot.yaml

# Run the required test suite
uv run pytest tests/connectors/hubspot/ -v

# Register in the connector registry (if applicable)
inandout connector publish --connector hubspot.yaml --registry ./connectors/
```

Connector files are version-controlled YAML. They are not deployed separately —
they are mounted into the daemon container at `/connectors/`.
