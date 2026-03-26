# Plan: Stateful Demo Simulator

**Status:** Implemented (P1–P4, commit c422127)  
**Date:** 2026-03-26  
**Audience:** Sales demos, stakeholder walkthroughs, developer onboarding

---

## 1. Problem

The existing simulator framework (`GenericSimulator`) is a stateless `respx` HTTP interceptor that lives inside test processes. It is fast and ideal for CI, but:

- It has no persistent state — records vanish when the test ends.
- It has no UI — there is nothing to show a non-developer.
- It cannot be pointed at by a running engine instance — it only intercepts in-process `httpx` calls.
- It cannot demonstrate the data flow visually: ingestion pulling records, writeback pushing changes, conflicts, pagination.

For demos and onboarding, we need a **real HTTP server** that acts as a fake CRM, holds mutable records, and shows what is happening in real time.

---

## 2. Goals

1. **Config-driven:** Read any connector YAML and dynamically expose the exact API surface the engine expects — no per-connector Python code required.
2. **Stateful:** Hold records in a mutable store (in-memory or SQLite) that persists across requests and optionally across restarts.
3. **Interactive:** A web UI where users can browse, create, edit, and delete records to trigger ingestion changes, conflict scenarios, etc.
4. **Reactive:** Real-time visual feedback (SSE) so the user sees record mutations, engine requests, and sync activity as they happen.
5. **Zero-friction setup:** `docker compose up` starts the full demo stack — Postgres, simulator (seeded), ingest daemon, writeback daemon.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Demo Simulator (FastAPI)                    :6100      │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Route Builder │  │ Record Store │  │   Web UI     │  │
│  │  (reads YAML) │  │ (in-mem/SQL) │  │ (SSE+HTMX)  │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
│         │  derives         │  pushes         │          │
│         ▼  routes          ▼  events         ▼          │
│  ┌──────────────────────────────────────────────────┐   │
│  │            Dynamic API Surface                    │   │
│  │  GET  /crm/v3/objects/contacts       (list)      │   │
│  │  GET  /crm/v3/objects/contacts/:id   (detail)    │   │
│  │  POST /crm/v3/objects/contacts       (insert)    │   │
│  │  PATCH /crm/v3/objects/contacts/:id  (update)    │   │
│  │  DELETE /crm/v3/objects/contacts/:id (delete)    │   │
│  │  POST /oauth/v1/token                (auth)      │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
         ▲                              ▲
         │  HTTP (engine calls)         │  Browser (human)
         │                              │
┌────────┴────────┐           ┌─────────┴─────────┐
│  Ingest/Write   │           │  Demo Dashboard   │
│  Engine         │           │  (HTMX + SSE)     │
└─────────────────┘           └───────────────────┘
```

---

## 4. Components

### 4.1 Route Builder — config-driven API generation

Reads a connector YAML at startup and registers FastAPI routes for every endpoint the engine would call.

| Connector config field | Generated route |
|---|---|
| `ingestion.list.path` | `GET {path}` with pagination per `list.pagination.strategy` |
| `ingestion.list.detail_path` | `GET {path}/{external_id}` |
| `writeback.operations.insert` | `POST {path}` — returns generated ID |
| `writeback.operations.update` | `PATCH {path}/{external_id}` |
| `writeback.operations.delete` | `DELETE {path}/{external_id}` |
| `writeback.operations.lookup` | `GET {path}/{external_id}` |
| `writeback.operations.upsert` | `PUT {path}` |
| `auth.oauth2.token_url` | `POST {token_url}` — returns `access_token` |
| `webhooks.path` | Outbound webhook dispatch (simulator → engine) |

**Pagination strategies** — all five derived from the same config the engine reads:

| Strategy | Behaviour |
|---|---|
| **cursor** | Returns `cursor.response_path` token in response body; reads `cursor.request_param` from next request |
| **offset** | Reads `offset_param` / `limit_param` from query string |
| **page_number** | Reads `page_param` / `per_page_param` from query string |
| **link_header** | Returns RFC 5988 `Link: <url>; rel="next"` header |
| **keyset** | Reads `request_param` (e.g. `after`), returns records after that key |

**Incremental sync:** Records carry an auto-updated `modified_at` timestamp. The list endpoint respects `incremental.request_filter` to return only records modified after the watermark value, matching engine expectations.

### 4.2 Record Store — pluggable state backend

```
RecordStore (protocol)
├── MemoryStore      — dict-of-dicts, default, zero config
└── SQLiteStore      — file-backed, survives restarts
```

Per datatype, the store holds:

- Records keyed by auto-generated ID (UUID or sequential)
- `created_at` and `modified_at` timestamps (for watermark / incremental support)
- An append-only **mutation log** recording every create, update, and delete with timestamp — feeds the UI activity stream

### 4.3 Web UI — HTMX + SSE, zero build step

Server-rendered Jinja2 templates with HTMX for interactivity and **Server-Sent Events** for real-time updates. No JavaScript build toolchain — ships as static assets inside the Python package.

#### Pages

| Page | Purpose |
|---|---|
| **Dashboard** | All datatypes as cards with record counts, last engine sync time, and a live activity feed |
| **Datatype table** | Paginated record table with inline edit, create button, delete button |
| **Record detail** | Single-record JSON view, edit form, complete mutation history |
| **Activity log** | Real-time SSE-driven stream of all operations |

#### Reactive behaviour (SSE)

- Every mutation — whether from an engine write or a user edit — publishes an event to an SSE channel.
- Dashboard and table views subscribe and auto-update affected rows and counts.
- Changed rows briefly highlight to draw attention.
- Toast notifications surface engine operations ("Engine fetched page 2 of contacts").
- Colour-coded badges distinguish event sources:
  - **Green** — ingested by engine
  - **Blue** — written back by engine
  - **Yellow** — modified by user via UI
  - **Red** — deleted

### 4.4 Activity & Request Logging

Every HTTP request the engine makes is logged:

- Timestamp, method, path, query params, request body (truncated)
- Response status, body (truncated), duration
- Matched datatype and operation type

This provides full visibility into engine behaviour — useful for demos ("watch the polling happen") and debugging ("why did the engine send this request?").

---

## 5. CLI & Configuration

```bash
# Run standalone, reading one or more connector YAMLs
inandout simulator run \
  --connector connectors/hubspot.example.yaml \
  --listen 0.0.0.0:6100 \
  --store memory \          # or sqlite:///demo.db
  --seed 50                 # generate 50 records per datatype

# Shorthand via justfile
just simulator
```

| Flag | Default | Description |
|---|---|---|
| `--connector` | (required) | Path to connector YAML (repeatable for multi-connector) |
| `--listen` | `0.0.0.0:6100` | Bind address |
| `--store` | `memory` | `memory` or `sqlite:///path.db` |
| `--seed` | `0` | Number of fixture records to generate per datatype on startup |
| `--log-level` | `info` | Logging verbosity |

Environment variable override: `INOUT_SIMULATOR_STORE=sqlite:///demo.db`

---

## 6. Docker Compose Integration

New service added to `docker-compose.yml`:

```yaml
simulator:
  build: .
  command: inandout simulator run --connector /connectors/hubspot.example.yaml --seed 50
  ports:
    - "6100:6100"
  volumes:
    - ./connectors:/connectors:ro
```

The connector YAML's `base_url` is overridden to point at `http://simulator:6100` via an environment variable (`INOUT_CONNECTOR_BASE_URL_OVERRIDE`) or a CLI flag on the ingest/writeback services.

Running `docker compose up` starts: **Postgres → migrations → simulator (seeded) → ingest daemon → writeback daemon** — a complete working demo.

A corresponding `just` recipe:

```just
# Start the full demo stack including simulator
demo:
    docker compose --profile demo up -d
```

---

## 7. Project Structure

```
src/inandout/simulator/
├── __init__.py
├── app.py                  # FastAPI app factory, startup/shutdown
├── cli.py                  # CLI subcommand: inandout simulator run
├── route_builder.py        # Reads connector YAML → registers FastAPI routes
├── store/
│   ├── __init__.py         # RecordStore protocol
│   ├── memory.py           # In-memory dict-of-dicts implementation
│   └── sqlite.py           # SQLite-backed implementation
├── handlers/
│   ├── __init__.py
│   ├── list.py             # Paginated list endpoint (all 5 strategies)
│   ├── detail.py           # Single-record GET
│   ├── write.py            # Insert / update / delete / upsert handlers
│   ├── auth.py             # OAuth2 token endpoint
│   └── webhook.py          # Outbound webhook dispatch to engine
├── ui/
│   ├── router.py           # UI page routes (dashboard, table, record, activity)
│   ├── sse.py              # SSE event broadcaster
│   └── templates/
│       ├── base.html       # Layout with HTMX + SSE script tags
│       ├── dashboard.html  # Datatype cards, counts, activity feed
│       ├── table.html      # Record table with inline CRUD
│       ├── record.html     # Record detail + mutation history
│       └── activity.html   # Full activity log stream
└── seed.py                 # Loads seed_data from connector YAML manifest
```

---

## 8. Relationship to Existing Simulators

The existing `GenericSimulator` (`respx`-based in-process interceptor) remains unchanged. It is purpose-built for unit and integration tests — fast, no network, no server process.

The new stateful simulator is a **separate tool** for demos and exploratory development. Both share the same connector config parsing (`inandout.config.connector`) but serve different purposes:

| | GenericSimulator (existing) | Stateful Demo Simulator (new) |
|---|---|---|
| **Use case** | Automated tests | Demos, onboarding, exploration |
| **Runs as** | In-process `respx` context manager | Standalone HTTP server |
| **State** | Stateless, fixture data only | Mutable records, mutation log |
| **Persistence** | None (test lifetime) | Memory or SQLite |
| **UI** | None | Web dashboard with SSE |
| **Network** | No real HTTP | Real TCP — engine connects over network |

---

## 9. Implementation Phases

| Phase | Scope | Depends on |
|---|---|---|
| **P1 — Core server** | Record store (memory), route builder with multi-connector prefix routing (`/{connector_name}/...`), list + detail + write handlers, CLI entry point (accepts multiple `--connector` flags), basic pagination (cursor + offset) | — |
| **P2 — Web UI** | Dashboard (per-connector sections), datatype table, record CRUD forms, SSE broadcaster, reactive updates | P1 |
| **P3 — Full pagination & webhooks** | All five pagination strategies, incremental sync support, outbound webhook dispatch (simulator → engine on every mutation) | P1 |
| **P4 — Persistence & seeding** | SQLite store, seed data loaded from connector manifest `seed_data` section, docker-compose integration, `just` recipes | P1 |
| **P5 — Observability** | Activity log page, request inspector | P2, P3 |
| **P6 — Advanced** | Scenario presets ("demo conflict", "demo rate limit", "demo pagination edge cases"), auth enforcement (optional) | P4, P5 |

---

## 10. Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Base URL override mechanism | Implementation detail — decide during P1. |
| 2 | Webhook outbound | **Yes.** The simulator proactively POSTs webhook events to the engine on every record mutation. This is the key visual demo effect. Scheduled for P3. |
| 3 | Multi-connector routing | **Prefix by connector name** (`/{connector_name}/...`). Avoids path collisions when multiple YAMLs are loaded. |
| 4 | Auth enforcement | **Skip for now.** Accept all requests, log the auth header. Auth enforcement is optional, deferred to P6. |
| 5 | Seed data | **Embed minimal test data in the connector YAML manifest** under a new `seed_data` section per datatype. No Faker dependency. This also doubles as documentation — connector authors see example records alongside the config. |

---

## 11. Connector Manifest `seed_data` Extension

A new optional `seed_data` key per datatype in the connector YAML. Ignored by the engine — consumed only by the simulator.

```yaml
connector:
  name: hubspot
  # ... existing config ...
  datatypes:
    contacts:
      ingestion:
        # ... existing config ...
      writeback:
        # ... existing config ...
      seed_data:
        - id: "1001"
          firstname: Alice
          lastname: Smith
          email: alice.smith@example.com
          lastmodifieddate: "2026-03-20T10:00:00Z"
        - id: "1002"
          firstname: Bob
          lastname: Jones
          email: bob.jones@example.com
          lastmodifieddate: "2026-03-21T14:30:00Z"
        - id: "1003"
          firstname: Carol
          lastname: Lee
          email: carol.lee@example.com
          lastmodifieddate: "2026-03-22T09:15:00Z"
```

Benefits:

- **Zero extra dependencies** — just YAML.
- **Self-documenting** — connector authors see realistic example records next to the schema they defined.
- **Portable** — `seed_data` travels with the connector and works in any environment.
- The simulator loads `seed_data` into the record store at startup. If `--seed N` is also provided, additional synthetic records are generated on top.
