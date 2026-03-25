# in-and-out

Declarative, bidirectional HTTP API synchronization for composite MDM platforms.

in-and-out is the I/O layer between external SaaS APIs and your PostgreSQL-based MDM pipeline. It runs as two stateless daemons:

- Ingestion daemon: pulls records from external APIs into PostgreSQL source tables
- Writeback daemon: pushes desired-state changes back to external APIs with conflict protection

All behavior is configured through YAML connector files and tool config, so integrations are version-controlled and repeatable.

## Why this project exists

Most MDM programs need robust API integration plumbing before identity resolution can add value. in-and-out focuses specifically on that plumbing:

- Inbound sync with polling and webhook support
- Outbound writeback with pre-flight checks and conflict strategies
- Durable operational state in PostgreSQL
- Strong observability and operator controls

This lets upstream systems like OSI-Mapping focus on identity resolution and consolidation logic.

## Core capabilities

- Declarative connectors in YAML (no custom integration code required)
- Multiple generation profiles:
  - ingestion_polling_readonly
  - ingestion_webhook_incremental
  - writeback_patch
  - full_duplex
- Built-in auth models: OAuth2, API key, JWT, custom flows
- Pagination support: cursor, offset, link header, keyset
- Incremental sync with watermarks and checkpoints
- Writeback protection levels and conflict-resolution strategies
- Dead-letter queues and replay workflows
- Runtime control plane via control table and CLI
- Health/readiness endpoints, Prometheus metrics, OpenTelemetry traces

## High-level architecture

External APIs and webhooks feed the ingestion daemon, which writes source tables in PostgreSQL.
OSI-Mapping and pg-trickle consume those tables and produce desired-state rows.
The writeback daemon consumes desired state and writes changes back to external APIs.

The daemons are decoupled through PostgreSQL and can be scaled independently.

## Repository layout

- src/inandout: application source code
- config: tool config for ingestion and writeback daemons
- connectors: connector examples and templates
- migrations: Alembic schema migrations
- schemas: JSON schemas for connector validation
- tests: unit, integration, contract, acceptance, load
- book: mdBook documentation source
- docs-build: generated local docs output

## Quick start

### Prerequisites

- Python 3.13+
- uv
- PostgreSQL 15 or 16
- Docker and Docker Compose (optional but recommended for local stack)
- just (optional convenience task runner)

### Install

```bash
git clone git@github.com:grove/in-and-out.git
cd in-and-out
uv sync --all-extras
```

### Start local database and migrate

With just:

```bash
just up-db
just db-upgrade
```

Or directly:

```bash
docker compose up -d postgres
uv run alembic upgrade head
```

### Run daemons locally

```bash
just ingest
just writeback
```

### Validate a connector

```bash
uv run inandout ingest validate-connector --connector connectors/hubspot.example.yaml
```

## Local documentation

This project uses mdBook for documentation.

Build docs:

```bash
just docs-build
```

Serve docs locally with live reload:

```bash
just docs-serve
```

## CLI overview

Main command groups:

- inandout ingest
- inandout writeback
- inandout db
- inandout control
- inandout dead-letter
- inandout webhook
- inandout connector
- inandout api

Show all commands:

```bash
uv run inandout --help
```

## Operations model

Operational state is stored in inout_ops_* tables, including:

- inout_ops_sync_run
- inout_ops_watermark
- inout_ops_control
- inout_ops_writeback_result
- inout_ops_identity_map

Dead-letter queues are stored in inout_dl_* tables for ingestion and writeback replay workflows.

## Development workflow

Useful commands:

```bash
just check            # lint + typecheck
just test             # unit tests
just test-all         # all non-acceptance/non-load tests
just validate-connectors
```

Run the full local CI path:

```bash
just ci
```

## Deployment

- Docker Compose files are included for local and observability stacks
- Kubernetes manifests are provided under k8s
- GitHub Actions workflow builds mdBook docs from book

## License

This project is licensed under the terms in LICENSE.
