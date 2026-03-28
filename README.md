<div align="center">

# 🔄 in-and-out

**Keep your systems in sync — automatically.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.13+](https://img.shields.io/badge/Python-3.13%2B-3776AB.svg?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791.svg?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED.svg?logo=docker&logoColor=white)](engine/Dockerfile)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Supported-326CE5.svg?logo=kubernetes&logoColor=white)](k8s/)

---

*A data synchronization tool that connects your external services (like HubSpot, Salesforce, etc.) to your central database — pulling data in and pushing changes back out, all configured through simple YAML files.*

</div>

## What does in-and-out do?

Imagine you have customer data spread across multiple cloud services. You need that data in one central place (a PostgreSQL database), and when you make changes there, those changes need to flow back out to the original services.

**in-and-out handles both directions automatically:**

```
                    ┌──────────────┐
  HubSpot ─────┐   │              │   ┌───── HubSpot
  Salesforce ──┼──▶│  PostgreSQL  │──▶├───── Salesforce
  Stripe ──────┘   │  (your hub)  │   └───── Stripe
                    └──────────────┘
               PULL data in          PUSH changes out
```

It runs as two background services:

| Service | What it does |
|---------|-------------|
| **Ingestion** | Pulls records from external APIs into your database on a schedule or via webhooks |
| **Writeback** | Pushes changes from your database back to external APIs with conflict protection |

## Why use it?

- **No code required** — define integrations in YAML config files, not custom code
- **Reliable** — tracks progress with checkpoints so nothing gets lost on restart
- **Conflict-aware** — detects when external data changed before overwriting it
- **Observable** — built-in health checks, metrics, and tracing
- **Scalable** — stateless services backed by PostgreSQL; run multiple instances safely

## How it fits into the bigger picture

in-and-out is the data transport layer. It works alongside [OSI-Mapping](https://github.com/BaardBouvet/OSI-mapping), which handles the harder problem of figuring out which records across different systems represent the same real-world entity (identity resolution).

```
External APIs → [in-and-out] → PostgreSQL → [OSI-Mapping] → PostgreSQL → [in-and-out] → External APIs
                  (pull in)                  (resolve &                      (push out)
                                              merge)
```

## Getting started

### What you'll need

| Requirement | Notes |
|------------|-------|
| Python 3.13+ | Runtime |
| [uv](https://docs.astral.sh/uv/) | Python package manager |
| PostgreSQL 15+ | Database |
| Docker + Compose | *Optional* — easiest way to run PostgreSQL locally |
| [just](https://github.com/casey/just) | *Optional* — handy task runner |

### 1. Clone and install

```bash
git clone git@github.com:grove/in-and-out.git
cd in-and-out
uv sync --all-extras
```

### 2. Start the database

Using `just` (recommended):

```bash
just up-db
just db-upgrade
```

Or with Docker Compose directly:

```bash
docker compose up -d postgres
uv run alembic upgrade head
```

### 3. Run the services

```bash
just ingest       # start pulling data in
just writeback    # start pushing changes out
```

### 4. Validate a connector file

```bash
uv run inandout ingest validate-connector --connector connectors/hubspot.example.yaml
```

## Key features

| Feature | Description |
|---------|-------------|
| **YAML connectors** | Define integrations declaratively — no custom code |
| **Multiple sync modes** | Polling, webhooks, incremental sync, full duplex |
| **Built-in authentication** | OAuth2, API key, JWT, and custom auth flows |
| **Smart pagination** | Cursor, offset, link header, and keyset pagination |
| **Conflict resolution** | Configurable strategies to handle concurrent changes |
| **Dead-letter queues** | Failed records are saved for review and replay |
| **Runtime control** | Pause, resume, and reconfigure without restarting |
| **Observability** | Prometheus metrics, OpenTelemetry traces, health endpoints |

## CLI commands

in-and-out provides a command-line interface organized into groups:

```
inandout ingest        # Data ingestion commands
inandout writeback     # Writeback commands
inandout db            # Database management
inandout control       # Runtime control (pause, resume, etc.)
inandout dead-letter   # Review and replay failed records
inandout webhook       # Webhook management
inandout connector     # Connector utilities
inandout api           # API server
```

Run `uv run inandout --help` to see all available commands.

## Documentation

Full documentation is built with [mdBook](https://rust-lang.github.io/mdBook/). To browse it locally:

```bash
just docs-serve     # builds and opens docs with live reload
```

## Project structure

```
src/inandout/    Application source code
config/          Daemon configuration files
connectors/      Connector examples and templates
migrations/      Database schema migrations (Alembic)
schemas/         JSON schemas for connector validation
tests/           Unit, integration, contract, acceptance, and load tests
book/            Documentation source (mdBook)
k8s/             Kubernetes deployment manifests
```

## Development

```bash
just check       # Run linter and type checker
just test        # Run unit tests
just test-all    # Run all tests (except acceptance & load)
just ci          # Full CI pipeline locally
```

## Deployment

in-and-out is designed for containerized deployment:

- **Docker Compose** — included for local and observability stacks
- **Kubernetes** — manifests provided in `k8s/`
- **CI/CD** — GitHub Actions workflow for automated builds and docs

## License

Licensed under [Apache 2.0](LICENSE).
