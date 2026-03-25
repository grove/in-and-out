# in-and-out

**Declarative, bidirectional HTTP API synchronisation for Master Data Management.**

in-and-out is the I/O layer of a composite MDM architecture. It pulls data from external HTTP APIs into PostgreSQL, and pushes desired-state changes back out — all driven by declarative YAML configuration.

## Who is this for?

| Role | What you'll find here |
|---|---|
| **Operators** | Installation, config reference, database guide, CLI reference, runbook |
| **Integration authors** | Connector authoring guide, testing, writeback configuration |
| **Platform developers** | Architecture overview, schema contract, OSI-Mapping integration |

## Quick links

- **New here?** Start with the [Architecture Overview](./architecture.md), then follow the [Getting Started](./getting-started.md) guide.
- **Writing a connector?** Head to the [Connector Authoring Guide](./connector-authoring.md).
- **Operating in production?** Check the [Configuration Reference](./configuration.md) and [Database Guide](./database.md).

## What does in-and-out do?

1. **Ingestion** — Polls external HTTP APIs on a schedule (or receives webhooks), normalises the responses, and upserts records into PostgreSQL source tables.
2. **Writeback** — Reads desired-state rows produced by upstream MDM logic, performs conflict detection via three-way comparison, and writes changes back to external APIs.

Both daemons are stateless, long-lived processes. All state lives in PostgreSQL. Configuration is declarative YAML — no code required to add a new integration.

## What it does *not* do

- **Identity resolution** — that's OSI-Mapping
- **Field-level conflict scoring** — that's OSI-Mapping
- **Incremental view maintenance** — that's pg-trickle
- **Business logic** — the bridge layer between ingestion output and writeback input is outside this tool's scope
