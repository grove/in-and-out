# Installation Guide

Get in-and-out installed and verify everything works.

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.13+ | Required |
| PostgreSQL | 15 or 16 | Required |
| [uv](https://docs.astral.sh/uv/) | latest | Recommended package manager |
| Docker + Compose | latest | Optional — for the local dev stack |
| [just](https://just.systems/) | latest | Optional — for convenience recipes |

## Installing from source

```bash
git clone git@github.com:grove/in-and-out.git
cd in-and-out
uv sync
```

This installs all dependencies (including dev tools) into a virtual environment managed by `uv`.

For production (no dev dependencies):

```bash
uv sync --no-dev
```

## Installing as a package

When published to PyPI:

```bash
pip install inandout
```

Or with uv:

```bash
uv tool install inandout
```

## Verifying the installation

```bash
inandout version
inandout --help
```

You should see the version number and a list of available commands. See the [CLI Reference](./cli.md) for the full command tree.

## Setting up the database

### 1. Create the PostgreSQL database and user

```sql
CREATE USER inandout WITH PASSWORD 'your-secure-password';
CREATE DATABASE inandout OWNER inandout;
```

### 2. Set the database connection string

```bash
export INOUT_DATABASE_URL="postgresql://inandout:your-secure-password@localhost:5432/inandout"
```

> **Tip**: Never put the database URL in a config file that's committed to version control. Always use an environment variable.

### 3. Run migrations

```bash
inandout db upgrade
```

This applies all Alembic migrations to bring the schema up to date.

### 4. Verify the schema

```bash
inandout db status
```

You should see the current migration revision and no pending migrations.

## Quick smoke test

Validate the bundled example connector to confirm everything is wired up:

```bash
inandout ingest validate-connector --connector connectors/hubspot.example.yaml --skip-connectivity
```

A successful validation confirms that:
- The YAML parses correctly
- All required fields are present
- The configuration matches the declared generation profile

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `INOUT_DATABASE_URL` | PostgreSQL connection string | *(required)* |
| `INOUT_CONFIG_PATH` | Path to tool config YAML | `config/ingestion.yaml` |
| `INOUT_LOG_LEVEL` | Override log level | `info` |
| `INOUT_LOG_FORMAT` | Override log format | `json` |

Connector-specific credentials use `${VAR_NAME}` interpolation in connector YAML files. See the [Connector Authoring Guide](./connector-authoring.md) for details.

## Docker quickstart

If you prefer Docker, the bundled Compose file starts everything:

```bash
docker compose up -d postgres     # Start PostgreSQL
docker compose up migrate         # Run migrations (waits for healthy DB)
docker compose up -d ingest       # Start ingestion daemon
docker compose up -d writeback    # Start writeback daemon
```

Or with `just`:

```bash
just up-db
just db-upgrade
just up
```

The Compose stack exposes:
- PostgreSQL on `localhost:5432`
- Ingestion health endpoint on `localhost:9090`
- Writeback health endpoint on `localhost:9091`

## Upgrading

```bash
git pull
uv sync
inandout db upgrade
```

> **Important**: Always run `inandout db upgrade` before restarting daemons after an update. The daemons check the schema version at startup and refuse to start if the database schema is behind.

To check if migrations are pending without applying them:

```bash
inandout db status
```
