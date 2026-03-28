# justfile — common development commands for in-and-out
# Install just: https://github.com/casey/just
# Usage: just <recipe>

set dotenv-load := true

# Default: list available recipes
default:
    @just --list

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

# Install all dependencies (including dev) using uv
install:
    uv sync --all-packages --all-extras

# Install production dependencies only
install-prod:
    uv sync --all-packages --no-dev

# Show the active Python and uv versions
versions:
    uv run python --version
    uv --version

# ---------------------------------------------------------------------------
# Formatting & Linting
# ---------------------------------------------------------------------------

# Format code with ruff
fmt:
    uv run --package inandout ruff format engine/src engine/tests
    uv run --package inandout ruff format simulator/src

# Lint code with ruff (auto-fix safe issues)
lint:
    uv run --package inandout ruff check --fix engine/src engine/tests

# Lint without auto-fix (CI mode)
lint-check:
    uv run --package inandout ruff check engine/src engine/tests

# Type-check with mypy
typecheck:
    uv run --package inandout mypy engine/src

# Run all code quality checks (no auto-fix)
check: lint-check typecheck

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

# Run unit tests
test:
    uv run --package inandout pytest engine/tests/unit -v

# Run unit tests with coverage report
test-cov:
    uv run --package inandout pytest engine/tests/unit --cov=engine/src/inandout --cov-report=term-missing --cov-report=html -v

# Run integration tests (requires running Postgres — use `just up` first)
test-integration:
    uv run --package inandout pytest engine/tests/integration -v

# Run contract tests
test-contract:
    uv run --package inandout pytest engine/tests/contract -v

# Run acceptance tests (requires real external APIs)
test-acceptance:
    uv run --package inandout pytest engine/tests/acceptance -v -m acceptance

# Run load tests
test-load:
    uv run --package inandout pytest engine/tests/load -v -m load

# Run all engine tests except acceptance and load
test-all:
    uv run --package inandout pytest engine/tests -v -m "not acceptance and not load"

# Run simulator tests
test-simulator:
    uv run --package inandout-simulator pytest simulator/tests -v

# Run a specific test file or expression (e.g.: just test-one engine/tests/unit/test_foo.py)
test-one path:
    uv run pytest {{ path }} -v

# ---------------------------------------------------------------------------
# Database / Migrations
# ---------------------------------------------------------------------------

# Apply all pending migrations
db-upgrade:
    cd engine && uv run --package inandout alembic upgrade head

# Roll back the last migration
db-downgrade:
    cd engine && uv run --package inandout alembic downgrade -1

# Show the current migration revision
db-current:
    cd engine && uv run --package inandout alembic current

# Show migration history
db-history:
    cd engine && uv run --package inandout alembic history --verbose

# Auto-generate a new migration (usage: just db-revision "add my table")
db-revision msg:
    cd engine && uv run --package inandout alembic revision --autogenerate -m "{{ msg }}"

# ---------------------------------------------------------------------------
# Docker — local development stack
# ---------------------------------------------------------------------------

# Start the full dev stack (postgres + services)
up:
    docker compose up -d

# Start only the postgres service
up-db:
    docker compose up -d postgres

# Stop all services
down:
    docker compose down

# Stop and remove volumes (destructive — resets database)
down-clean:
    docker compose down -v

# ---------------------------------------------------------------------------
# Demo simulator
# ---------------------------------------------------------------------------

# Run the stateful demo simulator locally (no engine required)
simulator:
    uv run inandout-simulator \
      --connector connectors/hubspot.example.yaml \
      --connector connectors/tripletex.example.yaml \
      --listen 0.0.0.0:6100 \
      --reload

# Start the full demo stack: simulator + postgres + engine (requires Docker)
demo:
    docker compose --profile demo up -d

# Tail logs from all services
logs:
    docker compose logs -f

# Tail logs from a specific service (e.g.: just logs-svc ingest)
logs-svc svc:
    docker compose logs -f {{ svc }}

# Rebuild images without cache
build:
    docker compose build --no-cache

# ---------------------------------------------------------------------------
# Docker — observability stack
# ---------------------------------------------------------------------------

# Start the observability stack (Prometheus + Grafana + Alertmanager)
up-obs:
    docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d

# Stop the observability stack
down-obs:
    docker compose -f docker-compose.yml -f docker-compose.observability.yml down

# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

# Run the ingest service locally
ingest:
    uv run --package inandout inandout ingest run --config engine/config/ingestion.yaml

# Run the writeback service locally
writeback:
    uv run --package inandout inandout writeback run --config engine/config/writeback.yaml

# Show inandout CLI help
cli-help:
    uv run inandout --help

# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------

# Build mdBook documentation locally into docs-build/
docs-build:
    @command -v mdbook >/dev/null || (echo "mdbook is required. Install with: brew install mdbook" && exit 1)
    mdbook build book

# Serve mdBook documentation locally with live reload (http://127.0.0.1:3000)
docs-serve:
    @command -v mdbook >/dev/null || (echo "mdbook is required. Install with: brew install mdbook" && exit 1)
    mdbook serve book --open

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

# Validate connector fixture files
validate-connectors:
    uv run python scripts/validate_connector_fixtures.py

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

# Remove Python bytecode caches
clean:
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage

# Full CI pipeline: format check → lint → typecheck → engine unit tests → simulator tests
ci: lint-check typecheck test test-simulator
