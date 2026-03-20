# Python Libraries & Frameworks for the Research Phase

**Date:** 20 March 2026
**Scope:** Recommended Python technology stack for implementing the In-and-Out declarative MDM synchronization tools during the research/prototyping phase.
**Context:** Python 3.13 was selected as the research-phase language per [REPORT_PROGRAMMING_LANGUAGES.md](REPORT_PROGRAMMING_LANGUAGES.md), Section 10.

---

## Table of Contents

1. [Design Principles for Library Selection](#1-design-principles-for-library-selection)
2. [Project Setup & Dependency Management](#2-project-setup--dependency-management)
3. [Async Runtime](#3-async-runtime)
4. [PostgreSQL](#4-postgresql)
5. [Database Migrations](#5-database-migrations)
6. [HTTP Client](#6-http-client)
7. [HTTP Server (Webhooks & Health Endpoints)](#7-http-server-webhooks--health-endpoints)
8. [Configuration & Validation](#8-configuration--validation)
9. [Expression Evaluation & Data Extraction](#9-expression-evaluation--data-extraction)
10. [Data Transformation & Diffing](#10-data-transformation--diffing)
11. [Scheduling](#11-scheduling)
12. [Observability — Logging](#12-observability--logging)
13. [Observability — Metrics](#13-observability--metrics)
14. [Observability — Tracing](#14-observability--tracing)
15. [CLI](#15-cli)
16. [Testing](#16-testing)
17. [Rate Limiting](#17-rate-limiting)
18. [Cryptography & Webhook Signatures](#18-cryptography--webhook-signatures)
19. [Type Checking & Code Quality](#19-type-checking--code-quality)
20. [Full Dependency List](#20-full-dependency-list)
21. [Architecture Sketch](#21-architecture-sketch)

---

## 1. Design Principles for Library Selection

Choices are guided by the research-phase priorities:

1. **Async-first.** The daemon processes run concurrent subsystems (HTTP server, polling loops, replication listener, control table poller). All I/O-bound libraries must support `async/await` natively — no blocking calls on the event loop.
2. **Minimal abstraction layers.** Prefer libraries that expose the underlying protocol clearly (e.g., raw SQL over an ORM) so the team understands what PostgreSQL is doing. An ORM hides too much during the research phase when we need to validate PostgreSQL-specific features (advisory locks, JSONB operators, logical replication, `REPLICA IDENTITY FULL`).
3. **Pydantic-centric.** Configuration validation is the single most important research deliverable. Every configuration structure — connector definitions, authentication schemes, pagination strategies, field mappings — should be a Pydantic model. This gives us schema validation, clear error messages, JSON Schema export, and documentation generation for free.
4. **Production-path-compatible.** Even in research, prefer libraries that are viable in production. Avoid toys or unmaintained projects that would need replacement if the Python implementation continues past the research phase.
5. **Fewest dependencies.** Prefer the standard library when it's adequate. Every additional dependency is a supply-chain risk and a maintenance burden.

---

## 2. Project Setup & Dependency Management

### Recommended: `uv` + `pyproject.toml`

| Tool | Role |
|---|---|
| **uv** (astral.sh) | Package manager, virtual environment manager, Python version manager, task runner |
| **pyproject.toml** | Single configuration file for project metadata, dependencies, tool settings |

**Why `uv`:**
- 10–100x faster than `pip` for dependency resolution and installation.
- Replaces `pip`, `pip-tools`, `virtualenv`, `pyenv`, and `poetry` with a single tool.
- Lockfile (`uv.lock`) ensures reproducible builds.
- `uv run` executes commands in the project's virtual environment without manual activation.
- Written in Rust — zero Python bootstrap dependency.

**Project structure:**

```
in-and-out/
├── pyproject.toml
├── uv.lock
├── src/
│   └── inandout/
│       ├── __init__.py
│       ├── config/           # Pydantic models for connector config
│       ├── engine/           # Core orchestration (scheduler, checkpointing)
│       ├── ingestion/        # Ingestion engine
│       ├── writeback/        # Writeback engine
│       ├── transport/        # Transport adapter interface + HTTP adapter
│       ├── postgres/         # PostgreSQL client, migrations, replication
│       ├── observability/    # Logging, metrics, tracing setup
│       ├── cli/              # CLI commands
│       └── simulators/       # HTTP stub server framework
├── tests/
│   ├── unit/
│   ├── integration/
│   └── simulators/           # Per-connector simulator implementations
├── connectors/               # Connector YAML definitions
└── migrations/               # Alembic migration scripts
```

**Why not Poetry / PDM / Hatch:**
`uv` has effectively replaced these tools as the community standard in 2025–2026. It's faster, simpler, and handles all the same use cases. Poetry's resolver is slow; PDM and Hatch have smaller ecosystems. `uv` is the forward-looking choice.

---

## 3. Async Runtime

### Recommended: `asyncio` (standard library) + `anyio` (abstraction layer)

| Library | Role |
|---|---|
| **asyncio** | Standard event loop — the foundation |
| **anyio** (3.x) | Structured concurrency, task groups, cancellation scopes |

**Why `anyio`:**
The standard `asyncio` module works, but `anyio` provides structured concurrency primitives that directly support the project's needs:

- **Task groups** (`anyio.create_task_group()`): Run the webhook server, polling scheduler, replication listener, and control table poller as a structured task group. If any sub-task crashes, the group cancels all siblings and propagates the error — preventing silent goroutine-leak equivalents.
- **Cancellation scopes**: Implement graceful shutdown drain with configurable timeout. `move_on_after(drain_seconds)` wraps the shutdown sequence cleanly.
- **Signal handling**: `anyio` integrates with SIGTERM/SIGINT handling.
- **Backend-agnostic**: Code written with `anyio` works on both `asyncio` and `trio` backends. We use `asyncio` but preserve the option.

`anyio` is already a transitive dependency of `httpx` and `starlette`, so it adds no new dependency.

**Why not `trio`:**
`trio` has better structured concurrency than raw `asyncio`, but library compatibility is weaker. `psycopg3` and `opentelemetry` assume `asyncio`. Using `trio` would force compatibility shims that add friction during research.

---

## 4. PostgreSQL

### Recommended: `psycopg` 3.2 (async mode)

| Library | Role |
|---|---|
| **psycopg[binary]** (3.2.x) | Async PostgreSQL client — queries, transactions, JSONB, advisory locks |
| **psycopg** logical replication API | Logical replication slot consumption for writeback triggers |
| **psycopg_pool** | Async connection pool |

**Why `psycopg` 3 (not `asyncpg`):**

Both are excellent async PostgreSQL drivers. `psycopg` 3 wins for this project because:

1. **Logical replication support.** `psycopg` 3 has a built-in API for consuming logical replication streams (`connection.stream(ReplicationSlot(...))`) — directly supporting T2 #10, #22, and #32. `asyncpg` has no logical replication API; you'd need to implement the streaming replication protocol manually.

2. **Advisory lock ergonomics.** `SELECT pg_advisory_lock(key)` and `pg_try_advisory_lock(key)` within async transactions work cleanly in `psycopg` 3. These are critical for T1 #36, T2 #36 (per-datatype concurrency control).

3. **JSONB-native.** `psycopg` 3 natively serialises/deserialises Python dicts to PostgreSQL JSONB and back, using `psycopg.types.json`. No manual `json.dumps()` wrappers needed.

4. **`COPY` protocol.** For bulk data loads (T1 #48 — bulk export API support), `psycopg` 3's async `COPY` support writes thousands of rows per second — substantially faster than individual `INSERT` statements.

5. **`LISTEN/NOTIFY`.** Native async support for PostgreSQL notifications — useful for the control table poller as an optimisation over polling.

6. **Server-side cursors.** For iterating over large result sets without loading all rows into memory — useful for full-sync diff operations (T1 #4, #13).

**Connection pool setup:**

```python
from psycopg_pool import AsyncConnectionPool

pool = AsyncConnectionPool(
    conninfo="postgresql://user:pass@localhost:5432/inandout",
    min_size=2,
    max_size=20,
    open=False,  # Open explicitly during startup
)
await pool.open()
```

**Advisory lock pattern:**

```python
async with pool.connection() as conn:
    acquired = await conn.execute(
        "SELECT pg_try_advisory_lock(%s)", [lock_key]
    )
    if not acquired.fetchone()[0]:
        logger.warning("Lock not acquired — another instance holds it")
        return
    try:
        # ... run sync operation ...
    finally:
        await conn.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
```

**Why not SQLAlchemy:**
SQLAlchemy is the dominant Python ORM/query builder, but it adds a thick abstraction layer over PostgreSQL that hides the raw SQL we need to understand during research. Advisory locks, logical replication, `REPLICA IDENTITY FULL`, JSONB operators, and atomic watermark writes are all easier to reason about in raw SQL via `psycopg`. If the project continues in Python past the research phase, SQLAlchemy Core (not the ORM) is a reasonable addition for query composition — but it should not be the starting point.

---

## 5. Database Migrations

### Recommended: `alembic` (1.14+)

| Library | Role |
|---|---|
| **alembic** | Schema migration management — version tracking, up/down migrations, autogeneration |

**Why `alembic`:**
- The standard Python migration tool — battle-tested, well-documented.
- Version tracking with a migration chain that maps to GOAL.md's schema migration coordination requirement.
- Supports `--sql` mode for generating SQL scripts without executing them — useful for operator review.
- `stamp` command sets the migration version without running migrations — supports the schema-version check at startup.
- Works directly with `psycopg` via custom `run_async` wrappers.

**Schema-version check at startup:**

```python
# At daemon startup — before any processing begins:
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext

def check_schema_version(connection):
    alembic_cfg = Config("alembic.ini")
    script = ScriptDirectory.from_config(alembic_cfg)
    context = MigrationContext.configure(connection)
    current = context.get_current_revision()
    head = script.get_current_head()
    if current != head:
        raise SystemExit(
            f"Schema version mismatch: database is at {current}, "
            f"tool expects {head}. Run migrations first."
        )
```

**Migration naming convention:**
Prefix migration files with a sequence number and date for readability: `001_20260320_initial_schema.py`, `002_20260321_add_watermark_table.py`.

**Why not raw SQL files:**
Hand-written SQL migration files are simpler but lack version tracking, dependency ordering, and the `stamp`/`check` workflow that the schema-version-at-startup requirement demands.

---

## 6. HTTP Client

### Recommended: `httpx` (0.28+)

| Library | Role |
|---|---|
| **httpx** | Async HTTP client — requests, auth, retries, streaming |
| **tenacity** | Retry/backoff decorator for transient failures |

**Why `httpx`:**

1. **Async-native.** `httpx.AsyncClient` is the best async HTTP client in Python. It shares the `requests`-like API that every Python developer knows, but adds `async/await` support natively.

2. **Connection pooling.** A single `AsyncClient` instance manages a connection pool per host — reusing TCP connections across requests to the same API. This is essential for rate-limited APIs where connection setup overhead matters.

3. **Auth framework.** `httpx` has a pluggable `Auth` base class. OAuth2 token refresh, API key injection, JWT signing, and custom pre-request auth flows (T1 #11, #24) can all be implemented as `Auth` subclasses:

    ```python
    class OAuth2Auth(httpx.Auth):
        async def async_auth_flow(self, request):
            if self.token_expired():
                await self.refresh_token()
            request.headers["Authorization"] = f"Bearer {self.access_token}"
            yield request
    ```

4. **Streaming responses.** For bulk export downloads (T1 #48), `httpx` supports `async for chunk in response.aiter_bytes()` — streaming large payloads without loading them entirely into memory.

5. **Transport layer is mockable.** `httpx`'s transport architecture allows injecting a mock transport for testing — directly supporting the simulator framework. The `respx` library hooks into this cleanly.

6. **HTTP/2 support.** Some modern APIs require or prefer HTTP/2. `httpx` supports it via `httpx[http2]`.

**Retry strategy with `tenacity`:**

```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception(is_retryable_error),
)
async def fetch_page(client, url, params):
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.json()
```

**Why not `aiohttp`:**
`aiohttp` is mature and fast but its API is less intuitive than `httpx`, error handling is more verbose, and it doesn't integrate as cleanly with `respx` for testing. `httpx` is the modern standard.

**Why not `requests`:**
`requests` is synchronous only. Using it from `asyncio` requires `run_in_executor()` which blocks a thread pool thread and defeats the purpose of async I/O.

---

## 7. HTTP Server (Webhooks & Health Endpoints)

### Recommended: `starlette` (0.45+) + `uvicorn` (0.34+)

| Library | Role |
|---|---|
| **starlette** | Lightweight ASGI framework — routing, middleware, WebSocket support |
| **uvicorn** | ASGI server — production-grade, supports TLS, graceful shutdown |

**Why `starlette` (not FastAPI):**

Both are built on ASGI and share the same foundation (FastAPI is built on top of Starlette). For this project, Starlette is the better choice because:

1. **Lighter weight.** The webhook receiver (T1 #42) and health endpoints are simple HTTP handlers — they don't need FastAPI's dependency injection, automatic OpenAPI generation, or request validation middleware. Starlette provides routing, middleware, and request/response handling with minimal overhead.

2. **Direct request body access.** Webhook signature verification (T1 #34) requires access to the raw request body before any parsing. Starlette's `request.body()` gives you the raw bytes. FastAPI's dependency injection parses the body before your handler sees it, requiring workarounds (like a middleware) to access the raw bytes for HMAC verification — this is a known pain point.

3. **Middleware composition.** Rate limiting (T1 #42), IP allowlisting, TLS, and signature verification are all best implemented as Starlette middleware. The middleware stack is clean and explicit:

    ```python
    app = Starlette(routes=[...])
    app = RateLimitMiddleware(app, max_requests=100, window=60)
    app = IPAllowlistMiddleware(app, allowed_ips=["1.2.3.0/24"])
    ```

4. **Multiple port binding.** The webhook receiver (T1 #42) and health endpoints (Cross-Cutting) must be on separate ports. This is achieved by running two Uvicorn instances in the same process — one for webhooks, one for health — each bound to its own port.

**Webhook receiver example:**

```python
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse

async def webhook_handler(request: Request) -> JSONResponse:
    body = await request.body()
    # Signature verification happens in middleware
    payload = orjson.loads(body)
    connector = request.path_params["connector"]
    await dispatch_webhook_event(connector, payload)
    return JSONResponse({"status": "accepted"}, status_code=200)

webhook_app = Starlette(routes=[
    Route("/webhook/{connector}", webhook_handler, methods=["POST"]),
])
```

**Health endpoint example:**

```python
async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"}, status_code=200)

async def ready(request: Request) -> JSONResponse:
    connectors = await get_connector_status()
    return JSONResponse({"connectors": connectors}, status_code=200)

health_app = Starlette(routes=[
    Route("/health", health, methods=["GET"]),
    Route("/ready", ready, methods=["GET"]),
])
```

**Running both on separate ports:**

```python
import uvicorn
import anyio

async def main():
    config_webhook = uvicorn.Config(webhook_app, host="0.0.0.0", port=8080)
    config_health = uvicorn.Config(health_app, host="0.0.0.0", port=9090)
    async with anyio.create_task_group() as tg:
        tg.start_soon(uvicorn.Server(config_webhook).serve)
        tg.start_soon(uvicorn.Server(config_health).serve)
```

**Why not FastAPI:**
FastAPI adds OpenAPI generation, Pydantic request validation, and dependency injection — all excellent for building user-facing APIs. But the webhook receiver is an internal infrastructure endpoint where we need raw body access, minimal latency, and full control over request processing. FastAPI's features add complexity without proportional value here. If a public management API is added later, FastAPI would be a natural choice for that surface.

---

## 8. Configuration & Validation

### Recommended: `pydantic` (2.10+) + `pydantic-settings` + `PyYAML`

| Library | Role |
|---|---|
| **pydantic** (v2) | Schema definition, validation, serialisation, JSON Schema export |
| **pydantic-settings** | Environment variable and `.env` file loading for runtime parameters |
| **PyYAML** (6.0+) | YAML parsing |
| **ruamel.yaml** | YAML round-tripping (preserve comments, ordering) — for config tooling |

**Why Pydantic v2 is the centrepiece:**

Pydantic v2 is the single most important library choice for this project. The connector configuration schema — covering authentication, pagination, field selection, response expressions, transformation rules, and operation definitions — is the primary research deliverable. Pydantic provides:

1. **Declarative schema definition** as Python classes:

    ```python
    from pydantic import BaseModel, Field
    from typing import Literal
    from enum import Enum

    class PaginationStrategy(str, Enum):
        offset = "offset"
        cursor = "cursor"
        link_header = "link_header"

    class PaginationConfig(BaseModel):
        strategy: PaginationStrategy
        page_size: int = Field(default=100, ge=1, le=10000)
        cursor_field: str | None = None
        offset_param: str = "offset"
        limit_param: str = "limit"
        stop_when: Literal["empty_page", "short_page", "no_next_link"] = "empty_page"
    ```

2. **Validation with clear error messages** — directly supporting T1 #43 and T2 #37 (connector validation mode):

    ```python
    try:
        config = ConnectorConfig.model_validate(yaml_data)
    except ValidationError as e:
        for error in e.errors():
            print(f"  Field: {' → '.join(str(l) for l in error['loc'])}")
            print(f"  Error: {error['msg']}")
    ```

3. **Discriminated unions** for polymorphic config sections — directly modelling the authentication scheme variants (T1 #11):

    ```python
    from pydantic import Discriminator

    class OAuth2Auth(BaseModel):
        type: Literal["oauth2"]
        token_url: str
        client_id: str
        scopes: list[str] = []

    class ApiKeyAuth(BaseModel):
        type: Literal["api_key"]
        header_name: str = "X-API-Key"
        key_ref: str  # Reference to credential store

    AuthConfig = Annotated[
        OAuth2Auth | ApiKeyAuth | JwtAuth | SessionTokenAuth,
        Discriminator("type"),
    ]
    ```

4. **JSON Schema export** for documentation and external tooling:

    ```python
    schema = ConnectorConfig.model_json_schema()
    # Produces a JSON Schema that documents every field, type, default, and constraint
    ```

5. **Custom validators** for complex cross-field validation:

    ```python
    @model_validator(mode="after")
    def check_pagination_cursor_field(self) -> Self:
        if self.pagination.strategy == "cursor" and not self.pagination.cursor_field:
            raise ValueError("cursor_field is required when strategy is 'cursor'")
        return self
    ```

**`pydantic-settings` for runtime parameters (T1 #28):**

```python
from pydantic_settings import BaseSettings

class RuntimeConfig(BaseSettings):
    database_url: str
    webhook_base_url: str  # T1 #28 — deployment-environment concern
    webhook_port: int = 8080
    health_port: int = 9090
    max_drain_seconds: int = 30

    model_config = SettingsConfigDict(env_prefix="INOUT_")
```

This lets operators set `INOUT_WEBHOOK_BASE_URL=https://hooks.example.com` in the environment, keeping deployment concerns out of connector config files.

**YAML loading:**

```python
import yaml
from pathlib import Path

def load_connector_config(path: Path) -> ConnectorConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return ConnectorConfig.model_validate(raw)
```

Use `yaml.safe_load()` always — never `yaml.load()` — to prevent arbitrary code execution from YAML files.

---

## 9. Expression Evaluation & Data Extraction

### Recommended: `jmespath` + `jsonpath-ng` + `fnmatch` (stdlib)

| Library | Role |
|---|---|
| **jmespath** (1.0+) | Primary expression language for extracting data from JSON responses |
| **jsonpath-ng** (1.6+) | JSONPath expressions where JMESPath is insufficient |
| **fnmatch** (stdlib) | Glob-style field selection patterns (T1 #21) |

**Why `jmespath` as the primary expression language:**

GOAL.md requires configurable expressions for: extracting record arrays from response envelopes (T1 #27), identifying primary key fields, extracting timestamps, and computing composite keys. JMESPath is purpose-built for this:

```yaml
# Connector config example:
datatypes:
  contacts:
    response_expression: "results[*]"           # Extract array from envelope
    primary_key_expression: "id"                 # Simple field
    timestamp_expression: "properties.updatedAt" # Nested field
    composite_key: "join('-', [type, id])"       # Composite expression
```

```python
import jmespath

expression = jmespath.compile(config.response_expression)
records = expression.search(response_json)
```

JMESPath advantages over raw JSONPath:
- **Richer expression language.** JMESPath supports projections, filters, function calls (`length()`, `sort_by()`, `join()`), and multi-select — enabling composite key expressions that JSONPath cannot express.
- **Read-only by design.** JMESPath cannot modify data, only extract it — eliminating injection risks. The expression language is inherently safe to evaluate against untrusted data.
- **Well-specified.** JMESPath has a formal grammar and compliance test suite. Implementations across languages behave identically.

**`jsonpath-ng` as a fallback:**
Some external APIs document JSONPath expressions in their documentation (e.g., `$.data.contacts[*]`). Supporting JSONPath as an alternative expression syntax lets connector authors copy expressions directly from API docs.

**Glob-style field selection (T1 #21):**

```python
from fnmatch import fnmatch

def should_include_field(field_path: str, patterns: list[str]) -> bool:
    for pattern in patterns:
        if pattern.startswith("!"):
            if fnmatch(field_path, pattern[1:]):
                return False
        elif fnmatch(field_path, pattern):
            return True
    return True  # Include by default if no explicit include patterns
```

**Why not embedding a scripting language (Lua, JavaScript):**
A general-purpose scripting engine (e.g., `lupa` for Lua, `PyMiniRacer` for V8) provides maximum flexibility but introduces injection risks, sandbox escapes, and performance unpredictability. JMESPath's read-only, side-effect-free design is a deliberate safety constraint.

---

## 10. Data Transformation & Diffing

### Recommended: `deepdiff` + `orjson`

| Library | Role |
|---|---|
| **deepdiff** (8.x) | Deep comparison of nested data structures — diffs, patches, hashing |
| **orjson** (3.x) | Fast JSON serialisation/deserialisation (10x faster than stdlib `json`) |

**Why `deepdiff`:**

Multiple requirements need structural comparison of nested JSON data:

- **T1 #4 (Deletion Tracking):** Diff previously known records against new full payloads to detect deletions.
- **T1 #31 (Schema Change Tracking):** Detect when API response structure changes — new fields, removed fields, type changes.
- **T2 #5 (Client-Side Patching):** Compute a minimal diff between current state and desired state to produce a PATCH payload.
- **T2 #4 (Base-Aware Updates):** Three-way merge between base, current, and desired states.

```python
from deepdiff import DeepDiff

diff = DeepDiff(
    current_state,
    desired_state,
    ignore_order=True,
    report_repetition=True,
)
if diff:
    patch_payload = extract_changed_fields(diff)
```

`deepdiff` also provides `DeepHash` — a content-addressable hash of any nested structure — directly supporting the `_raw_hash` column (T1 #2) for change detection:

```python
from deepdiff import DeepHash

raw_hash = DeepHash(raw_payload)[raw_payload]
```

**Why `orjson`:**

JSONB payloads are the project's primary data format. `orjson` is the fastest Python JSON library (written in Rust, 10x faster than `json` stdlib):

- Serialises Python dicts to bytes (not str) — reducing memory allocations.
- Natively handles `datetime`, `UUID`, `dataclass`, and `numpy` types.
- Supports `option=orjson.OPT_SORT_KEYS` for deterministic serialisation.

```python
import orjson

# Serialise
raw_bytes = orjson.dumps(payload)

# Deserialise
data = orjson.loads(response_body)

# Hash for change detection
import hashlib
raw_hash = hashlib.sha256(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)).hexdigest()
```

---

## 11. Scheduling

### Recommended: `APScheduler` (4.x)

| Library | Role |
|---|---|
| **APScheduler** (4.x) | Async-native job scheduler — cron expressions, interval triggers, job stores |

**Why APScheduler 4:**

APScheduler 4 (currently in beta, with stable releases expected in 2026) is a complete rewrite with async-first design. It directly supports T1 #37 and T2 #35 (sync scheduling):

- **Cron expressions:** `CronTrigger.from_crontab("*/5 * * * *")` for cron-style schedules.
- **Interval triggers:** `IntervalTrigger(minutes=5)` for fixed intervals.
- **Async execution:** Jobs are `async def` functions that run on the event loop — no thread pool required.
- **PostgreSQL job store:** Persists job state in PostgreSQL, supporting the multi-instance deployment model (jobs are coordinated across instances).
- **Externalizable:** Jobs can be added/removed/paused via the API at runtime — supporting the "externalizable scheduler" requirement.

```python
from apscheduler import AsyncScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = AsyncScheduler()

await scheduler.add_schedule(
    func=run_ingestion_sync,
    trigger=CronTrigger.from_crontab("*/5 * * * *"),
    id=f"sync_{connector}_{datatype}",
    kwargs={"connector": connector, "datatype": datatype},
)

await scheduler.start_in_background()
```

**Fallback: `croniter` + custom scheduler:**
If APScheduler 4's maturity is insufficient during the research phase, `croniter` (2.x) provides cron expression parsing and next-fire-time calculation, which can be combined with a simple `asyncio` loop:

```python
from croniter import croniter
from datetime import datetime, timezone

cron = croniter("*/5 * * * *", datetime.now(timezone.utc))
while True:
    next_fire = cron.get_next(datetime)
    await asyncio.sleep((next_fire - datetime.now(timezone.utc)).total_seconds())
    await run_sync()
```

This is less feature-rich (no job persistence, no multi-instance coordination) but is simple and adequate for research.

---

## 12. Observability — Logging

### Recommended: `structlog` (24.x)

| Library | Role |
|---|---|
| **structlog** | Structured, contextual logging with JSON output |

**Why `structlog`:**

GOAL.md requires structured JSON logs with `sync_run_id`, `connector`, and `datatype` on every log entry. `structlog` is purpose-built for this:

```python
import structlog

logger = structlog.get_logger()

# Bind context that appears on all subsequent log entries
log = logger.bind(connector="hubspot", datatype="contacts", sync_run_id=run_id)

log.info("sync_started", mode="incremental")
# Output: {"event": "sync_started", "mode": "incremental", "connector": "hubspot", "datatype": "contacts", "sync_run_id": "...", "timestamp": "..."}

log.info("page_fetched", page=3, records=100)
# Output: {"event": "page_fetched", "page": 3, "records": 100, "connector": "hubspot", ...}
```

Key features:
- **Context binding:** `logger.bind(key=value)` adds contextual fields to all subsequent log entries — no need to pass `connector` and `datatype` as parameters to every function.
- **JSON output:** `structlog.processors.JSONRenderer()` produces JSON log lines compatible with any log aggregation system.
- **PII masking:** Custom processors can hash or redact specific fields before logging — supporting the Data Privacy requirement.
- **Integration with stdlib `logging`:** `structlog` can wrap stdlib loggers, enabling integration with `uvicorn` and other libraries that use stdlib logging.

**PII masking processor:**

```python
def mask_pii(logger, method_name, event_dict):
    pii_fields = event_dict.pop("_pii_fields", set())
    for field in pii_fields:
        if field in event_dict:
            event_dict[field] = hashlib.sha256(
                str(event_dict[field]).encode()
            ).hexdigest()[:16]
    return event_dict
```

---

## 13. Observability — Metrics

### Recommended: `prometheus-client` (0.21+)

| Library | Role |
|---|---|
| **prometheus-client** | Prometheus metrics — counters, gauges, histograms, exposition |

**Why `prometheus-client`:**
- The standard Python Prometheus client. No alternatives are needed.
- Supports all metric types required by GOAL.md's Observability section.
- Built-in HTTP server (`start_http_server(port)`) for metrics exposition — or expose via the Starlette health app.

**Metrics matching GOAL.md requirements:**

```python
from prometheus_client import Counter, Gauge, Histogram

# Records processed / skipped / errored per run
records_processed = Counter(
    "inout_records_processed_total",
    "Records processed by outcome",
    ["connector", "datatype", "outcome"],  # outcome: written, skipped, errored
)

# Sync lag per datatype
sync_lag_seconds = Gauge(
    "inout_sync_lag_seconds",
    "Seconds since last successful sync",
    ["connector", "datatype"],
)

# HTTP error rates
http_errors = Counter(
    "inout_http_errors_total",
    "HTTP errors by status code",
    ["connector", "datatype", "status_code"],
)

# Circuit breaker state
circuit_breaker_state = Gauge(
    "inout_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["connector", "datatype"],
)

# Dead-letter queue depth
dlq_depth = Gauge(
    "inout_dead_letter_depth",
    "Unresolved dead-letter entries",
    ["tool", "connector", "datatype"],
)

# Replication slot lag (writeback)
replication_lag_bytes = Gauge(
    "inout_replication_lag_bytes",
    "Replication slot lag in bytes",
    [],
)
```

---

## 14. Observability — Tracing

### Recommended: OpenTelemetry Python SDK

| Library | Role |
|---|---|
| **opentelemetry-api** | Tracing API — span creation, context propagation |
| **opentelemetry-sdk** | Tracing SDK — span processors, exporters |
| **opentelemetry-exporter-otlp** | OTLP exporter (to Jaeger, Tempo, etc.) |
| **opentelemetry-instrumentation-httpx** | Auto-instrumentation for outbound `httpx` requests |
| **opentelemetry-instrumentation-psycopg** | Auto-instrumentation for `psycopg` database calls |
| **opentelemetry-instrumentation-asgi** | Auto-instrumentation for Starlette (webhook server) |

**Why OpenTelemetry:**
GOAL.md explicitly requires "distributed trace spans compatible with OpenTelemetry." The OpenTelemetry Python SDK is the standard implementation.

**Per-sync-run trace span:**

```python
from opentelemetry import trace

tracer = trace.get_tracer("inout.ingestion")

async def run_sync(connector: str, datatype: str, mode: str):
    with tracer.start_as_current_span(
        "sync_run",
        attributes={
            "inout.connector": connector,
            "inout.datatype": datatype,
            "inout.mode": mode,
        },
    ) as span:
        # Each page fetch creates a child span automatically via httpx instrumentation
        records = await fetch_all_pages(client, config)
        span.set_attribute("inout.records_fetched", len(records))
        await write_to_database(records)
```

The auto-instrumentation libraries are key — they create child spans for every outbound HTTP request and every PostgreSQL query without manual instrumentation, giving you a complete trace of a sync run from API call to database write.

---

## 15. CLI

### Recommended: `typer` (0.15+)

| Library | Role |
|---|---|
| **typer** | CLI framework — command routing, argument parsing, help generation |
| **rich** | Terminal formatting — tables, progress bars, coloured output |

**Why `typer`:**
Built on top of Click but uses Python type hints for argument declaration — consistent with the Pydantic-centric approach. Produces clean `--help` output automatically.

```python
import typer

app = typer.Typer()

@app.command()
def validate(
    connector: str = typer.Argument(help="Connector name"),
    config_path: Path = typer.Option("connectors/", help="Config directory"),
):
    """Validate a connector configuration (T1 #43, T2 #37)."""
    config = load_and_validate_config(connector, config_path)
    test_connectivity(config)
    test_authentication(config)
    dry_run_fetch(config)

@app.command()
def migrate(
    direction: str = typer.Argument(help="'up' or 'down'"),
):
    """Run database migrations."""
    run_alembic_migration(direction)

@app.command()
def sync_status():
    """Show current sync status for all connectors."""
    # Query sync-run log and display as a Rich table
```

**`rich` for operator-friendly output:**

```python
from rich.table import Table
from rich.console import Console

console = Console()
table = Table(title="Connector Status")
table.add_column("Connector")
table.add_column("Datatype")
table.add_column("Last Sync")
table.add_column("Status")
# ...
console.print(table)
```

---

## 16. Testing

### Recommended stack:

| Library | Role |
|---|---|
| **pytest** (8.x) | Test runner — the universal standard |
| **pytest-asyncio** (0.24+) | Async test support — `async def test_...()` |
| **respx** (0.22+) | Mock `httpx` requests — the simulator framework backbone |
| **testcontainers** (4.x) | Disposable PostgreSQL instances in Docker for integration tests |
| **pytest-cov** | Coverage reporting |
| **factory-boy** | Test data factories for Pydantic models |
| **hypothesis** | Property-based testing for config validation edge cases |

**`respx` as the simulator framework (Implementation Plan #3):**

`respx` mocks `httpx` at the transport layer — intercepting outbound requests and returning configured responses. This is the foundation for the simulator framework:

```python
import respx
import httpx

@respx.mock
async def test_incremental_sync():
    # Simulate a paginated API with 2 pages
    respx.get("https://api.example.com/contacts").mock(
        side_effect=[
            httpx.Response(200, json={
                "results": [{"id": "1", "name": "Alice"}],
                "paging": {"next": {"after": "cursor_1"}},
            }),
            httpx.Response(200, json={
                "results": [{"id": "2", "name": "Bob"}],
                "paging": {},
            }),
        ]
    )

    records = await run_ingestion(connector_config)
    assert len(records) == 2
    assert respx.calls.call_count == 2
```

For more complex simulators (multi-endpoint, stateful, auth flows), `respx` routes can be composed:

```python
@respx.mock
async def test_oauth2_with_token_refresh():
    # Token endpoint
    respx.post("https://auth.example.com/token").mock(
        return_value=httpx.Response(200, json={"access_token": "new_token", "expires_in": 3600})
    )
    # Protected data endpoint — first call returns 401, second succeeds
    data_route = respx.get("https://api.example.com/contacts")
    data_route.side_effect = [
        httpx.Response(401),
        httpx.Response(200, json={"results": []}),
    ]

    await run_ingestion(connector_config)
    assert data_route.call_count == 2  # Retried after token refresh
```

**`testcontainers` for PostgreSQL integration tests:**

```python
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgres:18.3") as pg:
        yield pg.get_connection_url()

@pytest.fixture
async def db_pool(postgres):
    pool = AsyncConnectionPool(conninfo=postgres)
    await pool.open()
    # Run migrations
    await run_migrations(pool)
    yield pool
    await pool.close()
```

This gives each test session a fresh PostgreSQL 18.3 instance in Docker — testing against the real database, not an in-memory mock. Advisory locks, JSONB operators, logical replication, and atomic watermark writes all work exactly as they will in production.

**`hypothesis` for config validation fuzz testing:**

```python
from hypothesis import given, strategies as st

@given(st.dictionaries(st.text(), st.from_type(int | str | list | dict | None)))
def test_connector_config_rejects_garbage(data):
    """Config validation must never crash — it must reject cleanly."""
    try:
        ConnectorConfig.model_validate(data)
    except ValidationError:
        pass  # Expected — graceful rejection
    # No unhandled exception = test passes
```

---

## 17. Rate Limiting

### Recommended: `aiolimiter` (1.2+)

| Library | Role |
|---|---|
| **aiolimiter** | Async token-bucket rate limiter — per-connector rate enforcement |

**Why `aiolimiter`:**
A simple async token-bucket implementation for rate limiting outbound HTTP requests (T1 #18, T2 #11):

```python
from aiolimiter import AsyncLimiter

# 10 requests per second for this connector
limiter = AsyncLimiter(max_rate=10, time_period=1)

async def rate_limited_request(client, url):
    async with limiter:
        return await client.get(url)
```

Token-bucket rate limiting is the standard approach — it allows bursts up to the bucket capacity while enforcing the average rate over time. Each connector gets its own limiter instance, configured from the connector YAML.

For `Retry-After` header handling (429 responses), the retry logic in `tenacity` handles the delay — `aiolimiter` handles the steady-state rate enforcement.

---

## 18. Cryptography & Webhook Signatures

### Recommended: `hashlib` + `hmac` (standard library)

| Library | Role |
|---|---|
| **hashlib** (stdlib) | SHA-256 hashing for `_raw_hash` column and content-addressable operations |
| **hmac** (stdlib) | HMAC-SHA256 for webhook signature verification (T1 #34) |
| **cryptography** (43+) | JWT signing/verification, encryption for credential store |

**Webhook signature verification (T1 #34):**

```python
import hmac
import hashlib

def verify_webhook_signature(
    body: bytes,
    signature_header: str,
    secret: bytes,
    algorithm: str = "sha256",
) -> bool:
    expected = hmac.new(secret, body, getattr(hashlib, algorithm)).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)
```

`hmac.compare_digest()` is essential — it provides constant-time comparison to prevent timing attacks.

**`cryptography` for JWT and credential encryption:**
- JWT signing (T1 #11): `cryptography` provides RSA and EC key operations for JWT-based auth.
- Credential store encryption: AES-GCM via `cryptography.fernet` for encrypting stored credentials.

The standard library covers 90% of the cryptographic needs. `cryptography` fills in JWT and symmetric encryption.

---

## 19. Type Checking & Code Quality

### Recommended: `mypy` + `ruff`

| Tool | Role |
|---|---|
| **mypy** (1.14+) | Static type checker — catches type errors at development time |
| **ruff** (0.9+) | Linter + formatter (replaces `flake8`, `isort`, `black`) — written in Rust, instant |

**`mypy` configuration** (`pyproject.toml`):

```toml
[tool.mypy]
python_version = "3.13"
strict = true
warn_return_any = true
warn_unused_configs = true
plugins = ["pydantic.mypy"]
```

The `pydantic.mypy` plugin enables type checking of Pydantic models — validating that field types match usage.

**`ruff` configuration** (`pyproject.toml`):

```toml
[tool.ruff]
target-version = "py313"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "S", "B", "A", "C4", "PT", "RUF"]
# S = security checks (bandit), B = bugbear, PT = pytest style
```

`ruff` replaces 5+ tools with a single Rust-based binary that lints and formats in milliseconds.

---

## 20. Full Dependency List

### Core dependencies (`pyproject.toml`):

```toml
[project]
name = "inandout"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    # Async runtime
    "anyio>=4.7",

    # PostgreSQL
    "psycopg[binary]>=3.2",
    "psycopg-pool>=3.2",

    # HTTP client
    "httpx>=0.28",
    "httpx[http2]",

    # HTTP server
    "starlette>=0.45",
    "uvicorn[standard]>=0.34",

    # Configuration & validation
    "pydantic>=2.10",
    "pydantic-settings>=2.7",
    "pyyaml>=6.0",

    # Expression evaluation
    "jmespath>=1.0",
    "jsonpath-ng>=1.6",

    # Data processing
    "orjson>=3.10",
    "deepdiff>=8.0",

    # Scheduling
    "apscheduler>=4.0",

    # Rate limiting
    "aiolimiter>=1.2",

    # Retry logic
    "tenacity>=9.0",

    # Observability - Logging
    "structlog>=24.0",

    # Observability - Metrics
    "prometheus-client>=0.21",

    # Observability - Tracing
    "opentelemetry-api>=1.29",
    "opentelemetry-sdk>=1.29",
    "opentelemetry-exporter-otlp>=1.29",
    "opentelemetry-instrumentation-httpx>=0.50b",
    "opentelemetry-instrumentation-psycopg>=0.50b",
    "opentelemetry-instrumentation-asgi>=0.50b",

    # CLI
    "typer>=0.15",
    "rich>=13.0",

    # Cryptography
    "cryptography>=43",

    # Database migrations
    "alembic>=1.14",
]
```

### Development dependencies:

```toml
[project.optional-dependencies]
dev = [
    # Testing
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "respx>=0.22",
    "testcontainers[postgres]>=4.0",
    "pytest-cov>=6.0",
    "hypothesis>=6.0",
    "factory-boy>=3.3",

    # Type checking & linting
    "mypy>=1.14",
    "ruff>=0.9",

    # YAML round-tripping (for config tooling)
    "ruamel.yaml>=0.18",
]
```

### Dependency count:
- **Core:** 25 direct dependencies (several are lightweight or standard-library-adjacent)
- **Development:** 9 additional dev dependencies
- **Total transitive:** Approximately 80–100 packages (most from OpenTelemetry's dependency tree)

---

## 21. Architecture Sketch

How these libraries compose into the daemon architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Daemon Process                           │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    anyio Task Group                        │  │
│  │                                                           │  │
│  │  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐   │  │
│  │  │  Webhook     │  │  Scheduler   │  │  Control Table │   │  │
│  │  │  Server      │  │  (APScheduler│  │  Poller        │   │  │
│  │  │  (starlette  │  │   or custom) │  │  (psycopg +    │   │  │
│  │  │   + uvicorn) │  │              │  │   LISTEN/      │   │  │
│  │  │              │  │              │  │   NOTIFY)       │   │  │
│  │  │  Port 8080   │  │              │  │                │   │  │
│  │  └──────┬───────┘  └──────┬───────┘  └───────┬────────┘   │  │
│  │         │                 │                   │            │  │
│  │         ▼                 ▼                   ▼            │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │              Engine Orchestration Layer               │  │  │
│  │  │  • Config loading (pydantic)                         │  │  │
│  │  │  • Concurrency control (pg advisory locks)           │  │  │
│  │  │  • Circuit breaker state machine                     │  │  │
│  │  │  • Error classification & retry (tenacity)           │  │  │
│  │  │  • Rate limiting (aiolimiter)                        │  │  │
│  │  │  • Checkpointing                                     │  │  │
│  │  │  • Observability (structlog + prometheus + OTel)      │  │  │
│  │  └──────────────────────┬───────────────────────────────┘  │  │
│  │                         │                                  │  │
│  │                         ▼                                  │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │            Transport Adapter Interface                │  │  │
│  │  │                                                      │  │  │
│  │  │  ┌──────────────────────────────────────────────┐    │  │  │
│  │  │  │  HTTP Adapter (httpx)                        │    │  │  │
│  │  │  │  • Auth (OAuth2, API key, JWT, session)      │    │  │  │
│  │  │  │  • Pagination (offset, cursor, link-header)  │    │  │  │
│  │  │  │  • Expression evaluation (jmespath)          │    │  │  │
│  │  │  │  • Response parsing (orjson)                 │    │  │  │
│  │  │  └──────────────────────────────────────────────┘    │  │  │
│  │  └──────────────────────┬───────────────────────────────┘  │  │
│  │                         │                                  │  │
│  │                         ▼                                  │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │            PostgreSQL Layer (psycopg 3)              │  │  │
│  │  │  • Connection pool (psycopg_pool)                    │  │  │
│  │  │  • JSONB read/write                                  │  │  │
│  │  │  • Atomic watermark updates                          │  │  │
│  │  │  • Advisory locks                                    │  │  │
│  │  │  • Logical replication (writeback tool)              │  │  │
│  │  │  • Schema migrations (alembic)                       │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  │                                                           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌─────────────┐                                                │
│  │ Health App   │  Port 9090  (starlette + uvicorn)             │
│  │ /health      │                                                │
│  │ /ready       │                                                │
│  │ /metrics     │  (prometheus-client exposition)                │
│  └─────────────┘                                                │
│                                                                 │
│  ┌─────────────┐                                                │
│  │ CLI (typer)  │  Separate entry point — connects to same PG   │
│  │ • validate   │                                                │
│  │ • migrate    │                                                │
│  │ • status     │                                                │
│  │ • control    │                                                │
│  └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key architectural boundaries:**

1. **Pydantic models** define every data structure that crosses a boundary: connector configs, sync-run records, control table commands, watermark entries, dead-letter entries.
2. **The transport adapter interface** is a Python `Protocol` (structural typing) — no base class inheritance required. The HTTP adapter is the first implementation; the interface is designed so a Kafka or database adapter could be added later without changing the engine.
3. **`psycopg` 3** is the sole database access layer — no ORM. All SQL is explicit, all transactions are explicit, all advisory locks are explicit.
4. **`anyio.TaskGroup`** is the top-level concurrency coordinator. Graceful shutdown cancels the task group, which cancels all sub-tasks, each of which completes its current operation before exiting.

---

*Library versions as of March 2026. All recommendations target Python 3.13 with `uv` as the package manager. The stack prioritises research-phase agility — fast iteration on config schemas, rapid prototyping of engine logic, and comprehensive testing against simulated and real PostgreSQL instances.*
