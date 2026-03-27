# Plugin Hooks

Plugin hooks let you inject custom Python logic into the ingestion pipeline **without modifying the core engine**. They are the right tool when:

- You need to reshape or normalise a record before it lands in PostgreSQL
- You want to drop records that don't meet a business rule
- You need to augment records from a secondary data source (another database, a cache, etc.)

Hooks operate at the **record level** — they run for every ingested record of a named connector.

---

## Hook types

Three hook types are available, applied in this order for each record:

| Hook | Signature | Purpose |
|---|---|---|
| `transform` | `async (record: dict) → dict` | Mutate or reshape the record; return the updated dict |
| `filter` | `async (record: dict) → bool` | Return `False` to drop the record before it is upserted |
| `enrich` | `async (record: dict, pool: AsyncConnectionPool) → dict` | Augment the record using the database pool or any other async I/O |

All hooks are **async**. You may register any combination — handlers you omit are skipped.

---

## Creating a plugin package

Hooks are distributed as ordinary Python packages and discovered automatically at daemon startup via the `inandout.hooks` [entry-point group](https://packaging.python.org/en/latest/specifications/entry-points/).

### 1. Write the hook factory

Create a Python package with a factory function that returns a `dict[str, ConnectorHooks]`. Keys must match the `connector.name` field in the connector YAML.

```python
# my_hooks/hooks.py
from inandout.plugins.hooks import ConnectorHooks


async def _uppercase_name(record: dict) -> dict:
    if "name" in record:
        record["name"] = record["name"].upper()
    return record


async def _drop_deleted(record: dict) -> bool:
    return not record.get("is_deleted", False)


def get_hooks() -> dict[str, ConnectorHooks]:
    return {
        "hubspot": ConnectorHooks(
            transform=_uppercase_name,
            filter=_drop_deleted,
        ),
    }
```

### 2. Register the entry point

In your package's `pyproject.toml`:

```toml
[project.entry-points."inandout.hooks"]
my_plugin = "my_hooks.hooks:get_hooks"
```

The key (`my_plugin`) is an arbitrary label — it only needs to be unique across installed plugins. The value points to the factory function.

### 3. Install the package

```bash
pip install -e ./my_hooks_package
# or, for production:
pip install my-hooks-package
```

The package must be installed into the **same Python environment** as in-and-out. Hooks are discovered via `importlib.metadata` at daemon startup — no restart is required after installation if you are using the development server, but a full restart is needed in production.

---

## Hook execution order

For each ingested record the pipeline runs:

```
raw record
    │
    ▼
transform(record) → record′          # or no-op if not registered
    │
    ▼
filter(record′) → True / False       # False drops the record
    │
    ▼
enrich(record′, pool) → record″      # or no-op if not registered
    │
    ▼
upsert into PostgreSQL
```

If `filter` returns `False` the record is discarded silently — it does not appear in the dead-letter queue and is not counted as an error.

---

## Notes and constraints

- Hook functions must be **async** (`async def`). Synchronous callables are not supported.
- The `transform` and `filter` hooks receive the **parsed, normalised** record dict as produced by the engine — not the raw HTTP response body.
- The `enrich` hook receives an `AsyncConnectionPool` (from [psycopg-pool](https://www.psycopg.org/psycopg3/docs/advanced/pool.html)). Use it to query the PostgreSQL instance that in-and-out writes to.
- Exceptions raised inside a hook propagate to the engine's standard error handling — the sync run will log the error and the record will be sent to the dead-letter queue, just as if the HTTP fetch had failed.
- Multiple installed packages may register hooks for different connectors. If two packages register hooks for the **same connector name**, the last package loaded wins (log output will warn you).
