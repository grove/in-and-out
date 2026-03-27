# Writeback Hooks

Writeback hooks let you inject custom Python logic into the outbound sync pipeline — the path from the delta table to the external system — **without modifying the core engine**.

They are the writeback-side counterpart of the [Ingestion Hooks](./ingestion-hooks.md) and follow the same pattern.

---

## When to use writeback hooks

- Reshape a payload before it is POSTed/PATCHed to the external API
- Suppress specific writes based on business rules (without dead-lettering the record)
- Inject metadata (source system tag, audit timestamps, etc.)
- Implement connector-specific field mapping that doesn't fit the YAML transform model

---

## Hook types

Two hook types are available and applied in order for each outbound write:

| Hook | Signature | Purpose |
|---|---|---|
| `transform` | `async (payload: dict, action: str) → dict` | Mutate or reshape the payload; return the updated dict |
| `filter` | `async (payload: dict, action: str) → bool` | Return `False` to skip this write; the record is logged but **not** dead-lettered |

`action` is one of `"insert"`, `"update"`, or `"upsert"`.

Both hooks are **async**. Register only the ones you need — omitted hooks are skipped.

---

## Creating a writeback hook package

### 1. Write the hook factory

```python
# my_hooks/writeback_hooks.py
from inandout.writeback.hooks import WritebackHooks


async def _inject_source_system(payload: dict, action: str) -> dict:
    payload["source_system"] = "mdm"
    return payload


async def _skip_empty_payload(payload: dict, action: str) -> bool:
    return bool(payload)  # return False (skip) when payload is empty


def get_writeback_hooks() -> dict[str, WritebackHooks]:
    return {
        "hubspot": WritebackHooks(
            transform=_inject_source_system,
            filter=_skip_empty_payload,
        ),
        "salesforce": WritebackHooks(
            transform=_inject_source_system,
        ),
    }
```

Keys must match the `connector.name` field in the connector YAML.

### 2. Register the entry point

```toml
[project.entry-points."inandout.writeback_hooks"]
my_plugin = "my_hooks.writeback_hooks:get_writeback_hooks"
```

### 3. Install the package

```bash
pip install -e ./my_hooks_package
```

---

## Programmatic registration

You can also register hooks directly without an entry point:

```python
from inandout.writeback.hooks import WritebackHooks, register_writeback_hooks

register_writeback_hooks("hubspot", WritebackHooks(transform=my_transform))
```

This is useful for application-level customisation that doesn't need packaging.

---

## Relationship to ingestion hooks

| | Ingestion hooks | Writeback hooks |
|---|---|---|
| Entry-point group | `inandout.hooks` | `inandout.writeback_hooks` |
| Direction | inbound (external → database) | outbound (database → external) |
| Public API | `inandout.plugins.ConnectorHooks` | `inandout.writeback.hooks.WritebackHooks` |
| Hook args | `(record: dict)` | `(payload: dict, action: str)` |
| `enrich` hook | ✓ (with DB pool access) | — |
