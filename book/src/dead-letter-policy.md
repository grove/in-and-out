# Dead-Letter Policy

The dead-letter queue (DLQ) captures writeback rows that fail repeatedly so they don't block the pipeline. The **dead-letter policy** extension point lets you customise two decisions:

1. **Should this failure go to the DLQ at all?** (`should_dead_letter`)
2. **Should this dead-lettered row be replayed automatically?** (`should_replay`)

Policies are registered per connector name, with a global default applied to connectors that have no explicit policy.

---

## The protocol

```python
class DeadLetterPolicy(Protocol):
    def should_dead_letter(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        error_class: str,
        attempt_count: int,
    ) -> bool:
        """Return True to send the row to the DLQ; False to silently discard."""
        ...

    def should_replay(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        requeue_count: int,
        age_secs: float,
    ) -> bool:
        """Return True to include this row in the next automated replay pass."""
        ...
```

### Built-in default behaviour

The default policy (`_DefaultPolicy`) always dead-letters failures and always accepts rows for replay (up to the engine's retry limit).

---

## Registering a policy

### Per connector

```python
from inandout.deadletter.policy import DeadLetterPolicy, register_policy

class StrictPolicy:
    def should_dead_letter(self, connector, datatype, external_id, error_class, attempt_count) -> bool:
        # Permanently-invalid records — discard without dead-lettering
        if error_class == "data_error":
            return False
        return True

    def should_replay(self, connector, datatype, external_id, requeue_count, age_secs) -> bool:
        # Only replay within 24 hours and fewer than 5 attempts
        return requeue_count < 5 and age_secs < 86_400

register_policy(connector_name="hubspot", policy=StrictPolicy())
```

### As the global default

```python
from inandout.deadletter.policy import set_default_policy
set_default_policy(StrictPolicy())
```

The global default is used for any connector that has no per-connector policy registered.

---

## Policy lookup order

1. Per-connector policy (`register_policy(connector_name=...)`) — looked up by exact connector name.
2. Global default (`set_default_policy(...)`) — used when no per-connector match exists.
3. Built-in default — always dead-letters, always replays.

---

## Typical use cases

| Use case | Implementation |
|---|---|
| Skip DLQ for known-bad data | Return `False` from `should_dead_letter` when `error_class == "data_error"` |
| Limit replay attempts | Return `False` from `should_replay` when `requeue_count >= N` |
| Expire stale DLQ rows | Return `False` from `should_replay` when `age_secs > TTL` |
| Connector-specific retention | Register different policies per connector |

---

## Clearing policies (testing)

```python
from inandout.deadletter.policy import clear_policies
clear_policies()
```
