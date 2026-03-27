"""Dead-letter acceptance and replay policy hooks.

Register a custom ``DeadLetterPolicy`` to control:

- Whether a failing record should be sent to the dead-letter queue
  (``should_dead_letter``).
- Whether a dead-lettered row is eligible for automated replay
  (``should_replay``).

Policies are registered per connector name.  A global default policy is used
when no per-connector policy is registered.

Usage::

    from inandout.deadletter.policy import DeadLetterPolicy, register_policy

    class StrictPolicy:
        def should_dead_letter(
            self, connector, datatype, external_id, error_class, attempt_count
        ) -> bool:
            # Permanently-invalid records: skip DL entirely
            if error_class == "data_error":
                return False
            return True

        def should_replay(
            self, connector, datatype, external_id, requeue_count, age_secs
        ) -> bool:
            # Only replay within 24 hours and fewer than 5 attempts
            return requeue_count < 5 and age_secs < 86400

    register_policy(connector_name="hubspot", policy=StrictPolicy())

    # Or set a global default for all connectors:
    from inandout.deadletter.policy import set_default_policy
    set_default_policy(StrictPolicy())
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

import structlog

logger = structlog.get_logger(__name__)


@runtime_checkable
class DeadLetterPolicy(Protocol):
    """Policy interface for dead-letter queue acceptance and replay decisions."""

    def should_dead_letter(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        error_class: str,
        attempt_count: int,
    ) -> bool:
        """Return ``True`` to route this failing record to the dead-letter queue.

        Parameters
        ----------
        connector:
            Connector name (matches ``connector.name`` in the YAML).
        datatype:
            Datatype name.
        external_id:
            The external ID of the failing record.
        error_class:
            One of ``"data_error"``, ``"http_error"``, ``"conflict:dead_letter"``,
            or another engine-defined error class string.
        attempt_count:
            Number of previous failures for this external_id in the writeback
            audit log.
        """
        ...

    def should_replay(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        requeue_count: int,
        age_secs: float,
    ) -> bool:
        """Return ``True`` to allow replaying this dead-lettered row.

        Parameters
        ----------
        connector:
            Connector name.
        datatype:
            Datatype name.
        external_id:
            The external ID of the dead-lettered record.
        requeue_count:
            How many times this row has already been replayed.
        age_secs:
            Seconds since the row was first dead-lettered.
        """
        ...


class _DefaultPolicy:
    """Built-in policy: dead-letter everything, replay up to _MAX_DL_REQUEUE_COUNT times."""

    def should_dead_letter(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        error_class: str,
        attempt_count: int,
    ) -> bool:
        return True

    def should_replay(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        requeue_count: int,
        age_secs: float,
    ) -> bool:
        try:
            from inandout.deadletter.writeback import _MAX_DL_REQUEUE_COUNT
            return requeue_count < _MAX_DL_REQUEUE_COUNT
        except Exception:
            return requeue_count < 3


# Per-connector policy map
_policies: dict[str, DeadLetterPolicy] = {}
_default_policy: DeadLetterPolicy = _DefaultPolicy()


def register_policy(connector_name: str, policy: DeadLetterPolicy) -> None:
    """Register *policy* for *connector_name*, replacing any existing entry."""
    _policies[connector_name] = policy


def get_policy(connector_name: str) -> DeadLetterPolicy:
    """Return the registered policy for *connector_name*, or the default policy."""
    return _policies.get(connector_name, _default_policy)


def set_default_policy(policy: DeadLetterPolicy) -> None:
    """Replace the global default policy used when no per-connector policy is registered."""
    global _default_policy
    _default_policy = policy


def clear_policies() -> None:
    """Remove all registered policies and restore the built-in default.

    Intended for use in tests only.
    """
    global _default_policy
    _policies.clear()
    _default_policy = _DefaultPolicy()
