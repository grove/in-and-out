"""CRDT merge helpers for writeback conflict resolution (T2 #6).

Supported CRDT types
--------------------
lww_register  Last-Write-Wins register.  Uses a per-document or per-field
              timestamp to decide which version is "newer".  If the remote
              state's timestamp is strictly greater than the local value's
              timestamp, the write is skipped (remote wins).  Otherwise the
              local payload is sent unchanged.

g_counter     Grow-only counter.  For each numeric field the engine sends
              only the *increment* (local - remote) rather than the absolute
              value, so the remote counter can only increase.  Non-numeric
              fields fall through as regular values.

Usage in WritebackConfig
------------------------
    crdt_type: lww_register          # or g_counter
    crdt_ts_field: _updated_at       # field name carrying the timestamp (LWW only)
"""
from __future__ import annotations

from typing import Any


def lww_merge(
    local: dict[str, Any],
    remote: dict[str, Any],
    ts_field: str = "_updated_at",
) -> dict[str, Any] | None:
    """Last-Write-Wins register merge.

    Compares the timestamp stored in *ts_field* across the local desired state
    and the remote current state.  Returns ``None`` to signal that the write
    should be skipped (remote is definitively newer); otherwise returns
    *local* unchanged (local is newer or the comparison is inconclusive).
    """
    local_ts = local.get(ts_field)
    remote_ts = remote.get(ts_field)
    if local_ts is not None and remote_ts is not None:
        # Compare as strings — ISO-8601 timestamps sort lexicographically
        if str(remote_ts) > str(local_ts):
            return None  # Remote wins — skip write
    return local


def gcounter_merge(
    local: dict[str, Any],
    remote: dict[str, Any],
) -> dict[str, Any]:
    """Grow-only counter merge.

    For each numeric field, sends only the *delta* (local − remote) when the
    local value is larger, ensuring the counter is monotonically increasing.
    Fields where local ≤ remote are omitted (server already has a higher
    value).  Non-numeric fields are forwarded as-is.
    """
    result: dict[str, Any] = {}
    for k, v in local.items():
        if k.startswith("_"):
            continue
        remote_v = remote.get(k)
        if isinstance(v, (int, float)) and isinstance(remote_v, (int, float)):
            delta = v - remote_v
            if delta > 0:
                result[k] = delta
            # delta <= 0 means remote already has this value or higher — skip
        else:
            result[k] = v
    return result


def crdt_merge(
    local: dict[str, Any],
    remote: dict[str, Any],
    crdt_type: str,
    ts_field: str = "_updated_at",
) -> dict[str, Any] | None:
    """Dispatch to the appropriate CRDT merge strategy.

    Returns ``None`` to signal "skip this write" (used by lww_register when
    remote is newer).  Returns a payload dict to be sent to the remote system.
    """
    if crdt_type == "lww_register":
        return lww_merge(local, remote, ts_field=ts_field)
    elif crdt_type == "g_counter":
        return gcounter_merge(local, remote)
    else:
        # Unknown type — fall through to normal write semantics
        return local
