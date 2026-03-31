"""Component-state gate for schema-manager coordination.

Each component (ingest, writeback) polls the ``component_state`` table to
decide whether it should run, pause, or enter shadow mode.  A PostgreSQL
advisory lock barrier lets the schema-manager block until all replicas have
finished their current work cycle before applying DDL.

Advisory lock keys (session-scoped):
  0x5E5A0001  ingest barrier
  0x5E5A0002  writeback barrier
"""
from __future__ import annotations

from typing import Literal

import psycopg
import structlog
from psycopg_pool import AsyncConnectionPool

logger = structlog.get_logger(__name__)

INGEST_LOCK_KEY = 0x5E5A_0001
WRITEBACK_LOCK_KEY = 0x5E5A_0002

ComponentName = Literal["ingest", "writeback"]
DesiredState = Literal["running", "stopped", "shadow"]


def _lock_key(component: ComponentName) -> int:
    return INGEST_LOCK_KEY if component == "ingest" else WRITEBACK_LOCK_KEY


async def get_desired_state(
    pool: AsyncConnectionPool,
    component: ComponentName,
) -> DesiredState | None:
    """Read the ``desired`` column from ``component_state``.

    Returns ``None`` if the table doesn't exist or the row is missing
    (schema-manager hasn't started yet — caller should treat this as
    'stopped' and wait).
    """
    async with pool.connection() as conn:
        try:
            row = await conn.execute(
                "SELECT desired FROM component_state WHERE component = %s",
                (component,),
            )
            result = await row.fetchone()
            return result[0] if result else None
        except psycopg.errors.UndefinedTable:
            # Table doesn't exist yet — schema-manager hasn't run self-upgrade
            return None


class ComponentGate:
    """Wrap a single work cycle with advisory-lock-gated state checking.

    Usage inside a polling loop::

        gate = ComponentGate(pool, "ingest")
        while True:
            async with gate.work_cycle() as allowed:
                if not allowed:
                    await anyio.sleep(5)
                    continue
                # ... do one work cycle ...
    """

    def __init__(self, pool: AsyncConnectionPool, component: ComponentName) -> None:
        self._pool = pool
        self._component = component
        self._lock_key = _lock_key(component)
        self._log = logger.bind(component=component, gate="component_state")

    @property
    def component(self) -> ComponentName:
        return self._component

    async def desired_state(self) -> DesiredState:
        state = await get_desired_state(self._pool, self._component)
        if state is None:
            return "stopped"  # safe default when schema-manager hasn't started
        return state

    class _CycleContext:
        """Async context manager for a single gated work cycle."""

        def __init__(self, gate: "ComponentGate") -> None:
            self._gate = gate
            self._conn: psycopg.AsyncConnection | None = None
            self.allowed: bool = False
            self.state: DesiredState = "stopped"

        async def __aenter__(self) -> "_CycleContext":
            state = await self._gate.desired_state()
            self.state = state
            if state == "stopped":
                self.allowed = False
                return self

            # Acquire shared advisory lock — blocks if schema-manager holds
            # the exclusive lock (migration in progress).
            self._conn = await self._gate._pool.getconn()
            await self._conn.execute(
                "SELECT pg_advisory_lock_shared(%s)", (self._gate._lock_key,)
            )

            # Double‐check after lock acquisition (TOCTOU guard): the
            # schema-manager may have set 'stopped' between our first check
            # and when we acquired the lock.
            state = await self._gate.desired_state()
            self.state = state
            if state == "stopped":
                await self._conn.execute(
                    "SELECT pg_advisory_unlock_shared(%s)", (self._gate._lock_key,)
                )
                await self._gate._pool.putconn(self._conn)
                self._conn = None
                self.allowed = False
                return self

            self.allowed = True
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
            if self._conn is not None:
                try:
                    await self._conn.execute(
                        "SELECT pg_advisory_unlock_shared(%s)", (self._gate._lock_key,)
                    )
                finally:
                    await self._gate._pool.putconn(self._conn)
                    self._conn = None
            return None  # don't suppress exceptions

        def __bool__(self) -> bool:
            return self.allowed

    def work_cycle(self) -> "_CycleContext":
        """Return an async context manager for one gated work cycle."""
        return self._CycleContext(self)
