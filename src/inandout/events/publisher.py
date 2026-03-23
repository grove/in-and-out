"""Event publisher implementations."""
from __future__ import annotations

import datetime
import sys
from abc import ABC, abstractmethod
from typing import Any

import orjson
import structlog

from inandout.events.config import EventOutputConfig

logger = structlog.get_logger(__name__)


class EventPublisher(ABC):
    """Base class for event publishers."""

    @abstractmethod
    async def publish(self, event: dict[str, Any]) -> None:
        """Publish a single event dict."""


class StdoutPublisher(EventPublisher):
    """Writes events as JSON lines to stdout. Useful for testing/dev."""

    async def publish(self, event: dict[str, Any]) -> None:
        line = orjson.dumps(event).decode()
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


class PgNotifyPublisher(EventPublisher):
    """Publishes events via PostgreSQL NOTIFY."""

    def __init__(self, pool: Any, channel: str = "inandout_events") -> None:
        self._pool = pool
        self._channel = channel

    async def publish(self, event: dict[str, Any]) -> None:
        payload = orjson.dumps(event).decode()
        # Escape single quotes in payload
        safe_payload = payload.replace("'", "''")
        query = f"NOTIFY {self._channel}, '{safe_payload}'"
        try:
            async with self._pool.connection() as conn:
                await conn.execute(query)
                await conn.commit()
        except Exception as exc:
            logger.warning("pg_notify_failed", channel=self._channel, error=str(exc))


class KafkaPublisher(EventPublisher):
    """Kafka publisher stub — requires aiokafka."""

    def __init__(self, topic: str, connection_string: str | None) -> None:
        try:
            import aiokafka  # noqa: F401
        except ImportError as exc:
            raise NotImplementedError(
                "KafkaPublisher requires 'aiokafka'. Install it with: uv add aiokafka"
            ) from exc
        self._topic = topic
        self._connection_string = connection_string

    async def publish(self, event: dict[str, Any]) -> None:
        raise NotImplementedError(
            "KafkaPublisher is a stub. Install 'aiokafka' and implement a producer."
        )


class KinesisPublisher(EventPublisher):
    """Kinesis publisher stub — requires aioboto3."""

    def __init__(self, topic: str, connection_string: str | None) -> None:
        try:
            import aioboto3  # noqa: F401
        except ImportError as exc:
            raise NotImplementedError(
                "KinesisPublisher requires 'aioboto3'. Install it with: uv add aioboto3"
            ) from exc
        self._topic = topic
        self._connection_string = connection_string

    async def publish(self, event: dict[str, Any]) -> None:
        raise NotImplementedError(
            "KinesisPublisher is a stub. Install 'aioboto3' and implement a producer."
        )


def get_publisher(
    config: EventOutputConfig,
    pool: Any = None,
) -> EventPublisher:
    """Factory: return an EventPublisher for the configured backend."""
    if config.backend == "stdout":
        return StdoutPublisher()
    if config.backend == "pg_notify":
        return PgNotifyPublisher(pool=pool, channel=config.channel)
    if config.backend == "kafka":
        return KafkaPublisher(topic=config.topic, connection_string=config.connection_string)
    if config.backend == "kinesis":
        return KinesisPublisher(topic=config.topic, connection_string=config.connection_string)
    raise ValueError(f"Unknown event backend: {config.backend!r}")


def build_event(
    connector: str,
    datatype: str,
    external_id: str,
    action: str,
    run_id: str,
    raw: dict[str, Any] | None = None,
    include_raw: bool = False,
) -> dict[str, Any]:
    """Build a standard event dict."""
    event: dict[str, Any] = {
        "connector": connector,
        "datatype": datatype,
        "external_id": external_id,
        "action": action,
        "run_id": run_id,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if include_raw and raw is not None:
        event["raw"] = raw
    return event
