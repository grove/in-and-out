"""Change Data Capture (CDC) source implementations."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class CdcSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: str  # "kafka" | "kinesis" | "pg_logical"
    connection_string: str
    topic_or_stream: str
    consumer_group: str = "inandout"


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class CdcSource(ABC):
    """Abstract CDC source — produces batches of change records."""

    @abstractmethod
    async def consume(self, batch_size: int, timeout_secs: float) -> list[dict]:
        """Consume up to *batch_size* records, waiting up to *timeout_secs*."""
        ...

    @abstractmethod
    async def commit(self) -> None:
        """Commit the last consumed batch (acknowledge offsets)."""
        ...


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------


class KafkaCdcSource(CdcSource):
    """Kafka-backed CDC source using aiokafka."""

    def __init__(self, cfg: CdcSourceConfig) -> None:
        try:
            import aiokafka  # noqa: F401
        except ImportError as exc:
            raise NotImplementedError(
                "Install aiokafka: pip install aiokafka"
            ) from exc
        self._cfg = cfg
        self._consumer: Any = None

    async def _ensure_consumer(self) -> Any:
        from aiokafka import AIOKafkaConsumer

        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                self._cfg.topic_or_stream,
                bootstrap_servers=self._cfg.connection_string,
                group_id=self._cfg.consumer_group,
                enable_auto_commit=False,
                value_deserializer=lambda v: __import__("orjson").loads(v),
            )
            await self._consumer.start()
        return self._consumer

    async def consume(self, batch_size: int = 100, timeout_secs: float = 5.0) -> list[dict]:
        consumer = await self._ensure_consumer()
        records: list[dict] = []
        try:
            batch = await consumer.getmany(timeout_ms=int(timeout_secs * 1000), max_records=batch_size)
            for tp_msgs in batch.values():
                for msg in tp_msgs:
                    records.append(msg.value)
        except Exception:
            pass
        return records

    async def commit(self) -> None:
        if self._consumer is not None:
            await self._consumer.commit()


# ---------------------------------------------------------------------------
# Kinesis
# ---------------------------------------------------------------------------


class KinesisCdcSource(CdcSource):
    """Kinesis-backed CDC source using aioboto3."""

    def __init__(self, cfg: CdcSourceConfig) -> None:
        try:
            import aioboto3  # noqa: F401
        except ImportError as exc:
            raise NotImplementedError(
                "Install aioboto3: pip install aioboto3"
            ) from exc
        self._cfg = cfg
        self._shard_iterator: str | None = None

    async def consume(self, batch_size: int = 100, timeout_secs: float = 5.0) -> list[dict]:
        import aioboto3
        import orjson

        session = aioboto3.Session()
        async with session.client("kinesis") as client:
            if self._shard_iterator is None:
                shards_resp = await client.list_shards(StreamName=self._cfg.topic_or_stream)
                shard_id = shards_resp["Shards"][0]["ShardId"]
                it_resp = await client.get_shard_iterator(
                    StreamName=self._cfg.topic_or_stream,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST",
                )
                self._shard_iterator = it_resp["ShardIterator"]

            resp = await client.get_records(
                ShardIterator=self._shard_iterator,
                Limit=batch_size,
            )
            self._shard_iterator = resp.get("NextShardIterator")
            return [orjson.loads(r["Data"]) for r in resp.get("Records", [])]

    async def commit(self) -> None:
        # Kinesis uses iterator-based consumption; no explicit commit needed
        pass


# ---------------------------------------------------------------------------
# PostgreSQL logical replication
# ---------------------------------------------------------------------------


class PgLogicalCdcSource(CdcSource):
    """PostgreSQL logical replication CDC source using psycopg replication connection."""

    def __init__(self, cfg: CdcSourceConfig, pool: Any = None) -> None:
        self._cfg = cfg
        self._pool = pool
        self._conn: Any = None
        self._pending: list[dict] = []

    async def _ensure_conn(self) -> Any:
        if self._conn is None:
            import psycopg

            self._conn = await psycopg.AsyncConnection.connect(
                self._cfg.connection_string,
                autocommit=True,
                replication="database",
            )
        return self._conn

    async def consume(self, batch_size: int = 100, timeout_secs: float = 5.0) -> list[dict]:
        import orjson

        conn = await self._ensure_conn()
        slot = self._cfg.topic_or_stream
        records: list[dict] = []

        try:
            await conn.start_replication(
                slot_name=slot,
                slot_type=conn.ReplicationCursor.REPLICATION_LOGICAL,
                decode=True,
                options={"proto_version": "1", "publication_names": slot},
            )
        except Exception:
            pass  # already started

        try:
            cur = conn.cursor()
            await cur.start_replication(slot_name=slot, decode=True)
            import anyio

            with anyio.fail_after(timeout_secs):
                async for msg in cur:
                    try:
                        data = orjson.loads(msg.payload)
                        records.append(data)
                    except Exception:
                        pass
                    if len(records) >= batch_size:
                        break
        except Exception:
            pass

        self._pending = records
        return records

    async def commit(self) -> None:
        self._pending = []


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_cdc_source(cfg: CdcSourceConfig, pool: Any = None) -> CdcSource:
    """Return the appropriate CdcSource for the given config."""
    if cfg.backend == "kafka":
        return KafkaCdcSource(cfg)
    if cfg.backend == "kinesis":
        return KinesisCdcSource(cfg)
    if cfg.backend == "pg_logical":
        return PgLogicalCdcSource(cfg, pool=pool)
    raise ValueError(f"Unknown CDC backend: {cfg.backend!r}")
