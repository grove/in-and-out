"""Event output configuration."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class EventOutputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    backend: Literal["pg_notify", "kafka", "kinesis", "stdout"] = "pg_notify"
    channel: str = "inandout_events"       # pg_notify channel name
    topic: str = "inandout-events"         # Kafka/Kinesis topic/stream
    connection_string: str | None = None   # Kafka/Kinesis connection
    include_raw: bool = False              # whether to include full raw record in event
