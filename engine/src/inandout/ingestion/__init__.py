"""Ingestion engine: polling loops, webhook reception, watermark management, circuit breakers."""
from inandout.ingestion.engine import IngestionEngine, SyncResult
from inandout.ingestion.daemon import run_ingestion_daemon

__all__ = [
    "IngestionEngine",
    "SyncResult",
    "run_ingestion_daemon",
]
