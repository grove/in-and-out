"""In-and-Out: declarative MDM synchronization tools.

Two long-lived daemon processes:
- Ingestion: pulls data from external HTTP APIs into PostgreSQL source tables.
- Writeback: reads desired-state deltas from PostgreSQL and pushes changes back to external APIs.

Architecture:
    External APIs → [Ingestion] → PostgreSQL source tables
                                       ↓
                    [OSI-Mapping + pg-trickle IVM]
                    _delta_{mapping} stream tables
                                       ↓
    External APIs ← [Writeback] ← desired-state deltas
"""

__version__ = "0.1.0"
