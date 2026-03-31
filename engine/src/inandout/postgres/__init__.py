"""PostgreSQL client utilities."""
from inandout.postgres.pool import create_pool
from inandout.postgres.schema import (
    source_table_name,
    source_table_ddl,
    ensure_source_table,
    source_history_table_ddl,
    ensure_source_history_table,
    dead_letter_table_name,
    OPERATIONAL_TABLES_DDL,
    set_schema_contract,
    get_schema_contract,
)
from inandout.postgres.watermark import get_watermark, set_watermark

__all__ = [
    "create_pool",
    "source_table_name",
    "source_table_ddl",
    "ensure_source_table",
    "source_history_table_ddl",
    "ensure_source_history_table",
    "dead_letter_table_name",
    "OPERATIONAL_TABLES_DDL",
    "get_watermark",
    "set_watermark",
    "set_schema_contract",
    "get_schema_contract",
]
