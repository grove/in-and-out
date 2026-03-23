"""Unit tests for housekeeping logic."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.config.tool import HousekeepingConfig, RetentionConfig
from inandout.postgres.housekeeping import _to_pg_interval, run_housekeeping


class TestToPgInterval:
    def test_90_days(self):
        assert _to_pg_interval("90d") == "90 days"

    def test_30_days(self):
        assert _to_pg_interval("30d") == "30 days"

    def test_365_days(self):
        assert _to_pg_interval("365d") == "365 days"

    def test_1_day(self):
        assert _to_pg_interval("1d") == "1 days"

    def test_7_days(self):
        assert _to_pg_interval("7d") == "7 days"

    def test_24_hours(self):
        # 24h = 86400 seconds = 1 day
        assert _to_pg_interval("24h") == "1 days"

    def test_48_hours(self):
        # 48h = 172800 seconds = 2 days
        assert _to_pg_interval("48h") == "2 days"


class TestRunHousekeeping:
    @pytest.mark.anyio
    async def test_returns_totals_dict(self):
        """run_housekeeping returns a dict with row counts."""
        mock_cur = MagicMock()
        mock_cur.rowcount = 5

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection = MagicMock(return_value=mock_conn)

        cfg = HousekeepingConfig(
            interval="1h",
            retention=RetentionConfig(
                sync_run_log="90d",
                dead_letter="30d",
                history_table="365d",
            )
        )

        result = await run_housekeeping(mock_pool, cfg, [("hubspot", "contacts")])

        assert isinstance(result, dict)
        assert "sync_run" in result

    @pytest.mark.anyio
    async def test_purges_sync_run_log(self):
        """run_housekeeping deletes from inout_ops_sync_run."""
        executed_sqls: list[str] = []

        mock_cur = MagicMock()
        mock_cur.rowcount = 3

        mock_conn = AsyncMock()

        async def capture_execute(sql, *args, **kwargs):
            executed_sqls.append(sql)
            return mock_cur

        mock_conn.execute = capture_execute
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection = MagicMock(return_value=mock_conn)

        cfg = HousekeepingConfig()

        await run_housekeeping(mock_pool, cfg, [])

        # Should have executed a DELETE on inout_ops_sync_run
        sync_run_sqls = [s for s in executed_sqls if "inout_ops_sync_run" in s]
        assert len(sync_run_sqls) == 1
        assert "DELETE FROM" in sync_run_sqls[0]
        assert "finished_at" in sync_run_sqls[0]

    @pytest.mark.anyio
    async def test_purges_dead_letter_and_history_tables(self):
        """run_housekeeping deletes from dead-letter and history tables."""
        executed_sqls: list[str] = []

        mock_cur = MagicMock()
        mock_cur.rowcount = 0

        mock_conn = AsyncMock()

        async def capture_execute(sql, *args, **kwargs):
            executed_sqls.append(sql)
            return mock_cur

        mock_conn.execute = capture_execute
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection = MagicMock(return_value=mock_conn)

        cfg = HousekeepingConfig()
        connector_datatypes = [("hubspot", "contacts"), ("salesforce", "accounts")]

        await run_housekeeping(mock_pool, cfg, connector_datatypes)

        dl_sqls = [s for s in executed_sqls if "inout_dl_ingestion_" in s]
        hist_sqls = [s for s in executed_sqls if "_history" in s]

        assert len(dl_sqls) == 2
        assert len(hist_sqls) == 2

        assert any("hubspot_contacts" in s for s in dl_sqls)
        assert any("salesforce_accounts" in s for s in dl_sqls)
        assert any("hubspot_contacts_history" in s for s in hist_sqls)
        assert any("salesforce_accounts_history" in s for s in hist_sqls)

    @pytest.mark.anyio
    async def test_uses_correct_intervals(self):
        """run_housekeeping uses the configured retention durations."""
        executed_sqls: list[str] = []

        mock_cur = MagicMock()
        mock_cur.rowcount = 0

        mock_conn = AsyncMock()

        async def capture_execute(sql, *args, **kwargs):
            executed_sqls.append(sql)
            return mock_cur

        mock_conn.execute = capture_execute
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection = MagicMock(return_value=mock_conn)

        cfg = HousekeepingConfig(
            retention=RetentionConfig(
                sync_run_log="7d",
                dead_letter="14d",
                history_table="30d",
            )
        )

        await run_housekeeping(mock_pool, cfg, [("test", "items")])

        sync_run_sql = next(s for s in executed_sqls if "inout_ops_sync_run" in s)
        assert "7 days" in sync_run_sql

        dl_sql = next(s for s in executed_sqls if "inout_dl_ingestion_" in s)
        assert "14 days" in dl_sql

        hist_sql = next(s for s in executed_sqls if "_history" in s)
        assert "30 days" in hist_sql

    @pytest.mark.anyio
    async def test_missing_table_does_not_raise(self):
        """run_housekeeping should silently skip tables that don't exist."""
        import psycopg

        call_count = 0

        mock_conn = AsyncMock()

        async def selective_execute(sql, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if "inout_dl_" in sql or "_history" in sql:
                raise psycopg.errors.UndefinedTable("table does not exist")
            mock_cur = MagicMock()
            mock_cur.rowcount = 0
            return mock_cur

        mock_conn.execute = selective_execute
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.connection = MagicMock(return_value=mock_conn)

        cfg = HousekeepingConfig()

        # Should not raise even though dl and history tables are missing
        result = await run_housekeeping(mock_pool, cfg, [("test", "items")])
        assert "sync_run" in result
