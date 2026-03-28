"""Unit tests for GDPR purge functionality (T2 #40)."""
from __future__ import annotations

import pytest


def test_gdpr_purge_function_exists():
    """purge_by_external_id function should be importable."""
    from inandout.postgres.housekeeping import purge_by_external_id
    assert callable(purge_by_external_id)


def test_gdpr_purge_control_command_added():
    """gdpr-purge control command should be supported."""
    from inandout.engine.control import ControlDispatcher
    from unittest.mock import AsyncMock, MagicMock
    from psycopg_pool import AsyncConnectionPool
    
    pool = MagicMock(spec=AsyncConnectionPool)
    paused = set()
    dispatcher = ControlDispatcher(pool, paused)
    
    # Verify the command is recognized (won't raise ValueError for unknown command)
    assert hasattr(dispatcher, '_cmd_gdpr_purge')


def test_gdpr_purge_requires_connector():
    """gdpr-purge command should require connector parameter."""
    from inandout.engine.control import ControlDispatcher
    from unittest.mock import MagicMock
    from psycopg_pool import AsyncConnectionPool
    import pytest
    
    pool = MagicMock(spec=AsyncConnectionPool)
    paused = set()
    dispatcher = ControlDispatcher(pool, paused)
    
    with pytest.raises(ValueError, match="requires 'connector'"):
        import asyncio
        asyncio.run(dispatcher._cmd_gdpr_purge(None, "datatype", {"external_id": "123"}))


def test_gdpr_purge_requires_datatype():
    """gdpr-purge command should require datatype parameter."""
    from inandout.engine.control import ControlDispatcher
    from unittest.mock import MagicMock
    from psycopg_pool import AsyncConnectionPool
    import pytest
    
    pool = MagicMock(spec=AsyncConnectionPool)
    paused = set()
    dispatcher = ControlDispatcher(pool, paused)
    
    with pytest.raises(ValueError, match="requires 'datatype'"):
        import asyncio
        asyncio.run(dispatcher._cmd_gdpr_purge("conn", None, {"external_id": "123"}))


def test_gdpr_purge_requires_external_id_in_payload():
    """gdpr-purge command should require external_id in payload."""
    from inandout.engine.control import ControlDispatcher
    from unittest.mock import MagicMock
    from psycopg_pool import AsyncConnectionPool
    import pytest
    
    pool = MagicMock(spec=AsyncConnectionPool)
    paused = set()
    dispatcher = ControlDispatcher(pool, paused)
    
    with pytest.raises(ValueError, match="external_id"):
        import asyncio
        asyncio.run(dispatcher._cmd_gdpr_purge("conn", "dt", {}))
