"""Integration test fixtures using testcontainers PostgreSQL."""
from __future__ import annotations

import os
import re

import pytest
import pytest_asyncio

from inandout.config.tool import DatabaseConfig
from inandout.postgres.pool import create_pool


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def db_url(postgres_container):
    # get_connection_url() returns "postgresql+psycopg2://user:pass@host:port/db"
    # psycopg_pool needs a plain libpq URL: "postgresql://user:pass@host:port/db"
    url = postgres_container.get_connection_url()
    url = re.sub(r"\+[^:]+(?=://)", "", url)
    return url


@pytest.fixture(scope="session")
def run_migrations(db_url):
    """Run Alembic migrations against the test DB."""
    from alembic import command
    from alembic.config import Config

    os.environ["INOUT_DATABASE_URL"] = db_url
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")


@pytest_asyncio.fixture(scope="function")
async def pool(db_url, run_migrations):
    """Per-test async pool — avoids session-scoped async fixture event loop issues."""
    cfg = DatabaseConfig(dsn=db_url)
    p = await create_pool(cfg)
    yield p
    await p.close()
