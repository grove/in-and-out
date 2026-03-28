import os
from alembic import context
from sqlalchemy import create_engine, text

config = context.config


def _get_url() -> str:
    url = os.environ.get("INOUT_DATABASE_URL", "")
    if not url:
        url = config.get_main_option("sqlalchemy.url", "")
    # Normalise URL dialect for SQLAlchemy + psycopg3
    url = url.replace("postgres://", "postgresql+psycopg://", 1)
    url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    if not url:
        raise RuntimeError("INOUT_DATABASE_URL env var is not set")
    return url


def run_migrations_online() -> None:
    engine = create_engine(_get_url())
    with engine.connect() as conn:
        context.configure(connection=conn, target_metadata=None)
        with context.begin_transaction():
            context.run_migrations()


def run_migrations_offline() -> None:
    url = _get_url()
    context.configure(url=url, target_metadata=None, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
