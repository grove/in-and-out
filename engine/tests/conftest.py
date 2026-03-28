"""Root conftest: configure structlog for tests."""
from __future__ import annotations

import logging

import structlog


def pytest_configure(config):
    """Configure structlog to use stdlib logging (has .name attr) for all tests."""
    logging.basicConfig(level=logging.WARNING)
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
