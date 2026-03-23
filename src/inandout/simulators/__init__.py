"""HTTP stub server framework for simulator-first testability.

Each connector ships a reference simulator covering its key interaction patterns:
pagination, auth flows, webhooks, error conditions, and rate-limit responses.
Simulators are usable in CI/CD and local development without live external systems.
"""
