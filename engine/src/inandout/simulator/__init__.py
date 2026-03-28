"""Stateful demo simulator for in-and-out connectors.

Reads one or more connector YAML files and exposes a real HTTP server that
mirrors the exact API surface the engine expects.  Includes a reactive web UI
showing live data mutations via Server-Sent Events.

Entry point::

    inandout simulator run --connector connectors/hubspot.example.yaml
"""
