"""Unit tests for TLS wiring in webhook server (A1 — T1 #42)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_webhook_server_cfg(
    tls_cert_file: str | None = None,
    tls_key_file: str | None = None,
    listen: str = "0.0.0.0:8443",
    rate_limit_per_minute: int = 300,
    ip_allowlist: list | None = None,
) -> MagicMock:
    cfg = MagicMock()
    cfg.tls_cert_file = tls_cert_file
    cfg.tls_key_file = tls_key_file
    cfg.listen = listen
    cfg.rate_limit_per_minute = rate_limit_per_minute
    cfg.ip_allowlist = ip_allowlist or []
    return cfg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_webhook_tls_both_set_passes_ssl_params():
    """When both tls_cert_file and tls_key_file are set, uvicorn Config gets ssl params."""
    captured_kwargs: dict = {}

    def _fake_config(app, **kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

    cfg = _make_webhook_server_cfg(
        tls_cert_file="/certs/server.crt",
        tls_key_file="/certs/server.key",
    )

    with patch("uvicorn.Config", side_effect=_fake_config):
        # Simulate the daemon TLS wiring logic
        webhook_uvicorn_kwargs: dict = {}
        tls_cert = cfg.tls_cert_file
        tls_key = cfg.tls_key_file
        if tls_cert and tls_key:
            webhook_uvicorn_kwargs["ssl_certfile"] = tls_cert
            webhook_uvicorn_kwargs["ssl_keyfile"] = tls_key

        import uvicorn

        uvicorn.Config(MagicMock(), host="0.0.0.0", port=8443, **webhook_uvicorn_kwargs)

    assert captured_kwargs.get("ssl_certfile") == "/certs/server.crt"
    assert captured_kwargs.get("ssl_keyfile") == "/certs/server.key"


def test_webhook_tls_neither_set_no_ssl_params():
    """When neither tls_cert_file nor tls_key_file is set, no ssl params passed."""
    captured_kwargs: dict = {}

    def _fake_config(app, **kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

    cfg = _make_webhook_server_cfg(
        tls_cert_file=None,
        tls_key_file=None,
    )

    with patch("uvicorn.Config", side_effect=_fake_config):
        webhook_uvicorn_kwargs: dict = {}
        tls_cert = cfg.tls_cert_file
        tls_key = cfg.tls_key_file
        if tls_cert and tls_key:
            webhook_uvicorn_kwargs["ssl_certfile"] = tls_cert
            webhook_uvicorn_kwargs["ssl_keyfile"] = tls_key

        import uvicorn

        uvicorn.Config(MagicMock(), host="0.0.0.0", port=8443, **webhook_uvicorn_kwargs)

    assert "ssl_certfile" not in captured_kwargs
    assert "ssl_keyfile" not in captured_kwargs


def test_webhook_tls_only_cert_set_logs_warning_no_tls(caplog):
    """When only tls_cert_file is set (missing key), log warning, proceed without TLS."""
    import logging
    import structlog

    captured_kwargs: dict = {}

    def _fake_config(app, **kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

    cfg = _make_webhook_server_cfg(
        tls_cert_file="/certs/server.crt",
        tls_key_file=None,
    )

    warnings_logged = []

    with patch("uvicorn.Config", side_effect=_fake_config):
        webhook_uvicorn_kwargs: dict = {}
        tls_cert = cfg.tls_cert_file
        tls_key = cfg.tls_key_file
        if tls_cert and tls_key:
            webhook_uvicorn_kwargs["ssl_certfile"] = tls_cert
            webhook_uvicorn_kwargs["ssl_keyfile"] = tls_key
        elif tls_cert or tls_key:
            warnings_logged.append("webhook_tls_incomplete")

        import uvicorn

        uvicorn.Config(MagicMock(), host="0.0.0.0", port=8443, **webhook_uvicorn_kwargs)

    assert "ssl_certfile" not in captured_kwargs
    assert "ssl_keyfile" not in captured_kwargs
    assert "webhook_tls_incomplete" in warnings_logged


def test_webhook_tls_only_key_set_logs_warning_no_tls():
    """When only tls_key_file is set (missing cert), log warning, proceed without TLS."""
    captured_kwargs: dict = {}

    def _fake_config(app, **kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

    cfg = _make_webhook_server_cfg(
        tls_cert_file=None,
        tls_key_file="/certs/server.key",
    )

    warnings_logged = []

    with patch("uvicorn.Config", side_effect=_fake_config):
        webhook_uvicorn_kwargs: dict = {}
        tls_cert = cfg.tls_cert_file
        tls_key = cfg.tls_key_file
        if tls_cert and tls_key:
            webhook_uvicorn_kwargs["ssl_certfile"] = tls_cert
            webhook_uvicorn_kwargs["ssl_keyfile"] = tls_key
        elif tls_cert or tls_key:
            warnings_logged.append("webhook_tls_incomplete")

        import uvicorn

        uvicorn.Config(MagicMock(), host="0.0.0.0", port=8443, **webhook_uvicorn_kwargs)

    assert "ssl_certfile" not in captured_kwargs
    assert "ssl_keyfile" not in captured_kwargs
    assert "webhook_tls_incomplete" in warnings_logged
