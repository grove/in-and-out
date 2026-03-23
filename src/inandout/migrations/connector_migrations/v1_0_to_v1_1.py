"""Migration: v1.0 → v1.1 — rename webhook.signature_header to webhook.signature.header.

In schema version 1.0, the webhook signature config was flat:
    webhook:
      signature_header: X-Signature-256

In schema version 1.1, it was restructured to be nested:
    webhook:
      signature:
        header: X-Signature-256
"""
from __future__ import annotations

import copy


def migrate(data: dict) -> dict:
    """Apply the v1.0 → v1.1 migration.

    Renames ``webhook.signature_header`` to the nested ``webhook.signature.header``.

    Args:
        data: Raw YAML dict (ConnectorFileConfig level).

    Returns:
        Modified dict.
    """
    result = copy.deepcopy(data)

    connector = result.get("connector", {})
    webhook = connector.get("webhooks") or connector.get("webhook")
    if not isinstance(webhook, dict):
        return result

    # Rename signature_header → signature.header
    if "signature_header" in webhook:
        header_value = webhook.pop("signature_header")
        if "signature" not in webhook:
            webhook["signature"] = {}
        if isinstance(webhook["signature"], dict):
            webhook["signature"]["header"] = header_value

    # Update the connector in result
    if "webhooks" in connector:
        connector["webhooks"] = webhook
    elif "webhook" in connector:
        connector["webhook"] = webhook

    return result
