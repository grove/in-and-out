"""Unit tests for LinkedObject configuration and resolution logic (A3)."""
from __future__ import annotations

from inandout.config.connector import LinkedObject


def test_linked_object_fields() -> None:
    lo = LinkedObject(
        field="line_item_ids",
        datatype="line_items",
        detail_path="/line-items/${id}",
        concurrency=5,
    )
    assert lo.field == "line_item_ids"
    assert lo.datatype == "line_items"
    assert lo.detail_path == "/line-items/${id}"
    assert lo.concurrency == 5


def test_linked_object_default_concurrency() -> None:
    lo = LinkedObject(
        field="attachment_ids",
        datatype="attachments",
        detail_path="/attachments/${id}",
    )
    assert lo.concurrency == 3


def test_linked_object_extra_forbid() -> None:
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        LinkedObject(
            field="x",
            datatype="y",
            detail_path="/x/${id}",
            unknown_key="bad",
        )
